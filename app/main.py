"""FastAPI application exposing the volume handover API."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

import anyio
from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse

from app.errors import ApiError
from app.expansion import ExpansionService
from app.kube import KubernetesClient, KubeClient, KubeError, Unavailable
from app.schemas import (
    ClaimRef,
    ErrorResponse,
    ExpansionQuota,
    ExpansionRequest,
    ExpansionResponse,
    HandoverRequest,
    HandoverResponse,
    HealthResponse,
)
from app.service import HandoverService

log = logging.getLogger("handover.api")


def _build_kube() -> KubeClient:
    """Honour ``KUBE_MODE`` so the same image serves real and mock clusters.

    ``remote`` (default) uses the official client against whatever the
    kubeconfig or in-cluster config points at. ``mock`` skips HTTP entirely
    and keeps the volumes in process -- handy for a quick Swagger demo.
    """
    mode = os.environ.get("KUBE_MODE", "remote").lower()
    if mode == "mock":
        from mock_kube.store import InMemoryKube

        log.info("using in-memory Kubernetes store (KUBE_MODE=mock)")
        return InMemoryKube()
    return KubernetesClient.from_config(
        kubeconfig=os.environ.get("KUBECONFIG") or None,
        context=os.environ.get("KUBE_CONTEXT") or None,
        in_cluster=_env_bool("KUBE_IN_CLUSTER"),
        request_timeout=float(os.environ.get("KUBE_REQUEST_TIMEOUT", "10")),
    )


def _env_bool(name: str) -> bool | None:
    value = os.environ.get(name)
    if value is None or value == "":
        return None
    return value.lower() in ("1", "true", "yes", "on")


def create_app(kube: KubeClient | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if kube is None:
            app.state.kube = _build_kube()
            app.state.service = HandoverService(app.state.kube)
        app.state.expansion = ExpansionService(app.state.kube)
        yield
        close = getattr(app.state.kube, "close", None)
        if callable(close):
            result = close()
            if hasattr(result, "__await__"):
                await result

    app = FastAPI(
        title="监控数据卷交接与扩容 API",
        version="1.1.0",
        description=(
            "在 Kubernetes 上重建监控库时，把旧的、已 Released 且回收策略为 "
            "Retain 的持久卷交给已预先绑定的新 PVC；并按存储类**逐块**扩容监控 PVC。\n\n"
            "**交接幂等性**：同一新 PVC（命名空间/名称/UID）重复提交只返回集群进度，"
            "不会重复写入。\n\n"
            "**交接并发**：不同接手方并发提交时，只有一方能拿到预留，另一方收到 409。\n\n"
            "**扩容名额**：每个存储类同时最多一笔经 `/expansions` 发起的扩容在途。"
            "名额记录在集群内的协调 ConfigMap 中，两个服务实例共享；只有核实实际容量"
            "已达目标，或核实写入未生效，才释放名额。写入结果未明时保持 `unconfirmed`"
            "（待核实）并继续占用。\n\n"
            "**扩容幂等性**：同一 `request_id` 重提可查进度或接续原申请；改参数报 409。"
        ),
        lifespan=lifespan,
    )
    if kube is not None:
        app.state.kube = kube
        app.state.service = HandoverService(kube)
        app.state.expansion = ExpansionService(kube)

    _install_handlers(app)
    _install_routes(app)
    return app


def _install_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={"error": exc.to_dict()})

    @app.exception_handler(RequestValidationError)
    async def _validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "invalid_request",
                    "message": "请求参数不合法。",
                    "details": {"errors": jsonable_errors(exc)},
                }
            },
        )

    @app.exception_handler(KubeError)
    async def _kube_error_handler(request: Request, exc: KubeError) -> JSONResponse:
        status_code = 503 if isinstance(exc, Unavailable) else 502
        return JSONResponse(
            status_code=status_code,
            content={
                "error": {
                    "code": "kube_unavailable" if status_code == 503 else "kube_error",
                    "message": f"Kubernetes API 交互失败：{exc}",
                    "details": {},
                }
            },
        )


def _install_routes(app: FastAPI) -> None:
    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse(url="/docs")

    @app.get("/healthz", response_model=HealthResponse, summary="健康检查", tags=["运维"])
    async def healthz(request: Request) -> HealthResponse:
        try:
            await anyio.to_thread.run_sync(_probe, request.app.state.service)
        except KubeError as exc:
            return HealthResponse(status="degraded", kubernetes="unreachable", detail=str(exc))
        return HealthResponse(status="ok", kubernetes="ok")

    @app.post(
        "/handovers",
        response_model=HandoverResponse,
        status_code=202,
        responses={
            200: {"model": HandoverResponse, "description": "交接已完成"},
            202: {"model": HandoverResponse, "description": "已预留，等待绑定完成"},
            404: {"model": ErrorResponse, "description": "PV 不存在"},
            409: {"model": ErrorResponse, "description": "卷状态或预留冲突"},
            422: {"model": ErrorResponse, "description": "请求参数不合法"},
            503: {"model": ErrorResponse, "description": "Kubernetes API 不可用"},
        },
        summary="提交交接（幂等，可重复调用查进度）",
        tags=["交接"],
    )
    async def submit_handover(body: HandoverRequest, request: Request) -> JSONResponse:
        service: HandoverService = request.app.state.service
        response = await anyio.to_thread.run_sync(service.submit, body)
        status_code = 200 if response.status == "completed" else 202
        return JSONResponse(status_code=status_code, content=response.model_dump())

    @app.get(
        "/handovers/status",
        response_model=HandoverResponse,
        responses={
            404: {"model": ErrorResponse},
            409: {"model": ErrorResponse, "description": "该卷未预留给请求中的新声明"},
            503: {"model": ErrorResponse},
        },
        summary="只读查询交接进度",
        tags=["交接"],
    )
    async def handover_status(
        request: Request,
        pv_name: str = Query(..., description="持久卷名称"),
        old_pvc_uid: str = Query(..., description="旧 PVC UID"),
        namespace: str = Query(..., description="新 PVC 命名空间"),
        name: str = Query(..., description="新 PVC 名称"),
        uid: str = Query(..., description="新 PVC UID"),
    ) -> HandoverResponse:
        service: HandoverService = request.app.state.service
        body = HandoverRequest(
            pv_name=pv_name,
            old_pvc_uid=old_pvc_uid,
            new_pvc=ClaimRef(namespace=namespace, name=name, uid=uid),
        )
        return await anyio.to_thread.run_sync(service.status, body)

    # ------------------------------------------------------------- expansion
    @app.post(
        "/expansions",
        response_model=ExpansionResponse,
        status_code=202,
        responses={
            200: {"model": ExpansionResponse, "description": "扩容已完成或已核实未生效"},
            202: {"model": ExpansionResponse, "description": "名额已占用，写入或核实进行中"},
            404: {"model": ErrorResponse, "description": "PVC 或请求号不存在"},
            409: {"model": ErrorResponse, "description": "名额被占用 / 请求号参数冲突 / 准入不通过"},
            422: {"model": ErrorResponse, "description": "请求参数不合法"},
            502: {"model": ErrorResponse, "description": "集群写入被拒绝或未反映"},
            503: {"model": ErrorResponse, "description": "写入结果未明 / Kubernetes API 不可用"},
        },
        summary="申请扩容一个 PVC（同名请求号幂等，可接续原申请）",
        tags=["扩容"],
    )
    async def submit_expansion(body: ExpansionRequest, request: Request) -> JSONResponse:
        service: ExpansionService = request.app.state.expansion
        response = await anyio.to_thread.run_sync(service.submit, body)
        # 202 while the gate is still held; 200 once nothing is in flight.
        status_code = 202 if response.status in ("writing", "unconfirmed", "expanding") else 200
        return JSONResponse(status_code=status_code, content=response.model_dump())

    @app.get(
        "/expansions/{request_id}",
        response_model=ExpansionResponse,
        responses={404: {"model": ErrorResponse}},
        summary="按请求号只读查询扩容进度（不推进核实、不改变名额）",
        tags=["扩容"],
    )
    async def expansion_status(request_id: str, request: Request) -> ExpansionResponse:
        service: ExpansionService = request.app.state.expansion
        return await anyio.to_thread.run_sync(service.status, request_id)

    @app.get(
        "/expansions",
        response_model=ExpansionQuota,
        summary="查询某个存储类的扩容名额占用情况",
        tags=["扩容"],
    )
    async def expansion_quota(
        request: Request,
        storage_class: str = Query(..., description="存储类名称"),
    ) -> ExpansionQuota:
        service: ExpansionService = request.app.state.expansion
        return await anyio.to_thread.run_sync(service.quota, storage_class)


def _probe(service: HandoverService) -> None:
    # Listing with limit=1 is a cheap reachability check and needs only "list".
    service.kube.list_pvs(limit=1)


def jsonable_errors(exc: RequestValidationError) -> list[dict[str, Any]]:
    errors = []
    for error in exc.errors():
        item = {k: v for k, v in error.items() if k != "ctx"}
        item["loc"] = [str(part) for part in item.get("loc", [])]
        errors.append(item)
    return errors


app = create_app()
