"""In-memory Kubernetes core/v1 API server (a local stand-in for a real cluster).

It listens on the same paths a real API server uses for the handful of
operations the handover service needs, so pointing the service at it with a
plain ``kubeconfig`` (or ``KUBE_MODE=remote``) exercises the real HTTP client
path end to end.

It is deliberately *not* a general-purpose Kubernetes emulator: only PV/PVC
read, PV merge-patch and a small operator surface (create/delete fixtures,
trigger the binder) are implemented. Anything else returns 404.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.kube import Conflict, Invalid, NotFound
from mock_kube.store import InMemoryKube

log = logging.getLogger("mock_kube")


class FaultInjector:
    """Tiny operator-controlled fault surface for the mock API server.

    ``drop_pvc_patch_receipts`` is a counter. Every PVC merge patch is applied
    normally, but while the counter is positive the response is replaced with
    a 503 and the counter is decremented. That reproduces the nasty case where
    the API server persists a write and the caller never receives the receipt
    (a proxy timeout, a connection reset, a 5xx from a load balancer).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._drop_pvc_patch_receipts = 0

    def consume_dropped_receipt(self) -> bool:
        with self._lock:
            if self._drop_pvc_patch_receipts <= 0:
                return False
            self._drop_pvc_patch_receipts -= 1
            return True

    def configure(self, body: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            if "drop_pvc_patch_receipts" in body:
                self._drop_pvc_patch_receipts = int(body["drop_pvc_patch_receipts"])
            return self._snapshot_locked()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self._snapshot_locked()

    def _snapshot_locked(self) -> dict[str, Any]:
        return {"drop_pvc_patch_receipts": self._drop_pvc_patch_receipts}

    def reset(self) -> None:
        with self._lock:
            self._drop_pvc_patch_receipts = 0


def _status_error(status: int, reason: str, message: str, details: dict | None = None) -> JSONResponse:
    body: dict[str, Any] = {
        "kind": "Status",
        "apiVersion": "v1",
        "status": "Failure",
        "message": message,
        "reason": reason,
        "code": status,
    }
    if details:
        body["details"] = details
    return JSONResponse(status_code=status, content=body)


def create_app(store: InMemoryKube | None = None) -> FastAPI:
    state = store or InMemoryKube()
    if os.environ.get("MOCK_SEED", "").lower() in ("1", "true", "yes"):
        from mock_kube.seed import seed

        seed(state)
        log.info("mock API server seeded with the monitoring handover scenario")
    app = FastAPI(
        title="Local Kubernetes API stand-in",
        description=(
            "本地 Kubernetes API 模拟端，仅供交接与扩容服务联调。\n\n"
            "### 运维接口\n"
            "* `POST /_mock/pvs` 创建 PV\n"
            "* `POST /_mock/storageclasses` 创建 StorageClass\n"
            "* `POST /_mock/namespaces/{namespace}/pvcs` 创建 PVC\n"
            "* `DELETE /_mock/namespaces/{namespace}/pvcs/{name}` 删除 PVC（模拟旧声明被删）\n"
            "* `POST /_mock/bind/{pv}` 触发一次绑定\n"
            "* `POST /_mock/reconcile` 对所有可绑定卷执行绑定\n"
            "* `POST /_mock/expansions/reconcile` 模拟存储驱动更新 status.capacity\n"
            "* `GET /_mock/state` 查看全部 PV/PVC/StorageClass/ConfigMap\n"
            "* `POST /_mock/faults` 注入故障：`{\"drop_pvc_patch_receipts\": 1}` 表示\n"
            "  下一次 PVC 写入照常生效，但回执以 503 丢弃（验证写入超时/回执丢失路径）\n"
            "* `POST /_mock/reset` 清空状态\n"
        ),
        version="1.1.0",
    )
    app.state.store = state
    app.state.faults = FaultInjector()
    _install(app, state)
    return app


def _install(app: FastAPI, store: InMemoryKube) -> None:
    # ---------------------------------------------------------- core/v1 reads
    @app.get("/api/v1/persistentvolumes")
    async def list_pvs(limit: int | None = None) -> dict[str, Any]:
        return _list("PersistentVolumeList", store.list_pvs(limit=limit))

    @app.get("/api/v1/persistentvolumes/{name}")
    async def read_pv(name: str):
        try:
            return store.get_pv(name)
        except NotFound as exc:
            return _status_error(
                404, "NotFound", str(exc), {"name": name, "kind": "persistentvolumes"}
            )

    @app.patch("/api/v1/persistentvolumes/{name}")
    async def patch_pv(name: str, request: Request):
        content_type = request.headers.get("content-type", "")
        if "merge-patch" not in content_type:
            return _status_error(
                415,
                "UnsupportedMediaType",
                f"mock server only accepts application/merge-patch+json, got {content_type!r}",
            )
        try:
            body = await request.json()
        except ValueError:
            return _status_error(400, "BadRequest", "request body is not valid JSON")
        try:
            return store.update_pv(name, body)
        except NotFound as exc:
            return _status_error(404, "NotFound", str(exc))
        except Conflict as exc:
            return _status_error(409, "Conflict", str(exc))
        except Invalid as exc:
            return _status_error(422, "Invalid", str(exc))

    @app.get("/api/v1/namespaces/{namespace}/persistentvolumeclaims")
    async def list_pvcs(namespace: str) -> dict[str, Any]:
        return _list("PersistentVolumeClaimList", store.list_pvcs(namespace))

    @app.get("/api/v1/namespaces/{namespace}/persistentvolumeclaims/{name}")
    async def read_pvc(namespace: str, name: str):
        try:
            return store.get_pvc(namespace, name)
        except NotFound as exc:
            return _status_error(
                404,
                "NotFound",
                str(exc),
                {"name": name, "kind": "persistentvolumeclaims"},
            )

    @app.patch("/api/v1/namespaces/{namespace}/persistentvolumeclaims/{name}")
    async def patch_pvc(namespace: str, name: str, request: Request):
        content_type = request.headers.get("content-type", "")
        if "merge-patch" not in content_type:
            return _status_error(
                415,
                "UnsupportedMediaType",
                f"mock server only accepts application/merge-patch+json, got {content_type!r}",
            )
        try:
            body = await request.json()
        except ValueError:
            return _status_error(400, "BadRequest", "request body is not valid JSON")
        try:
            updated = store.update_pvc(namespace, name, body)
        except NotFound as exc:
            return _status_error(404, "NotFound", str(exc))
        except Conflict as exc:
            return _status_error(409, "Conflict", str(exc))
        except Invalid as exc:
            return _status_error(422, "Invalid", str(exc))

        response = JSONResponse(status_code=200, content=updated)
        if request.app.state.faults.consume_dropped_receipt():
            # The write above *did* happen. Pretend the receipt never made it
            # back to the caller, so the service must re-read and verify.
            log.warning(
                "fault injected: PVC write %s/%s applied but receipt dropped",
                namespace,
                name,
            )
            return _status_error(
                503,
                "ServiceUnavailable",
                "connection lost after write was applied (injected fault)",
            )
        return response

    # ------------------------------------------------- storage.k8s.io/v1 reads
    @app.get("/apis/storage.k8s.io/v1/storageclasses")
    async def list_storage_classes() -> dict[str, Any]:
        return _list("StorageClassList", store.list_storage_classes())

    @app.get("/apis/storage.k8s.io/v1/storageclasses/{name}")
    async def read_storage_class(name: str):
        try:
            return store.get_storage_class(name)
        except NotFound as exc:
            return _status_error(
                404, "NotFound", str(exc),
                {"name": name, "kind": "storageclasses"},
            )

    # ------------------------------------------------------- core/v1 configmaps
    @app.get("/api/v1/namespaces/{namespace}/configmaps")
    async def list_config_maps(namespace: str) -> dict[str, Any]:
        return _list("ConfigMapList", store.list_config_maps(namespace))

    @app.post("/api/v1/namespaces/{namespace}/configmaps", status_code=201)
    async def create_config_map(namespace: str, request: Request):
        body = await request.json()
        try:
            return store.create_config_map(namespace, body)
        except Conflict as exc:
            return _status_error(409, "AlreadyExists", str(exc))
        except Invalid as exc:
            return _status_error(422, "Invalid", str(exc))

    @app.get("/api/v1/namespaces/{namespace}/configmaps/{name}")
    async def read_config_map(namespace: str, name: str):
        try:
            return store.get_config_map(namespace, name)
        except NotFound as exc:
            return _status_error(
                404, "NotFound", str(exc), {"name": name, "kind": "configmaps"}
            )

    @app.patch("/api/v1/namespaces/{namespace}/configmaps/{name}")
    async def patch_config_map(namespace: str, name: str, request: Request):
        content_type = request.headers.get("content-type", "")
        if "merge-patch" not in content_type:
            return _status_error(
                415,
                "UnsupportedMediaType",
                f"mock server only accepts application/merge-patch+json, got {content_type!r}",
            )
        try:
            body = await request.json()
        except ValueError:
            return _status_error(400, "BadRequest", "request body is not valid JSON")
        try:
            return store.update_config_map(namespace, name, body)
        except NotFound as exc:
            return _status_error(404, "NotFound", str(exc))
        except Conflict as exc:
            return _status_error(409, "Conflict", str(exc))
        except Invalid as exc:
            return _status_error(422, "Invalid", str(exc))

    # ------------------------------------------------------ operator fixture API
    @app.post("/_mock/pvs", status_code=201)
    async def create_pv(request: Request):
        body = await request.json()
        try:
            return store.create_pv(body)
        except Conflict as exc:
            return _status_error(409, "AlreadyExists", str(exc))
        except Invalid as exc:
            return _status_error(422, "Invalid", str(exc))

    @app.post("/_mock/storageclasses", status_code=201)
    async def create_storage_class(request: Request):
        body = await request.json()
        try:
            return store.create_storage_class(body)
        except Conflict as exc:
            return _status_error(409, "AlreadyExists", str(exc))
        except Invalid as exc:
            return _status_error(422, "Invalid", str(exc))

    @app.post("/_mock/namespaces/{namespace}/pvcs", status_code=201)
    async def create_pvc(namespace: str, request: Request):
        body = await request.json()
        try:
            return store.create_pvc(namespace, body)
        except Conflict as exc:
            return _status_error(409, "AlreadyExists", str(exc))
        except Invalid as exc:
            return _status_error(422, "Invalid", str(exc))

    @app.get("/_mock/expansions")
    async def expansion_state() -> dict[str, Any]:
        return {"storageClasses": [
            {"name": sc["metadata"]["name"],
             "allowVolumeExpansion": sc.get("allowVolumeExpansion", False)}
            for sc in store.list_storage_classes()
        ]}

    @app.post("/_mock/expansions/reconcile")
    async def reconcile_expansions() -> dict[str, Any]:
        return {"grown": store.reconcile_expansions()}

    @app.get("/_mock/state")
    async def state() -> dict[str, Any]:
        return store.snapshot()

    @app.post("/_mock/reset")
    async def reset(request: Request) -> dict[str, str]:
        store.reset()
        request.app.state.faults.reset()
        return {"status": "reset"}

    @app.post("/_mock/faults")
    async def configure_faults(request: Request) -> dict[str, Any]:
        body = await request.json()
        return request.app.state.faults.configure(body)

    @app.get("/_mock/faults")
    async def read_faults(request: Request) -> dict[str, Any]:
        return request.app.state.faults.snapshot()

    @app.delete("/_mock/namespaces/{namespace}/pvcs/{name}")
    async def delete_pvc(namespace: str, name: str):
        try:
            return store.delete_pvc(namespace, name)
        except NotFound as exc:
            return _status_error(404, "NotFound", str(exc))

    @app.delete("/_mock/pvs/{name}")
    async def delete_pv(name: str):
        try:
            return store.delete_pv(name)
        except NotFound as exc:
            return _status_error(404, "NotFound", str(exc))

    @app.post("/_mock/bind/{pv_name}")
    async def bind(pv_name: str):
        try:
            return store.bind_pv(pv_name)
        except (Conflict, NotFound) as exc:
            return _status_error(409, "Conflict", str(exc))

    @app.post("/_mock/reconcile")
    async def reconcile() -> dict[str, Any]:
        return {"bound": store.reconcile()}


def _list(kind: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "kind": kind,
        "apiVersion": "v1",
        "metadata": {"resourceVersion": "1"},
        "items": items,
    }


app = create_app()


def main() -> None:  # pragma: no cover - convenience entry point
    import uvicorn

    uvicorn.run(
        "mock_kube.app:app",
        host=os.environ.get("MOCK_KUBE_HOST", "0.0.0.0"),
        port=int(os.environ.get("MOCK_KUBE_PORT", "8001")),
        log_level=os.environ.get("LOG_LEVEL", "info").lower(),
    )


if __name__ == "__main__":  # pragma: no cover
    main()
