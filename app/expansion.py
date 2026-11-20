"""Per-storage-class PVC expansion with a shared, cluster-backed quota.

Why this exists
---------------
Monitoring PVCs on the same storage class must be expanded **one at a time**:
an operator asks for a bigger claim, and only after the cluster has actually
grown that claim (or after we have proven the write did not take effect) may
the next claim of that class be submitted.

Two API replicas serve the same cluster, so the gate cannot live in process
memory. It is stored in one Kubernetes ConfigMap (``pvc-expansion-state`` by
default), one key per request id. Claiming the gate is an optimistic-
concurrency write: read the ConfigMap, check no *other* request of the class
occupies the gate, then patch it with the ``resourceVersion`` we just read.
If two replicas race, only one patch is accepted; the loser re-reads, sees the
winner's record and returns ``409 expansion_in_flight``.

Request ids are the idempotency key
-----------------------------------
* same request id, same parameters -> progress, or resume the original write;
* same request id, different parameters -> ``409 request_id_conflict``.

What "in flight" means
----------------------
A record holds the gate while its phase is ``writing``, ``unconfirmed`` or
``expanding``. It is released only when a trusted read shows either

* the same PVC UID's ``status.capacity.storage`` has reached the target, or
* ``spec.resources.requests.storage`` is still below the target *and* the
  confirmation delay has passed, i.e. the write demonstrably did not happen.

A failed write with an ambiguous outcome (timeout, dropped connection, 5xx)
must not be assumed to have failed: the API server may have applied it. Such
a record turns into ``unconfirmed`` ("待核实") and keeps the gate. Re-submitting
the same request id re-reads the PVC; once the confirmation delay has passed it
either completes, waits for ``status.capacity``, or resumes the original write.

All coordination state is cluster state (the ConfigMap) and all progress is
derived from live PV/PVC objects, so service restarts lose nothing.
"""

from __future__ import annotations

import json
import logging
import os
import socket
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Literal

from app import kube_objects as ko
from app.errors import ApiError
from app.kube import (
    Ambiguous,
    Conflict,
    Invalid,
    KubeClient,
    KubeError,
    NotFound,
    Unavailable,
)
from app.schemas import (
    ExpansionQuota,
    ExpansionRequest,
    ExpansionResponse,
    PVCSummary,
)

log = logging.getLogger("expansion")

STATE_CONFIGMAP_DEFAULT = "pvc-expansion-state"
STATE_NAMESPACE_DEFAULT = "monitoring"
CONFIRM_DELAY_DEFAULT = 5.0
MAX_WRITE_ATTEMPTS = 3
MAX_STATE_ATTEMPTS = 8
RECORD_KEY_PREFIX = "request-"

Phase = Literal["writing", "unconfirmed", "expanding", "completed", "not_applied"]
OCCUPYING = {"writing", "unconfirmed", "expanding"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _record_key(request_id: str) -> str:
    return f"{RECORD_KEY_PREFIX}{request_id}"


class ExpansionService:
    """Admission, gate coordination and verification for PVC expansion."""

    def __init__(
        self,
        kube: KubeClient,
        *,
        state_namespace: str | None = None,
        state_configmap: str | None = None,
        confirm_delay_seconds: float | None = None,
        instance_id: str | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.kube = kube
        self.state_namespace = state_namespace or os.environ.get(
            "EXPANSION_STATE_NAMESPACE", STATE_NAMESPACE_DEFAULT
        )
        self.state_configmap = state_configmap or os.environ.get(
            "EXPANSION_STATE_CONFIGMAP", STATE_CONFIGMAP_DEFAULT
        )
        if confirm_delay_seconds is None:
            confirm_delay_seconds = float(
                os.environ.get("EXPANSION_CONFIRM_DELAY_SECONDS", CONFIRM_DELAY_DEFAULT)
            )
        self.confirm_delay = max(0.0, confirm_delay_seconds)
        self.instance_id = instance_id or os.environ.get(
            "INSTANCE_ID", socket.gethostname()
        )
        self._clock = clock or _now

    # ------------------------------------------------------------------ public
    def submit(self, request: ExpansionRequest) -> ExpansionResponse:
        """Start, resume or report one expansion request."""
        target = _parse_target(request.target_capacity)
        record = self._find_record(request.request_id)
        if record is not None:
            self._require_same_request(record, request, target)
            return self._advance(record, request, target)
        return self._start(request, target)

    def status(self, request_id: str) -> ExpansionResponse:
        """Read-only view of a stored request (never advances the gate)."""
        record = self._find_record(request_id)
        if record is None:
            raise ApiError(
                404,
                "request_not_found",
                f"请求号 {request_id} 不存在。",
                {"request_id": request_id},
            )
        pvc, _ = self._read_pvc(record["namespace"], record["name"])
        # Show what a fresh read of the cluster proves, but change nothing:
        # a completion is only recorded (and the gate only freed) by POSTing
        # the same request id again.
        phase, occupies, note = self._classify(record, pvc)
        text = (
            f"只读查询：按当前集群状态为 {phase}（{note}）。"
            "如需推进核实或释放名额，请用同一请求号重新 POST /expansions。"
        )
        return self._response(
            record,
            phase=phase,
            occupies=occupies,
            status_text=text,
            pvc=pvc,
        )

    def quota(self, storage_class: str) -> ExpansionQuota:
        """Read-only snapshot of the class gate.

        The gate is re-derived from live PVC state, exactly like an admission
        check would do it, so "occupied: false" means the next submission for
        this class will be admitted. Nothing is persisted here.
        """
        return self._quota(storage_class)

    def _find_holder(self, storage_class: str) -> dict[str, Any] | None:
        cm = self._load_state(create=False)
        if cm is None:
            return None
        return _holder(_parse_records(cm), storage_class, "")

    # ------------------------------------------------------------- first submit
    def _start(self, request: ExpansionRequest, target: Decimal) -> ExpansionResponse:
        pvc = self._admit(request, target)

        # Claim the per-class gate *before* touching the PVC. A competing
        # replica that read the same empty state loses the resourceVersion
        # race inside _mutate and re-evaluates.
        record = self._claim(request, target, pvc)

        # The gate is ours; now patch the PVC.
        return self._write(record, request, pvc, target)

    def _admit(self, request: ExpansionRequest, target: Decimal) -> dict[str, Any]:
        pvc = self._require_pvc(request)
        phase = (pvc.get("status") or {}).get("phase")
        if phase != "Bound":
            raise ApiError(
                409,
                "pvc_not_bound",
                f"PVC {request.namespace}/{request.name} 当前阶段为 "
                f"{phase or '<未知>'}，只有 Bound 的 PVC 可以扩容。",
                _pvc_details(pvc),
            )

        requested_raw = ko.requested_storage(pvc)
        requested = ko.parse_storage(requested_raw)
        if requested is None:
            raise ApiError(
                409,
                "current_capacity_unknown",
                f"无法解析 PVC 当前申请容量 {requested_raw!r}，不能判断扩容目标。",
                _pvc_details(pvc),
            )
        if target <= requested:
            raise ApiError(
                409,
                "target_not_greater",
                f"目标容量 {request.target_capacity} 必须大于当前申请容量 "
                f"{requested_raw}。",
                {
                    **_pvc_details(pvc),
                    "current_requested": requested_raw,
                    "target": request.target_capacity,
                },
            )

        self._require_expandable_class(pvc)
        return pvc

    def _require_expandable_class(self, pvc: dict[str, Any]) -> None:
        storage_class = ko.storage_class(pvc)
        if not storage_class:
            raise ApiError(
                409,
                "storage_class_missing",
                "该 PVC 未设置 storageClassName，无法确认存储类是否允许扩容。",
                _pvc_details(pvc),
            )
        try:
            sc = self.kube.get_storage_class(storage_class)
        except NotFound as exc:
            raise ApiError(
                409,
                "storage_class_not_found",
                f"存储类 {storage_class} 不存在，无法确认是否允许扩容。",
                {"storage_class": storage_class},
            ) from exc
        except KubeError as exc:
            raise _kube_api_error(exc, "读取存储类失败") from exc
        if sc.get("allowVolumeExpansion") is not True:
            raise ApiError(
                409,
                "storage_class_expansion_disabled",
                f"存储类 {storage_class} 未开启 allowVolumeExpansion，不能扩容。",
                {"storage_class": storage_class,
                 "allow_volume_expansion": sc.get("allowVolumeExpansion", False)},
            )

    # ------------------------------------------------------------ resume submit
    def _advance(
        self,
        record: dict[str, Any],
        request: ExpansionRequest,
        target: Decimal,
    ) -> ExpansionResponse:
        if record.get("phase") == "completed":
            pvc, _ = self._read_pvc(record["namespace"], record["name"])
            if pvc is not None and _pvc_uid(pvc) != record["uid"]:
                # The claim was rebuilt; the completed record describes the
                # old object and must not be reported as current progress.
                raise ApiError(
                    409,
                    "pvc_uid_mismatch",
                    f"同名 PVC 的 UID 已变为 {_pvc_uid(pvc)}，与原请求 "
                    f"{record['uid']} 不一致，原扩容记录已失效，请用新 UID 新请求号提交。",
                    {
                        "request_id": record["request_id"],
                        "stored_uid": record["uid"],
                        "actual_uid": _pvc_uid(pvc),
                    },
                )
            return self._response(
                record,
                phase="completed",
                occupies=False,
                status_text="扩容已完成，实际容量已达到目标。",
                pvc=pvc,
            )

        pvc, pvc_error = self._read_pvc(record["namespace"], record["name"])
        phase, occupies, note = self._classify(record, pvc)
        if phase in ("completed", "unconfirmed", "expanding"):
            record = self._persist(record, phase=phase, occupies=occupies, note=note)
            return self._response(
                record,
                phase=phase,
                occupies=occupies,
                status_text=_status_text(phase, note),
                pvc=pvc,
                pvc_error=pvc_error,
            )

        # phase == not_applied: the original write demonstrably did not take
        # effect. Resume the *same* request: re-admit, re-claim the gate (in
        # case another request took it after our record was released) and
        # write again.
        log.info(
            "resuming expansion request %s for %s/%s after verifying no effect",
            record["request_id"],
            record["namespace"],
            record["name"],
        )
        try:
            pvc = self._admit(request, target)
            record = self._claim(request, target, pvc, existing=record)
        except ApiError as exc:
            self._persist(record, phase="not_applied", occupies=False, note=note)
            raise exc
        return self._write(record, request, pvc, target)

    # ------------------------------------------------------------------- gate
    def _claim(
        self,
        request: ExpansionRequest,
        target: Decimal,
        pvc: dict[str, Any],
        *,
        existing: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        storage_class = ko.storage_class(pvc)
        now = self._clock()
        confirm_after = _iso(now + timedelta(seconds=self.confirm_delay))
        claimed: dict[str, Any] = {}

        def mutate(records: dict[str, dict[str, Any]]) -> None:
            holder = _holder(records, storage_class, request.request_id)
            if holder is not None:
                # Verify the holder before honouring its gate. If it clearly
                # finished or clearly never wrote, release it and carry on.
                held_pvc, _ = self._read_pvc(holder["namespace"], holder["name"])
                phase, occupies, note = self._classify(holder, held_pvc)
                if occupies:
                    raise ApiError(
                        409,
                        "expansion_in_flight",
                        f"存储类 {storage_class} 已有一笔扩容在途"
                        f"（请求号 {holder['request_id']}，PVC "
                        f"{holder['namespace']}/{holder['name']}，状态 {phase}）。"
                        "实际容量达到目标或核实写入未生效后才会放行下一笔。",
                        {
                            "storage_class": storage_class,
                            "holder": _quota_from_record(
                                holder, phase, occupies
                            ).model_dump(),
                        },
                    )
                holder = {**holder, "phase": phase, "occupies_quota": occupies,
                          "last_note": note, "updated_at": _iso(now)}
                records[holder["request_id"]] = holder

            base = existing or {
                "request_id": request.request_id,
                "namespace": request.namespace,
                "name": request.name,
                "uid": request.uid,
                "storage_class": storage_class,
                "created_at": _iso(now),
            }
            record = {
                **base,
                "storage_class": storage_class,
                "target": request.target_capacity,
                "target_bytes": str(target),
                "previous_requested": ko.requested_storage(pvc),
                "phase": "writing",
                "occupies_quota": True,
                "confirm_after": confirm_after,
                "last_error": None,
                "last_note": "已占用名额，准备写入扩容请求",
                "updated_at": _iso(now),
                "instance": self.instance_id,
                "write_attempts": int(base.get("write_attempts", 0)) + 1,
            }
            records[request.request_id] = record
            claimed.update(record)

        self._mutate(mutate)
        log.info(
            "claimed expansion gate for class=%s request=%s pvc=%s/%s",
            storage_class,
            request.request_id,
            request.namespace,
            request.name,
        )
        return claimed

    # ------------------------------------------------------------------ write
    def _write(
        self,
        record: dict[str, Any],
        request: ExpansionRequest,
        pvc: dict[str, Any],
        target: Decimal,
    ) -> ExpansionResponse:
        for attempt in range(MAX_WRITE_ATTEMPTS):
            body = {
                "metadata": {
                    "uid": (pvc.get("metadata") or {}).get("uid"),
                    "resourceVersion": (pvc.get("metadata") or {}).get("resourceVersion"),
                },
                "spec": {
                    "resources": {"requests": {"storage": record["target"]}}
                },
            }
            try:
                updated = self.kube.update_pvc(
                    request.namespace, request.name, body
                )
            except Conflict as exc:
                # Someone else touched the claim. Re-read; if it is still the
                # same PVC, retry with the fresh resourceVersion. If not, we
                # can no longer verify anything, so keep holding the gate.
                latest, _ = self._read_pvc(request.namespace, request.name)
                if latest is None or _pvc_uid(latest) != request.uid:
                    record = self._persist(
                        record,
                        phase="unconfirmed",
                        occupies=True,
                        note="写入时 PVC 已变化或消失，待核实",
                        last_error=str(exc),
                        bump_confirm=True,
                    )
                    return self._response(
                        record,
                        phase="unconfirmed",
                        occupies=True,
                        status_text=_status_text("unconfirmed", record["last_note"]),
                        pvc=latest,
                    )
                pvc = latest
                if attempt == MAX_WRITE_ATTEMPTS - 1:
                    record = self._persist(
                        record,
                        phase="unconfirmed",
                        occupies=True,
                        note="持续冲突，写入结果待核实",
                        last_error=str(exc),
                        bump_confirm=True,
                    )
                    return self._response(
                        record,
                        phase="unconfirmed",
                        occupies=True,
                        status_text=_status_text("unconfirmed", record["last_note"]),
                        pvc=pvc,
                    )
                continue
            except Ambiguous as exc:
                # The write may or may not have been applied. Hold the gate
                # and demand re-verification after the confirmation delay.
                latest, _ = self._read_pvc(request.namespace, request.name)
                record = self._persist(
                    record,
                    phase="unconfirmed",
                    occupies=True,
                    note="写入回执丢失或超时，结果未明，继续占用名额，待核实",
                    last_error=str(exc),
                    bump_confirm=True,
                )
                return self._response(
                    record,
                    phase="unconfirmed",
                    occupies=True,
                    status_text=_status_text("unconfirmed", record["last_note"]),
                    pvc=latest,
                )
            except NotFound as exc:
                self._persist(
                    record, phase="not_applied", occupies=False,
                    note="PVC 不存在，写入未生效", last_error=str(exc),
                )
                raise ApiError(
                    404,
                    "pvc_not_found",
                    f"PVC {request.namespace}/{request.name} 不存在，扩容写入未生效，"
                    "名额已释放。",
                    {"namespace": request.namespace, "name": request.name},
                ) from exc
            except (Invalid, KubeError) as exc:
                # Only a definitive 4xx proves nothing was written. Anything
                # else (a KubeError we could not classify, an unreachable
                # server) stays ambiguous: hold the gate and verify.
                if not isinstance(exc, Invalid):
                    latest, _ = self._read_pvc(request.namespace, request.name)
                    record = self._persist(
                        record,
                        phase="unconfirmed",
                        occupies=True,
                        note="写入失败但结果无法确认，继续占用名额，待核实",
                        last_error=str(exc),
                        bump_confirm=True,
                    )
                    return self._response(
                        record,
                        phase="unconfirmed",
                        occupies=True,
                        status_text=_status_text("unconfirmed", record["last_note"]),
                        pvc=latest,
                    )
                self._persist(
                    record, phase="not_applied", occupies=False,
                    note="API server 拒绝写入，已核实未生效",
                    last_error=str(exc),
                )
                raise ApiError(
                    409,
                    "expansion_write_rejected",
                    f"API server 拒绝了扩容写入，已核实未生效并释放名额：{exc}",
                    {"namespace": request.namespace, "name": request.name,
                     "storage_class": record["storage_class"]},
                ) from exc

            phase, occupies, note = self._classify(record, updated, trust=True)
            if phase == "not_applied":
                # The API server returned the patched object, so the write did
                # take effect; a mismatch means the server did something we do
                # not understand. Never release the gate on that basis.
                record = self._persist(
                    record,
                    phase="unconfirmed",
                    occupies=True,
                    note="API server 返回成功但申请容量未反映目标，待人工核实",
                    last_error=None,
                )
                raise ApiError(
                    502,
                    "expansion_not_reflected",
                    "API server 报告写入成功，但 PVC 申请容量未反映目标，"
                    "名额保持占用，请人工核实。",
                    {"namespace": request.namespace, "name": request.name,
                     "target": record["target"]},
                )
            record = self._persist(
                record,
                phase=phase,
                occupies=occupies,
                note=note,
                clear_error=True,
            )
            log.info(
                "expansion write request=%s pvc=%s/%s target=%s -> phase=%s",
                request.request_id,
                request.namespace,
                request.name,
                record["target"],
                phase,
            )
            return self._response(
                record,
                phase=phase,
                occupies=occupies,
                status_text=_status_text(phase, note),
                pvc=updated,
            )

        raise ApiError(  # pragma: no cover - loop always returns or raises
            409, "concurrent_update", "持续冲突导致扩容写入未完成，请重试。"
        )

    # ------------------------------------------------------------- verification
    def _classify(
        self,
        record: dict[str, Any],
        pvc: dict[str, Any] | None,
        *,
        trust: bool = False,
    ) -> tuple[Phase, bool, str]:
        """Decide a record's effective phase from live cluster state."""
        if pvc is None:
            return "unconfirmed", True, "PVC 不存在或已被删除，无法核实写入结果，待人工确认"
        if _pvc_uid(pvc) != record["uid"]:
            return (
                "unconfirmed",
                True,
                f"同名 PVC 的 UID 已变为 {_pvc_uid(pvc)}，无法核实原申请，待人工确认",
            )

        target = Decimal(str(record["target_bytes"]))
        actual = ko.parse_storage(ko.actual_capacity(pvc))
        requested = ko.parse_storage(ko.requested_storage(pvc))

        if actual is not None and actual >= target:
            return (
                "completed",
                False,
                f"实际容量 {ko.actual_capacity(pvc)} 已达到目标 {record['target']}",
            )
        if requested is None:
            return "unconfirmed", True, "无法读取 PVC 当前申请容量，待核实"
        if requested >= target:
            return (
                "expanding",
                True,
                f"申请容量已为 {ko.requested_storage(pvc)}，等待实际容量达到 "
                f"{record['target']} 后再放行下一笔",
            )

        # The claim still asks for less than the target, so the write has not
        # taken effect *as far as this read can tell*. During the confirmation
        # window an in-flight or timed-out write could still land, so hold.
        confirm_after = _parse_iso(record.get("confirm_after"))
        if not trust and confirm_after is not None and self._clock() < confirm_after:
            return (
                "unconfirmed",
                True,
                f"写入结果未明，待核实（{_iso(confirm_after)} 后可重新核实），继续占用名额",
            )
        return (
            "not_applied",
            False,
            f"已核实写入未生效：申请容量仍为 {ko.requested_storage(pvc)}，"
            f"低于目标 {record['target']}",
        )

    # ------------------------------------------------------------------- state
    def _find_record(self, request_id: str) -> dict[str, Any] | None:
        cm = self._load_state(create=False)
        if cm is None:
            return None
        return _parse_records(cm).get(request_id)

    def _find_holder(self, storage_class: str) -> dict[str, Any] | None:
        cm = self._load_state(create=False)
        if cm is None:
            return None
        return _holder(_parse_records(cm), storage_class, "")

    def _load_state(self, *, create: bool = True) -> dict[str, Any] | None:
        try:
            return self.kube.get_config_map(self.state_namespace, self.state_configmap)
        except NotFound:
            if not create:
                return None
            body = {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": self.state_configmap,
                    "namespace": self.state_namespace,
                    "labels": {"app.kubernetes.io/managed-by": "pvc-expansion"},
                },
                "data": {},
            }
            try:
                return self.kube.create_config_map(self.state_namespace, body)
            except Conflict:
                # Another replica created it first; use theirs.
                try:
                    return self.kube.get_config_map(
                        self.state_namespace, self.state_configmap
                    )
                except KubeError as exc:
                    raise _kube_api_error(exc, "读取扩容协调状态失败") from exc
            except Ambiguous as exc:
                # The create may have succeeded without a receipt. Re-read.
                try:
                    return self.kube.get_config_map(
                        self.state_namespace, self.state_configmap
                    )
                except NotFound:
                    raise ApiError(
                        503,
                        "state_unconfirmed",
                        "扩容协调状态 ConfigMap 创建结果未明，请重试。",
                        {"configmap": self.state_configmap},
                    ) from exc
            except KubeError as exc:
                raise _kube_api_error(exc, "创建扩容协调状态失败") from exc
        except KubeError as exc:
            raise _kube_api_error(exc, "读取扩容协调状态失败") from exc

    def _mutate(self, mutator: Callable[[dict[str, dict[str, Any]]], None]) -> dict[str, dict[str, Any]]:
        """Apply ``mutator`` to the record set under an optimistic precondition."""
        for _attempt in range(MAX_STATE_ATTEMPTS):
            cm = self._load_state()
            records = _parse_records(cm)
            mutator(records)
            data: dict[str, Any] = {}
            for request_id, record in records.items():
                data[_record_key(request_id)] = json.dumps(
                    record, ensure_ascii=False, sort_keys=True
                )
            meta = cm.get("metadata") or {}
            body = {
                "metadata": {
                    "uid": meta.get("uid"),
                    "resourceVersion": meta.get("resourceVersion"),
                },
                "data": data,
            }
            try:
                updated = self.kube.update_config_map(
                    self.state_namespace, self.state_configmap, body
                )
                return _parse_records(updated)
            except Conflict:
                continue
            except Ambiguous as exc:
                # Our mutation may or may not be recorded. Re-read and decide:
                # the next loop iteration re-applies an idempotent mutation.
                log.warning("ambiguous state write, re-reading: %s", exc)
                continue
            except KubeError as exc:
                raise _kube_api_error(exc, "写入扩容协调状态失败") from exc
        raise ApiError(
            409,
            "concurrent_update",
            "扩容协调状态正被频繁修改，本次操作未完成，请重试。",
        )

    def _persist(
        self,
        record: dict[str, Any],
        *,
        phase: Phase,
        occupies: bool,
        note: str,
        last_error: str | None = None,
        clear_error: bool = False,
        bump_confirm: bool = False,
    ) -> dict[str, Any]:
        now = self._clock()
        updated = {
            **record,
            "phase": phase,
            "occupies_quota": occupies,
            "last_note": note,
            "updated_at": _iso(now),
            "instance": self.instance_id,
        }
        if bump_confirm:
            updated["confirm_after"] = _iso(
                now + timedelta(seconds=self.confirm_delay)
            )
        if clear_error:
            updated["last_error"] = None
        elif last_error is not None:
            updated["last_error"] = last_error

        def mutate(records: dict[str, dict[str, Any]]) -> None:
            current = records.get(record["request_id"])
            if current is not None:
                # A newer write attempt (e.g. another replica resumed the same
                # request) supersedes this older verification result.
                if int(current.get("write_attempts", 0)) > int(
                    record.get("write_attempts", 0)
                ):
                    return
                if current.get("phase") == "completed":
                    return
            records[record["request_id"]] = updated

        self._mutate(mutate)
        return updated

    # ------------------------------------------------------------------ helpers
    def _require_pvc(self, request: ExpansionRequest) -> dict[str, Any]:
        try:
            pvc = self.kube.get_pvc(request.namespace, request.name)
        except NotFound as exc:
            raise ApiError(
                404,
                "pvc_not_found",
                f"PVC {request.namespace}/{request.name} 不存在。",
                {"namespace": request.namespace, "name": request.name},
            ) from exc
        except KubeError as exc:
            raise _kube_api_error(exc, "读取 PVC 失败") from exc
        if _pvc_uid(pvc) != request.uid:
            raise ApiError(
                409,
                "pvc_uid_mismatch",
                f"同名 PVC 的 UID 为 {_pvc_uid(pvc)}，与请求中的 {request.uid} "
                "不一致（同名 PVC 可能已被重建）。",
                {**_pvc_details(pvc), "requested_uid": request.uid},
            )
        return pvc

    def _read_pvc(
        self, namespace: str, name: str
    ) -> tuple[dict[str, Any] | None, str | None]:
        try:
            return self.kube.get_pvc(namespace, name), None
        except NotFound:
            return None, f"PVC {namespace}/{name} 不存在"
        except KubeError as exc:
            raise _kube_api_error(exc, "读取 PVC 失败") from exc

    def _require_same_request(
        self, record: dict[str, Any], request: ExpansionRequest, target: Decimal
    ) -> None:
        differences: dict[str, Any] = {}
        for field, value in (
            ("namespace", request.namespace),
            ("name", request.name),
            ("uid", request.uid),
        ):
            if record.get(field) != value:
                differences[field] = {"stored": record.get(field), "requested": value}
        stored_target_raw = record.get("target_bytes")
        stored_target = (
            Decimal(str(stored_target_raw)) if stored_target_raw is not None else None
        )
        if stored_target != target:
            differences["target_capacity"] = {
                "stored": record.get("target"),
                "requested": request.target_capacity,
            }
        if differences:
            raise ApiError(
                409,
                "request_id_conflict",
                f"请求号 {request.request_id} 已用于另一组参数，不能更改；"
                "请沿用原参数查询进度，或换一个新的请求号。",
                {"request_id": request.request_id, "differences": differences},
            )

    def _quota(self, storage_class: str) -> ExpansionQuota:
        """Gate snapshot used in responses; derives the effective phase too."""
        holder = self._find_holder(storage_class)
        if holder is None:
            return ExpansionQuota(storage_class=storage_class, occupied=False)
        pvc, _ = self._read_pvc(holder["namespace"], holder["name"])
        phase, occupies, note = self._classify(holder, pvc)
        holder = {**holder, "last_note": note}
        return _quota_from_record(holder, phase, occupies)

    def _response(
        self,
        record: dict[str, Any],
        *,
        phase: Phase,
        occupies: bool,
        status_text: str,
        pvc: dict[str, Any] | None,
        pvc_error: str | None = None,
    ) -> ExpansionResponse:
        quota = self._quota(record["storage_class"])
        summary = _pvc_summary(pvc) if pvc is not None else None
        if summary is None:
            summary = PVCSummary(
                namespace=record["namespace"],
                name=record["name"],
                uid=record["uid"],
                phase=None,
            )
        return ExpansionResponse(
            request_id=record["request_id"],
            status=phase,
            status_text=status_text,
            namespace=record["namespace"],
            name=record["name"],
            uid=record["uid"],
            storage_class=record["storage_class"],
            target_capacity=record["target"],
            requested_storage=pvc and ko.requested_storage(pvc),
            actual_capacity=pvc and ko.actual_capacity(pvc),
            pvc=summary,
            quota=quota,
            verify_after=record.get("confirm_after") if occupies else None,
            last_error=record.get("last_error"),
            message=status_text,
        )


# -------------------------------------------------------------------- helpers
def _parse_target(raw: str) -> Decimal:
    value = ko.parse_storage(raw)
    if value is None:
        raise ApiError(
            422,
            "invalid_request",
            f"目标容量 {raw!r} 不是合法的 Kubernetes 容量（如 100Gi、200G）。",
            {"target_capacity": raw},
        )
    if value <= 0:
        raise ApiError(
            422,
            "invalid_request",
            "目标容量必须大于 0。",
            {"target_capacity": raw},
        )
    return value


def _pvc_uid(pvc: dict[str, Any] | None) -> str | None:
    if not pvc:
        return None
    uid = (pvc.get("metadata") or {}).get("uid")
    return str(uid) if uid is not None else None


def _pvc_details(pvc: dict[str, Any]) -> dict[str, Any]:
    return {
        "namespace": (pvc.get("metadata") or {}).get("namespace"),
        "name": (pvc.get("metadata") or {}).get("name"),
        "uid": pvc.get("metadata", {}).get("uid"),
        "phase": (pvc.get("status") or {}).get("phase"),
        "storage_class": ko.storage_class(pvc),
        "requested_storage": ko.requested_storage(pvc),
        "actual_capacity": ko.actual_capacity(pvc),
    }


def _pvc_summary(pvc: dict[str, Any]) -> PVCSummary:
    meta = pvc.get("metadata") or {}
    requested = ko.requested_storage(pvc)
    actual = ko.actual_capacity(pvc)
    return PVCSummary(
        namespace=str(meta.get("namespace") or ""),
        name=str(meta.get("name") or ""),
        uid=str(meta.get("uid") or ""),
        phase=(pvc.get("status") or {}).get("phase"),
        volume_name=(pvc.get("spec") or {}).get("volumeName"),
        storage_class=ko.storage_class(pvc),
        requested_storage=requested,
        actual_capacity=actual,
        access_modes=ko.access_modes(pvc),
        volume_mode=ko.volume_mode(pvc),
    )


def _parse_records(cm: dict[str, Any]) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for key, raw in (cm.get("data") or {}).items():
        if not key.startswith(RECORD_KEY_PREFIX):
            continue
        try:
            record = json.loads(raw)
        except (TypeError, ValueError):
            log.warning("ignoring malformed expansion record %s", key)
            continue
        if isinstance(record, dict) and record.get("request_id"):
            records[str(record["request_id"])] = record
    return records


def _holder(
    records: dict[str, dict[str, Any]], storage_class: str, exclude_request_id: str
) -> dict[str, Any] | None:
    for record in records.values():
        if record.get("request_id") == exclude_request_id:
            continue
        if record.get("storage_class") != storage_class:
            continue
        if record.get("occupies_quota") or record.get("phase") in OCCUPYING:
            return record
    return None


def _quota_from_record(
    record: dict[str, Any], phase: str, occupies: bool
) -> ExpansionQuota:
    return ExpansionQuota(
        storage_class=record.get("storage_class", ""),
        occupied=bool(occupies),
        holder_request_id=record.get("request_id"),
        holder_phase=phase,
        holder_pvc=f"{record.get('namespace')}/{record.get('name')}",
        since=record.get("created_at"),
    )


def _status_text(phase: str, note: str) -> str:
    prefix = {
        "writing": "扩容请求已占用名额并正在写入。",
        "unconfirmed": "扩容写入结果未明，待核实；名额保持占用。",
        "expanding": "扩容申请已提交，等待实际容量达到目标。",
        "completed": "扩容完成，名额已释放。",
        "not_applied": "已核实扩容写入未生效，名额已释放。",
    }.get(phase, phase)
    return f"{prefix}{note}"


def _kube_api_error(exc: KubeError, what: str) -> ApiError:
    if isinstance(exc, NotFound):
        return ApiError(404, "kube_not_found", f"{what}：{exc}")
    if isinstance(exc, Conflict):
        return ApiError(409, "concurrent_update", f"{what}：{exc}")
    if isinstance(exc, Ambiguous):
        return ApiError(503, "state_unconfirmed", f"{what}：{exc}")
    if isinstance(exc, Unavailable):
        return ApiError(503, "kube_unavailable", f"{what}：{exc}")
    return ApiError(502, "kube_error", f"{what}：{exc}")
