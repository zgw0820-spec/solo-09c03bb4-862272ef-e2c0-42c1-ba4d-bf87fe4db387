"""Expansion rules: admission, the per-class gate, and verification.

The interesting cases are the ones operators get wrong in a runbook:

* two claims of the same class -- only one may be in flight, regardless of
  which API replica accepted the request;
* the write times out -- we must *not* assume it failed;
* the process restarts between the write and the verification;
* the same request id is retried -- it must never write twice, and a changed
  parameter set must be a conflict rather than a silent re-target.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.errors import ApiError
from app.expansion import ExpansionService
from app.kube import Ambiguous, Conflict, Invalid, KubeError
from app.schemas import ExpansionRequest
from mock_kube.store import InMemoryKube

NS = "monitoring"
CLASS = "fast-ssd"
FROZEN = "slow-hdd"
PVC_A_UID = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
PVC_B_UID = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"


class FakeClock:
    def __init__(self) -> None:
        self.now = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def _seed(kube: InMemoryKube) -> None:
    kube.create_storage_class(
        {"metadata": {"name": CLASS}, "allowVolumeExpansion": True}
    )
    kube.create_storage_class(
        {"metadata": {"name": FROZEN}, "allowVolumeExpansion": False}
    )
    for name, uid, capacity, storage_class in (
        ("prometheus-data-0", PVC_A_UID, "100Gi", CLASS),
        ("prometheus-data-1", PVC_B_UID, "100Gi", CLASS),
        ("thanos-compact-0", "cccccccc-3333-4333-8333-cccccccccccc", "50Gi", FROZEN),
    ):
        kube.create_pvc(
            NS,
            {
                "metadata": {"name": name, "namespace": NS, "uid": uid},
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": capacity}},
                    "storageClassName": storage_class,
                },
                "status": {"phase": "Bound", "capacity": {"storage": capacity}},
            },
        )


@pytest.fixture
def kube() -> InMemoryKube:
    store = InMemoryKube()
    _seed(store)
    return store


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def _service(kube: InMemoryKube, clock: FakeClock, *, instance: str = "replica-a") -> ExpansionService:
    return ExpansionService(
        kube,
        confirm_delay_seconds=5,
        instance_id=instance,
        clock=clock,
        state_namespace=NS,
    )


def _request(**overrides) -> ExpansionRequest:
    data = {
        "request_id": "exp-001",
        "namespace": NS,
        "name": "prometheus-data-0",
        "uid": PVC_A_UID,
        "target_capacity": "250Gi",
    }
    data.update(overrides)
    return ExpansionRequest(**data)


def _pvc(kube: InMemoryKube, name: str) -> dict:
    return kube.get_pvc(NS, name)


def _requested(kube: InMemoryKube, name: str) -> str:
    return _pvc(kube, name)["spec"]["resources"]["requests"]["storage"]


# ------------------------------------------------------------------- admission
def test_happy_path_writes_and_keeps_the_gate_until_actual_capacity(kube, clock):
    service = _service(kube, clock)

    response = service.submit(_request())

    assert response.status == "expanding"
    assert response.quota.occupied is True
    assert response.quota.holder_request_id == "exp-001"
    assert _requested(kube, "prometheus-data-0") == "250Gi"
    # The request moved, but the volume is not actually bigger yet.
    assert response.actual_capacity == "100Gi"

    kube.reconcile_expansions()
    done = service.submit(_request())

    assert done.status == "completed"
    assert done.quota.occupied is False
    assert done.actual_capacity == "250Gi"


@pytest.mark.parametrize(
    "phase,name,target",
    [("Pending", "prometheus-data-0", "250Gi")],
)
def test_requires_bound_pvc(kube, clock, phase, name, target):
    pvc = kube.get_pvc(NS, name)
    pvc["status"]["phase"] = phase
    kube._pvcs[(NS, name)] = pvc
    service = _service(kube, clock)

    with pytest.raises(ApiError) as exc:
        service.submit(_request(target_capacity=target))
    assert exc.value.code == "pvc_not_bound"
    assert _requested(kube, name) == "100Gi", "rejection must not write"


def test_disabled_storage_class_is_rejected(kube, clock):
    service = _service(kube, clock)
    with pytest.raises(ApiError) as exc:
        service.submit(
            _request(
                request_id="exp-frozen",
                name="thanos-compact-0",
                uid="cccccccc-3333-4333-8333-cccccccccccc",
                target_capacity="100Gi",
            )
        )
    assert exc.value.code == "storage_class_expansion_disabled"


@pytest.mark.parametrize("target", ["100Gi", "50Gi", "1G"])
def test_target_must_be_greater_than_current_request(kube, clock, target):
    service = _service(kube, clock)
    with pytest.raises(ApiError) as exc:
        service.submit(_request(target_capacity=target))
    assert exc.value.code == "target_not_greater"


def test_uid_mismatch_is_rejected(kube, clock):
    service = _service(kube, clock)
    with pytest.raises(ApiError) as exc:
        service.submit(_request(uid="99999999-9999-4999-8999-999999999999"))
    assert exc.value.code == "pvc_uid_mismatch"


def test_unknown_pvc_is_404(kube, clock):
    service = _service(kube, clock)
    with pytest.raises(ApiError) as exc:
        service.submit(_request(name="nope"))
    assert exc.value.status_code == 404
    assert exc.value.code == "pvc_not_found"


def test_invalid_quantity_is_422(kube, clock):
    service = _service(kube, clock)
    with pytest.raises(ApiError) as exc:
        service.submit(_request(target_capacity="lots"))
    assert exc.value.status_code == 422
    assert exc.value.code == "invalid_request"


# --------------------------------------------------------------------- the gate
def test_second_claim_of_same_class_is_blocked_until_verified(kube, clock):
    service = _service(kube, clock)
    service.submit(_request())

    with pytest.raises(ApiError) as exc:
        service.submit(
            _request(
                request_id="exp-002",
                name="prometheus-data-1",
                uid=PVC_B_UID,
            )
        )
    assert exc.value.status_code == 409
    assert exc.value.code == "expansion_in_flight"
    assert exc.value.details["holder"]["holder_request_id"] == "exp-001"
    assert _requested(kube, "prometheus-data-1") == "100Gi", "blocked claim untouched"


def test_gate_is_scoped_per_storage_class(kube, clock):
    kube.create_storage_class(
        {"metadata": {"name": "other-class"}, "allowVolumeExpansion": True}
    )
    kube.create_pvc(
        NS,
        {
            "metadata": {"name": "loki-0", "namespace": NS, "uid": "dddddddd-4444-4444-8444-dddddddddddd"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "10Gi"}},
                "storageClassName": "other-class",
            },
            "status": {"phase": "Bound", "capacity": {"storage": "10Gi"}},
        },
    )
    service = _service(kube, clock)
    service.submit(_request())
    response = service.submit(
        _request(
            request_id="exp-loki",
            name="loki-0",
            uid="dddddddd-4444-4444-8444-dddddddddddd",
            target_capacity="20Gi",
        )
    )
    assert response.status == "expanding"
    assert response.quota.occupied is True


def test_two_replicas_share_the_gate(kube, clock):
    """Two ExpansionService instances over the same cluster see one gate."""
    replica_a = _service(kube, clock, instance="replica-a")
    replica_b = _service(kube, clock, instance="replica-b")

    replica_a.submit(_request())

    with pytest.raises(ApiError) as exc:
        replica_b.submit(
            _request(request_id="exp-b", name="prometheus-data-1", uid=PVC_B_UID)
        )
    assert exc.value.code == "expansion_in_flight"

    # The same request id from the other replica is progress, not a second write.
    patches_before = len(kube._pvcs)
    progress = replica_b.submit(_request())
    assert progress.status == "expanding"
    assert progress.quota.holder_request_id == "exp-001"
    assert len(kube._pvcs) == patches_before


def test_concurrent_first_submits_from_two_replicas_only_one_wins(kube, clock):
    replica_a = _service(kube, clock, instance="replica-a")
    replica_b = _service(kube, clock, instance="replica-b")
    barrier = threading.Barrier(2)
    outcomes: dict[str, object] = {}

    def attempt(label, service, request):
        barrier.wait()
        try:
            outcomes[label] = service.submit(request).status
        except ApiError as exc:
            outcomes[label] = exc.code

    requests = {
        "a": _request(request_id="exp-a", name="prometheus-data-0", uid=PVC_A_UID),
        "b": _request(request_id="exp-b", name="prometheus-data-1", uid=PVC_B_UID),
    }
    threads = [
        threading.Thread(target=attempt, args=("a", replica_a, requests["a"])),
        threading.Thread(target=attempt, args=("b", replica_b, requests["b"])),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    winners = [v for v in outcomes.values() if v == "expanding"]
    losers = [v for v in outcomes.values() if v == "expansion_in_flight"]
    assert len(winners) == 1, outcomes
    assert len(losers) == 1, outcomes


def test_ten_way_burst_across_two_replicas_has_exactly_one_winner(kube, clock):
    """Different request ids for one class: the gate admits exactly one."""
    replicas = [
        _service(kube, clock, instance=f"replica-{i % 2}") for i in range(2)
    ]
    for index in range(10):
        kube.create_pvc(
            NS,
            {
                "metadata": {
                    "name": f"burst-{index}",
                    "namespace": NS,
                    "uid": f"b{index:07d}-0000-4000-8000-000000000000",
                },
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": "100Gi"}},
                    "storageClassName": CLASS,
                },
                "status": {"phase": "Bound", "capacity": {"storage": "100Gi"}},
            },
        )
    barrier = threading.Barrier(10)
    lock = threading.Lock()
    results: list[tuple[str, object]] = []

    def attempt(index: int) -> None:
        request = _request(
            request_id=f"burst-{index}",
            name=f"burst-{index}",
            uid=f"b{index:07d}-0000-4000-8000-000000000000",
        )
        barrier.wait()
        try:
            status = replicas[index % 2].submit(request).status
        except ApiError as exc:
            status = exc.code
        with lock:
            results.append((request.request_id, status))

    threads = [threading.Thread(target=attempt, args=(i,)) for i in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    winners = [r for r in results if r[1] == "expanding"]
    losers = [r for r in results if r[1] == "expansion_in_flight"]
    assert len(winners) == 1, results
    assert len(losers) == 9, results
    quota = replicas[0].quota(CLASS)
    assert quota.occupied is True
    assert quota.holder_request_id == winners[0][0]
    # Only the winning claim was written to.
    grown = [
        i for i in range(10)
        if _requested(kube, f"burst-{i}") == "250Gi"
    ]
    assert len(grown) == 1


# ----------------------------------------------------------- retry semantics
def test_same_request_id_same_params_never_writes_twice(kube, clock):
    service = _service(kube, clock)
    service.submit(_request())
    rv = _pvc(kube, "prometheus-data-0")["metadata"]["resourceVersion"]

    second = service.submit(_request())
    third = service.submit(_request())

    assert second.status == third.status == "expanding"
    assert _pvc(kube, "prometheus-data-0")["metadata"]["resourceVersion"] == rv


def test_same_request_id_changed_params_is_conflict(kube, clock):
    service = _service(kube, clock)
    service.submit(_request())

    with pytest.raises(ApiError) as exc:
        service.submit(_request(target_capacity="300Gi"))
    assert exc.value.code == "request_id_conflict"
    assert "target_capacity" in exc.value.details["differences"]

    with pytest.raises(ApiError) as name_exc:
        service.submit(_request(name="prometheus-data-1"))
    assert name_exc.value.code == "request_id_conflict"


def test_status_is_read_only(kube, clock):
    service = _service(kube, clock)
    service.submit(_request())
    rv = _pvc(kube, "prometheus-data-0")["metadata"]["resourceVersion"]
    cm_rv = kube.get_config_map(NS, "pvc-expansion-state")["metadata"]["resourceVersion"]

    response = service.status("exp-001")

    assert response.status == "expanding"
    assert _pvc(kube, "prometheus-data-0")["metadata"]["resourceVersion"] == rv
    assert kube.get_config_map(NS, "pvc-expansion-state")["metadata"]["resourceVersion"] == cm_rv


def test_status_of_a_new_class_does_not_create_state(kube, clock):
    service = _service(kube, clock)
    quota = service.quota(CLASS)
    assert quota.occupied is False
    with pytest.raises(Exception):
        kube.get_config_map(NS, "pvc-expansion-state")


def test_completed_record_does_not_follow_a_rebuilt_pvc(kube, clock):
    service = _service(kube, clock)
    service.submit(_request())
    kube.reconcile_expansions()
    assert service.submit(_request()).status == "completed"

    kube.delete_pvc(NS, "prometheus-data-0")
    kube.create_pvc(
        NS,
        {
            "metadata": {"name": "prometheus-data-0", "namespace": NS, "uid": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "100Gi"}},
                "storageClassName": CLASS,
            },
            "status": {"phase": "Bound", "capacity": {"storage": "100Gi"}},
        },
    )

    with pytest.raises(ApiError) as exc:
        service.submit(_request())
    assert exc.value.code == "pvc_uid_mismatch"


def test_status_unknown_request_is_404(kube, clock):
    service = _service(kube, clock)
    with pytest.raises(ApiError) as exc:
        service.status("never-seen")
    assert exc.value.status_code == 404


# -------------------------------------------------- uncertain write & restart
class DroppedReceiptKube(InMemoryKube):
    """Applies the PVC patch, then raises as if the response never arrived."""

    def __init__(self) -> None:
        super().__init__()
        self.drop_next = 0

    def update_pvc(self, namespace, name, body):
        result = super().update_pvc(namespace, name, body)
        if self.drop_next > 0:
            self.drop_next -= 1
            raise Ambiguous("connection reset after the write was applied")
        return result


def test_dropped_receipt_holds_the_gate_and_is_unconfirmed(clock):
    kube = DroppedReceiptKube()
    _seed(kube)
    kube.drop_next = 1
    service = _service(kube, clock)

    response = service.submit(_request())

    assert response.status == "unconfirmed"
    assert response.quota.occupied is True
    assert response.verify_after is not None
    # The write really did land, even though we got no receipt.
    assert _requested(kube, "prometheus-data-0") == "250Gi"

    # ... and a second claim is still blocked.
    with pytest.raises(ApiError) as exc:
        service.submit(
            _request(request_id="exp-b", name="prometheus-data-1", uid=PVC_B_UID)
        )
    assert exc.value.code == "expansion_in_flight"


def test_dropped_receipt_full_recovery_after_restart(clock):
    """Write applied, receipt lost, API process restarted, then verified.

    This is the scenario the brief calls out explicitly. The new process has
    no memory, so everything has to come back from the ConfigMap and a fresh
    read of the PVC.
    """
    kube = DroppedReceiptKube()
    _seed(kube)
    kube.drop_next = 1
    first_process = _service(kube, clock, instance="replica-a")
    lost = first_process.submit(_request())
    assert lost.status == "unconfirmed"
    assert _requested(kube, "prometheus-data-0") == "250Gi"

    # A replica dying within the confirmation window must not release anything.
    # The new process sees requests == target, i.e. the original write is
    # verifiably in place, so it reports "expanding" rather than "unconfirmed"
    # -- but it neither rewrites nor frees the gate.
    clock.advance(1)
    restarted = _service(kube, clock, instance="replica-b")
    still = restarted.submit(_request())
    assert still.status == "expanding"
    assert still.quota.occupied is True
    assert _requested(kube, "prometheus-data-0") == "250Gi"

    # A different claim is still blocked while the expansion is unresolved.
    with pytest.raises(ApiError) as blocked:
        restarted.submit(
            _request(request_id="exp-b", name="prometheus-data-1", uid=PVC_B_UID)
        )
    assert blocked.value.code == "expansion_in_flight"

    # Driver grows the volume; the same request id verifies and releases.
    clock.advance(10)
    kube.reconcile_expansions()
    verified = restarted.submit(_request())
    assert verified.status == "completed"
    assert verified.quota.occupied is False
    assert verified.actual_capacity == "250Gi"

    # Now the next claim proceeds -- and only now.
    following = restarted.submit(
        _request(request_id="exp-b", name="prometheus-data-1", uid=PVC_B_UID)
    )
    assert following.status == "expanding"
    assert following.quota.holder_request_id == "exp-b"


def test_restart_within_window_cannot_know_the_write_missed(clock):
    """Receipt lost, write *not* applied, process restarts inside the window.

    The fresh process sees requests < target. Without the confirmation window
    it would wrongly conclude "not applied" and free the gate while the write
    may still be landing on another replica. With the window it reports
    "unconfirmed" and keeps the gate until the window passes.
    """
    kube = DroppedReceiptKube()
    _seed(kube)
    undone = {"n": 0}
    original = kube.update_pvc

    def lost_without_effect(namespace, name, body):
        result = original(namespace, name, body)
        if undone["n"] == 0:
            undone["n"] = 1
            pvc = kube.get_pvc(namespace, name)
            pvc["spec"]["resources"]["requests"]["storage"] = "100Gi"
            kube._pvcs[(namespace, name)] = pvc
            raise Ambiguous("connection reset before the write was applied")
        return result

    kube.update_pvc = lost_without_effect  # type: ignore[method-assign]
    first_process = _service(kube, clock, instance="replica-a")
    assert first_process.submit(_request()).status == "unconfirmed"

    clock.advance(1)
    restarted = _service(kube, clock, instance="replica-b")
    pending = restarted.submit(_request())
    assert pending.status == "unconfirmed", "window still open"
    assert pending.quota.occupied is True
    with pytest.raises(ApiError) as exc:
        restarted.submit(
            _request(request_id="exp-b", name="prometheus-data-1", uid=PVC_B_UID)
        )
    assert exc.value.code == "expansion_in_flight"

    # Window passes; the claim really is still at 100Gi, so the write is
    # verified as not applied and the original request resumes writing under
    # the same request id. A new request may not jump the queue while the
    # holder record is still the one that occupies the gate.
    clock.advance(10)
    after = restarted.submit(_request())
    assert after.status == "expanding"
    assert _requested(kube, "prometheus-data-0") == "250Gi"

    with pytest.raises(ApiError) as blocked:
        restarted.submit(
            _request(request_id="exp-b", name="prometheus-data-1", uid=PVC_B_UID)
        )
    assert blocked.value.code == "expansion_in_flight"


def test_dropped_receipt_resumes_original_request_after_window(clock):
    """No effect observed after the window: re-submit resumes the write."""
    kube = DroppedReceiptKube()
    _seed(kube)

    call_count = {"n": 0}
    original = kube.update_pvc

    def flaky(namespace, name, body):
        call_count["n"] += 1
        result = original(namespace, name, body)
        if call_count["n"] == 1:
            # Undo the effect but still return a lost receipt, so the write
            # really did not take effect.
            pvc = kube.get_pvc(namespace, name)
            pvc["spec"]["resources"]["requests"]["storage"] = "100Gi"
            kube._pvcs[(namespace, name)] = pvc
            raise Ambiguous("connection reset, write not applied")
        return result

    kube.update_pvc = flaky  # type: ignore[method-assign]
    service = _service(kube, clock)
    response = service.submit(_request())
    assert response.status == "unconfirmed"
    assert _requested(kube, "prometheus-data-0") == "100Gi"

    # Before the window, retrying must not write.
    clock.advance(1)
    early = service.submit(_request())
    assert early.status == "unconfirmed"
    assert call_count["n"] == 1

    # After the window, the same request id is allowed to resume the original
    # write (not a new request, not a new gate acquisition conflict).
    clock.advance(10)
    resumed = service.submit(_request())
    assert resumed.status == "expanding"
    assert call_count["n"] == 2
    assert _requested(kube, "prometheus-data-0") == "250Gi"


def test_rejected_write_releases_the_gate_immediately(kube, clock):
    class RejectingKube(InMemoryKube):
        def update_pvc(self, namespace, name, body):
            if name == "prometheus-data-0":
                raise Invalid("spec.resources.requests.storage: Forbidden")
            return super().update_pvc(namespace, name, body)

    rejecting = RejectingKube()
    _seed(rejecting)
    service = _service(rejecting, clock)

    with pytest.raises(ApiError) as exc:
        service.submit(_request())
    assert exc.value.status_code == 409
    assert exc.value.code == "expansion_write_rejected"

    # A definitive 4xx means nothing was written, so the gate is free.
    response = service.submit(
        _request(request_id="exp-b", name="prometheus-data-1", uid=PVC_B_UID)
    )
    assert response.status == "expanding"


def test_verified_no_effect_frees_the_gate_for_the_next_claim(kube, clock):
    """Once 'not applied' is verified, a *different* request may take the slot."""
    service = _service(kube, clock)
    service.submit(_request())
    # Force our own write away, as if it never landed, without a receipt.
    pvc = kube.get_pvc(NS, "prometheus-data-0")
    pvc["spec"]["resources"]["requests"]["storage"] = "100Gi"
    kube._pvcs[(NS, "prometheus-data-0")] = pvc
    # Re-persist an unconfirmed state to model the lost receipt.
    service._persist(
        service._find_record("exp-001"),
        phase="unconfirmed",
        occupies=True,
        note="模拟回执丢失",
        bump_confirm=True,
    )

    with pytest.raises(ApiError) as blocked:
        service.submit(
            _request(request_id="exp-b", name="prometheus-data-1", uid=PVC_B_UID)
        )
    assert blocked.value.code == "expansion_in_flight"

    clock.advance(10)
    following = service.submit(
        _request(request_id="exp-b", name="prometheus-data-1", uid=PVC_B_UID)
    )
    assert following.status == "expanding"
    assert following.quota.holder_request_id == "exp-b"
    assert _requested(kube, "prometheus-data-0") == "100Gi", "old request not rewritten"


def test_retry_after_a_terminal_state_does_not_rewrite(kube, clock):
    service = _service(kube, clock)
    service.submit(_request())
    kube.reconcile_expansions()
    assert service.submit(_request()).status == "completed"

    rv = _pvc(kube, "prometheus-data-0")["metadata"]["resourceVersion"]
    again = service.submit(_request())
    assert again.status == "completed"
    assert _pvc(kube, "prometheus-data-0")["metadata"]["resourceVersion"] == rv


def test_unclassified_write_failure_is_treated_as_unconfirmed(kube, clock):
    """Only a 4xx proves the write did not happen; anything else must verify."""

    class BrokenKube(InMemoryKube):
        def update_pvc(self, namespace, name, body):
            if name == "prometheus-data-0":
                raise KubeError("transport blew up in an unexpected way")
            return super().update_pvc(namespace, name, body)

    broken = BrokenKube()
    _seed(broken)
    service = _service(broken, clock)

    response = service.submit(_request())
    assert response.status == "unconfirmed"
    assert response.quota.occupied is True

    with pytest.raises(ApiError) as exc:
        service.submit(
            _request(request_id="exp-b", name="prometheus-data-1", uid=PVC_B_UID)
        )
    assert exc.value.code == "expansion_in_flight"
