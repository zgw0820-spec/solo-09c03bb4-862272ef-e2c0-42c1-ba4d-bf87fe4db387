"""HTTP-level tests for the expansion API.

Covers the FastAPI contract (status codes, error bodies, Swagger) with an
injected store, plus one full-stack run over the real ``kubernetes`` client and
the mock API server, including the injected "write applied, receipt dropped"
fault and a service restart in the middle.
"""

from __future__ import annotations

import json
import socket
import threading
import time
import urllib.error
import urllib.request

import pytest
from fastapi.testclient import TestClient

from app.kube import KubernetesClient
from app.main import create_app as create_handover_app
from mock_kube.app import create_app as create_mock_app
from mock_kube.store import InMemoryKube

NS = "monitoring"
CLASS = "fast-ssd"
PVC_A_UID = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
PVC_B_UID = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"


def _seed(store: InMemoryKube) -> None:
    store.create_storage_class(
        {"metadata": {"name": CLASS}, "allowVolumeExpansion": True}
    )
    for name, uid in (("prometheus-data-0", PVC_A_UID), ("prometheus-data-1", PVC_B_UID)):
        store.create_pvc(
            NS,
            {
                "metadata": {"name": name, "namespace": NS, "uid": uid},
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": "100Gi"}},
                    "storageClassName": CLASS,
                },
                "status": {"phase": "Bound", "capacity": {"storage": "100Gi"}},
            },
        )


BODY_A = {
    "request_id": "exp-001",
    "namespace": NS,
    "name": "prometheus-data-0",
    "uid": PVC_A_UID,
    "target_capacity": "250Gi",
}
BODY_B = {
    "request_id": "exp-002",
    "namespace": NS,
    "name": "prometheus-data-1",
    "uid": PVC_B_UID,
    "target_capacity": "250Gi",
}


@pytest.fixture
def api_client(monkeypatch) -> TestClient:
    monkeypatch.setenv("EXPANSION_CONFIRM_DELAY_SECONDS", "0")
    store = InMemoryKube()
    _seed(store)
    app = create_handover_app(kube=store)
    with TestClient(app) as client:
        client.store = store  # type: ignore[attr-defined]
        yield client


# ----------------------------------------------------------------- contract
def test_openapi_lists_expansion_routes(api_client):
    schema = api_client.get("/openapi.json").json()
    assert "/expansions" in schema["paths"]
    assert "post" in schema["paths"]["/expansions"]
    assert "/expansions/{request_id}" in schema["paths"]


def test_submit_returns_202_while_gate_held_then_200(api_client):
    first = api_client.post("/expansions", json=BODY_A)
    assert first.status_code == 202, first.text
    payload = first.json()
    assert payload["status"] == "expanding"
    assert payload["quota"]["occupied"] is True

    api_client.store.reconcile_expansions()
    done = api_client.post("/expansions", json=BODY_A)
    assert done.status_code == 200
    assert done.json()["status"] == "completed"
    assert done.json()["quota"]["occupied"] is False


def test_gate_blocks_second_claim_with_details(api_client):
    api_client.post("/expansions", json=BODY_A)
    response = api_client.post("/expansions", json=BODY_B)
    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "expansion_in_flight"
    assert error["details"]["holder"]["holder_request_id"] == "exp-001"


def test_quota_endpoint_reports_the_holder(api_client):
    empty = api_client.get("/expansions", params={"storage_class": CLASS}).json()
    assert empty["occupied"] is False

    api_client.post("/expansions", json=BODY_A)

    held = api_client.get("/expansions", params={"storage_class": CLASS}).json()
    assert held["occupied"] is True
    assert held["holder_request_id"] == "exp-001"
    assert held["holder_pvc"] == "monitoring/prometheus-data-0"


def test_status_endpoint_is_read_only(api_client):
    api_client.post("/expansions", json=BODY_A)
    rv = api_client.store.get_pvc(NS, "prometheus-data-0")["metadata"]["resourceVersion"]
    cm_rv = api_client.store.get_config_map(
        NS, "pvc-expansion-state"
    )["metadata"]["resourceVersion"]

    response = api_client.get("/expansions/exp-001")
    assert response.status_code == 200
    assert response.json()["status"] == "expanding"

    assert api_client.store.get_pvc(NS, "prometheus-data-0")["metadata"][
        "resourceVersion"
    ] == rv
    assert api_client.store.get_config_map(NS, "pvc-expansion-state")["metadata"][
        "resourceVersion"
    ] == cm_rv


def test_status_unknown_request_is_404(api_client):
    response = api_client.get("/expansions/nope")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "request_not_found"


def test_changed_params_conflict(api_client):
    api_client.post("/expansions", json=BODY_A)
    response = api_client.post("/expansions", json={**BODY_A, "target_capacity": "300Gi"})
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "request_id_conflict"


@pytest.mark.parametrize(
    "body, expected",
    [
        ({"request_id": BODY_A["request_id"]}, "invalid_request"),
        ({**BODY_A, "target_capacity": "abc"}, "invalid_request"),
        ({**BODY_A, "uid": "ffffffff-ffff-4fff-8fff-ffffffffffff"}, "pvc_uid_mismatch"),
        ({**BODY_A, "target_capacity": "10Gi"}, "target_not_greater"),
    ],
)
def test_admission_errors(api_client, body, expected):
    response = api_client.post("/expansions", json=body)
    error = response.json()["error"]
    assert error["code"] == expected


def test_unknown_field_is_rejected(api_client):
    response = api_client.post("/expansions", json={**BODY_A, "force": True})
    assert response.status_code == 422


# ---------------------------------------------------- full stack + fault + restart
def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def live_mock():
    import uvicorn

    port = _free_port()
    store = InMemoryKube()
    _seed(store)
    app = create_mock_app(store=store)
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not server.started and time.time() < deadline:
        time.sleep(0.02)
    if not server.started:
        raise RuntimeError("mock API server failed to start")
    yield store, f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=5)


def _wire_client(base_url: str) -> KubernetesClient:
    from kubernetes import client as k8s_client

    configuration = k8s_client.Configuration()
    configuration.host = base_url
    configuration.api_key = {}
    configuration.api_key_prefix = {}
    api = k8s_client.CoreV1Api(k8s_client.ApiClient(configuration))
    return KubernetesClient(core=api)


def _post(url: str, body: dict) -> tuple[int, dict]:
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def test_full_stack_write_applied_receipt_lost_then_restart(live_mock, monkeypatch):
    """The exact scenario asked for, over the real HTTP path.

    1. operator A submits for PVC A -> write applied, but the mock drops the
       receipt (503) -> service reports unconfirmed and keeps the gate;
    2. the API process "restarts" (a brand-new app + client);
    3. operator B is still blocked;
    4. after the confirmation window the restarted process re-reads the PVC,
       proves the original write landed, and only releases once status.capacity
       reaches the target;
    5. operator B can then submit.
    """
    store, base_url = live_mock
    store.reset()
    _seed(store)
    frozen_clock = FakeClock()
    monkeypatch.setenv("EXPANSION_CONFIRM_DELAY_SECONDS", "5")

    def make_app():
        from app.expansion import ExpansionService
        from fastapi import FastAPI

        app = create_handover_app(kube=_wire_client(base_url))
        app.state.expansion = ExpansionService(
            app.state.kube, confirm_delay_seconds=5, clock=frozen_clock,
            instance_id="replica-" + str(time.time()), state_namespace=NS,
        )
        return app

    # ---- 1. write lands, receipt dropped -------------------------------
    status, _ = _post(f"{base_url}/_mock/faults", {"drop_pvc_patch_receipts": 1})
    assert status == 200

    app_a = make_app()
    with TestClient(app_a) as client_a:
        first = client_a.post("/expansions", json=BODY_A)
        # 202 = the gate is still held (writing/unconfirmed/expanding).
        assert first.status_code == 202, first.text
        payload = first.json()
        assert payload["status"] == "unconfirmed"
        assert payload["quota"]["occupied"] is True
        assert payload["verify_after"] is not None
        assert "待核实" in payload["status_text"]

    assert (
        store.get_pvc(NS, "prometheus-data-0")["spec"]["resources"]["requests"][
            "storage"
        ]
        == "250Gi"
    ), "the write really did land despite the lost receipt"

    # ---- 2/3. restart; the new process knows nothing in memory ---------
    frozen_clock.advance(1)
    app_b = make_app()
    with TestClient(app_b) as client_b:
        from app.errors import ApiError
        from app.schemas import ExpansionRequest

        with pytest.raises(ApiError) as blocked:
            client_b.app.state.expansion.submit(
                ExpansionRequest(**BODY_B)
            )
        assert blocked.value.code == "expansion_in_flight"

        # Re-submitting the original request before the window: the request
        # is verifiably in place, so it is "expanding" and nothing is rewritten.
        rv = store.get_pvc(NS, "prometheus-data-0")["metadata"]["resourceVersion"]
        progress = client_b.post("/expansions", json=BODY_A)
        assert progress.status_code == 202
        assert progress.json()["status"] == "expanding"
        assert (
            store.get_pvc(NS, "prometheus-data-0")["metadata"]["resourceVersion"]
            == rv
        ), "restart must not rewrite the PVC"

        # ---- 4. driver grows the volume; gate releases ------------------
        frozen_clock.advance(10)
        urllib.request.urlopen(
            urllib.request.Request(
                f"{base_url}/_mock/expansions/reconcile", method="POST"
            )
        ).read()
        verified = client_b.post("/expansions", json=BODY_A)
        assert verified.status_code == 200, verified.text
        assert verified.json()["status"] == "completed"
        assert verified.json()["quota"]["occupied"] is False

        # ---- 5. the next claim proceeds ---------------------------------
        following = client_b.post("/expansions", json=BODY_B)
        assert following.status_code == 202, following.text
        assert following.json()["quota"]["holder_request_id"] == "exp-002"


def test_full_stack_verified_no_effect_releases_gate(live_mock, monkeypatch):
    """A write that was rejected outright frees the gate at once (over HTTP)."""
    store, base_url = live_mock
    store.reset()
    _seed(store)
    monkeypatch.setenv("EXPANSION_CONFIRM_DELAY_SECONDS", "0")

    app = create_handover_app(kube=_wire_client(base_url))
    with TestClient(app) as client:
        # Shrink attempt is rejected by the API server (spec is immutable for
        # anything but growing requests), proving 4xx -> gate released.
        response = client.post("/expansions", json={**BODY_A, "target_capacity": "100Gi"})
        # The service rejects this itself before writing, with a precise code.
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "target_not_greater"

        ok = client.post("/expansions", json=BODY_A)
        assert ok.status_code == 202

        other = client.post("/expansions", json=BODY_B)
        assert other.status_code == 409
        assert other.json()["error"]["code"] == "expansion_in_flight"


class FakeClock:
    def __init__(self):
        from datetime import datetime, timezone

        self.now = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        from datetime import timedelta

        self.now += timedelta(seconds=seconds)
