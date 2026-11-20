"""Two replicas, one cluster, one expansion slot -- driven end to end.

This script mirrors what ``docker compose up`` gives you, but without Docker:

* starts the mock Kubernetes API server as its own process (port auto-picked);
* starts **two** API replicas as separate processes, each pointed at the mock
  server through a generated kubeconfig (``KUBE_MODE=remote``, real HTTP);
* drives the Swagger flow against both replicas:

  1. operator A submits (replica 1) -- the mock applies the write but drops the
     receipt -> ``202 unconfirmed``, the slot stays taken;
  2. operator B (replica 2) is rejected while the outcome is unverified;
  3. replica 1 is killed and replaced by a fresh process (restart);
  4. the replacement still blocks B, then re-verifies A's original request;
  5. the storage driver grows the volume -> ``completed`` and the slot frees;
  6. B is admitted, and the slot moves to B;
  7. read-only progress, and a changed-parameter re-submit is a conflict.

Run: ``python3 scripts/expansion_demo.py``
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable

NS = "monitoring"
PVC_A_UID = "4a1c9e70-aaaa-4bbb-8ccc-00000000000a"
PVC_B_UID = "5b2da081-bbbb-4ccc-8ddd-00000000000b"
REQUEST_A = {
    "request_id": "exp-20261120-prom-001",
    "namespace": NS,
    "name": "prometheus-data-0",
    "uid": PVC_A_UID,
    "target_capacity": "250Gi",
}
REQUEST_B = {
    "request_id": "exp-20261120-prom-002",
    "namespace": NS,
    "name": "prometheus-data-1",
    "uid": PVC_B_UID,
    "target_capacity": "250Gi",
}


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def call(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        return exc.code, json.loads(raw) if raw else {}


def wait_ready(url: str, timeout: float = 20) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1):
                return
        except Exception:  # noqa: BLE001 - still starting
            time.sleep(0.1)
    raise RuntimeError(f"{url} did not become ready")


def start_mock(port: int) -> subprocess.Popen:
    env = {**os.environ, "MOCK_SEED": "1", "PYTHONPATH": str(ROOT)}
    proc = subprocess.Popen(
        [PYTHON, "-m", "uvicorn", "mock_kube.app:app", "--host", "127.0.0.1",
         "--port", str(port), "--log-level", "warning"],
        env=env,
        cwd=ROOT,
    )
    wait_ready(f"http://127.0.0.1:{port}/_mock/state")
    return proc


def _write_kubeconfig(mock_port: int, instance: str) -> Path:
    path = ROOT / "data" / f"kubeconfig-{instance}"
    path.parent.mkdir(exist_ok=True)
    path.write_text(
        f"""apiVersion: v1
kind: Config
clusters:
- name: mock
  cluster:
    server: http://127.0.0.1:{mock_port}
contexts:
- name: mock
  context: {{cluster: mock, user: mock}}
current-context: mock
users:
- name: mock
  user: {{}}
"""
    )
    return path


def start_api(port: int, mock_port: int, instance: str) -> subprocess.Popen:
    env = {
        **os.environ,
        "PYTHONPATH": str(ROOT),
        "KUBE_MODE": "remote",
        "KUBECONFIG": str(_write_kubeconfig(mock_port, instance)),
        "KUBE_REQUEST_TIMEOUT": "10",
        "INSTANCE_ID": instance,
        "EXPANSION_STATE_NAMESPACE": NS,
        "EXPANSION_CONFIRM_DELAY_SECONDS": "3",
    }
    proc = subprocess.Popen(
        [PYTHON, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1",
         "--port", str(port), "--log-level", "warning"],
        env=env,
        cwd=ROOT,
    )
    wait_ready(f"http://127.0.0.1:{port}/healthz")
    return proc


def stop(proc: subprocess.Popen | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:  # pragma: no cover - defensive
        proc.kill()


def main() -> int:
    mock_port, port_a, port_b = free_port(), free_port(), free_port()
    mock = start_mock(mock_port)
    base_mock = f"http://127.0.0.1:{mock_port}"
    api_a = start_api(port_a, mock_port, "handover-a")
    api_b = start_api(port_b, mock_port, "handover-b")
    url_a, url_b = f"http://127.0.0.1:{port_a}", f"http://127.0.0.1:{port_b}"
    ok = True

    def show(label: str, status: int, payload: dict, *,
             expect_status=None, expect=None) -> None:
        nonlocal ok
        summary = payload.get("status") or payload.get("error", {}).get("code")
        text = payload.get("status_text") or payload.get("error", {}).get("message", "")
        quota = payload.get("quota")
        holder = ""
        if isinstance(quota, dict):
            holder = (f"  [名额 occupied={quota.get('occupied')} "
                      f"holder={quota.get('holder_request_id')}]")
        print(f"{label:<52} HTTP {status}  {summary}{holder}\n{'':<52}{text}")
        if expect_status is not None and status != expect_status:
            print(f"   !! expected HTTP {expect_status}")
            ok = False
        if expect is not None and summary != expect:
            print(f"   !! expected status {expect!r}")
            ok = False

    try:
        print("== 0. 重置 mock 并载入扩容演示数据（两个 Bound PVC，同一可扩容存储类）")
        call("POST", f"{base_mock}/_mock/reset", {})
        _reseed(base_mock)
        print("   fast-ssd allowVolumeExpansion=true；"
              "prometheus-data-0 / -1 均为 100Gi Bound")

        print("\n== 1. 注入故障后实例 A 提交 001：写入生效，但回执丢失")
        status, _ = call("POST", f"{base_mock}/_mock/faults",
                         {"drop_pvc_patch_receipts": 1})
        print("   POST /_mock/faults {\"drop_pvc_patch_receipts\": 1}")
        status, payload = call("POST", f"{url_a}/expansions", REQUEST_A)
        show("POST :8000 /expansions (A, 001)", status, payload,
             expect_status=202, expect="unconfirmed")
        state = call("GET", f"{base_mock}/_mock/state")[1]
        pvc_a = next(p for p in state["persistentVolumeClaims"]
                     if p["metadata"]["name"] == "prometheus-data-0")
        print("   集群中申请容量 =",
              pvc_a["spec"]["resources"]["requests"]["storage"],
              "（回执虽丢，写入确实生效）")

        print("\n== 2. 实例 B 为同存储类的 PVC B 提交 002 —— 名额仍被占用")
        status, payload = call("POST", f"{url_b}/expansions", REQUEST_B)
        show("POST :8002 /expansions (B, 002)", status, payload,
             expect_status=409, expect="expansion_in_flight")

        print("\n== 3. 杀掉实例 A，另起新进程接管（重启，内存无记录）")
        stop(api_a)
        api_a = start_api(port_a, mock_port, "handover-a-restarted")
        print("   新进程已启动")

        print("\n== 4. 重启后的实例仍挡住 002，并核实 001 的原申请")
        status, payload = call("POST", f"{url_b}/expansions", REQUEST_B)
        show("POST :8002 /expansions (B, 002)", status, payload,
             expect_status=409, expect="expansion_in_flight")
        status, payload = call("POST", f"{url_a}/expansions", REQUEST_A)
        show("POST :8000 /expansions (A, 001, after restart)", status, payload,
             expect_status=202, expect="expanding")

        print("\n== 5. 等待核实窗口过后由存储驱动扩容，再核实 001")
        print("   sleep 4s; POST /_mock/expansions/reconcile")
        time.sleep(4)
        call("POST", f"{base_mock}/_mock/expansions/reconcile", {})
        status, payload = call("POST", f"{url_a}/expansions", REQUEST_A)
        show("POST :8000 /expansions (A, 001, verified)", status, payload,
             expect_status=200, expect="completed")

        print("\n== 6. 名额释放：实例 B 的 002 现在可以提交")
        status, payload = call("POST", f"{url_b}/expansions", REQUEST_B)
        show("POST :8002 /expansions (B, 002)", status, payload,
             expect_status=202, expect="expanding")
        status, payload = call("GET", f"{url_b}/expansions?storage_class=fast-ssd")
        print("   名额查询：", json.dumps(payload, ensure_ascii=False))

        print("\n== 7. 只读进度查询与请求号改参数冲突")
        status, payload = call("GET", f"{url_b}/expansions/exp-20261120-prom-002")
        show("GET :8002 /expansions/{002}", status, payload,
             expect_status=200, expect="expanding")
        changed = {**REQUEST_A, "target_capacity": "300Gi"}
        status, payload = call("POST", f"{url_a}/expansions", changed)
        show("POST :8000 /expansions (A, 001, target changed)", status, payload,
             expect_status=409, expect="request_id_conflict")

        print("\n== 8. 两个实例并发抢同一存储类名额（burst-class，10 路同时提交）")
        ok &= _race_two_replicas(base_mock, url_a, url_b)
    finally:
        stop(api_a)
        stop(api_b)
        stop(mock)

    print("\nEXPANSION DEMO", "OK" if ok else "FAILED")
    return 0 if ok else 1


def _race_two_replicas(base: str, url_a: str, url_b: str) -> bool:
    """Ten different claims of one fresh class hit both replicas at once.

    Exactly one must win the slot; the rest must get ``expansion_in_flight``.
    """
    import threading

    call("POST", f"{base}/_mock/storageclasses",
         {"metadata": {"name": "burst-class"}, "allowVolumeExpansion": True})
    claims = []
    for index in range(10):
        name = f"burst-{index}"
        uid = f"b{index:07d}-0000-4000-8000-000000000000"
        call("POST", f"{base}/_mock/namespaces/{NS}/pvcs", {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": NS, "uid": uid},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "10Gi"}},
                "storageClassName": "burst-class",
            },
            "status": {"phase": "Bound", "capacity": {"storage": "10Gi"}},
        })
        claims.append((name, uid))

    results: list[tuple[int, str]] = []
    lock = threading.Lock()
    gate = threading.Barrier(len(claims))

    def attempt(index: int) -> None:
        name, uid = claims[index]
        body = {
            "request_id": f"burst-req-{index}",
            "namespace": NS,
            "name": name,
            "uid": uid,
            "target_capacity": "20Gi",
        }
        gate.wait()
        status, payload = call("POST", f"{url_a if index % 2 else url_b}/expansions", body)
        with lock:
            results.append((status, payload.get("status")
                            or payload.get("error", {}).get("code", "")))

    threads = [threading.Thread(target=attempt, args=(i,)) for i in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    winners = [r for r in results if r[0] == 202 and r[1] in ("writing", "unconfirmed", "expanding")]
    losers = [r for r in results if r[0] == 409 and r[1] == "expansion_in_flight"]
    unexpected = [r for r in results if r not in winners and r not in losers]
    print(f"   10 路并发结果：{len(winners)} 个拿到名额，{len(losers)} 个 409 expansion_in_flight，"
          f"{len(unexpected)} 个异常")
    if unexpected:
        print("   !! unexpected:", unexpected)
        return False
    if len(winners) != 1 or len(losers) != 9:
        print("   !! 名额没有被唯一占用")
        return False
    quota = call("GET", f"{url_a}/expansions?storage_class=burst-class")[1]
    print("   名额持有者：", quota.get("holder_request_id"),
          "PVC", quota.get("holder_pvc"))
    return quota.get("occupied") is True and len(winners) == 1


def _reseed(base: str) -> None:
    """Reapply the expansion fixtures on a freshly reset mock server."""
    call("POST", f"{base}/_mock/storageclasses",
         {"metadata": {"name": "fast-ssd"}, "allowVolumeExpansion": True})
    for name, uid in (("prometheus-data-0", PVC_A_UID),
                      ("prometheus-data-1", PVC_B_UID)):
        call("POST", f"{base}/_mock/namespaces/{NS}/pvcs", {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": NS, "uid": uid},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "100Gi"}},
                "storageClassName": "fast-ssd",
            },
            "status": {"phase": "Bound", "capacity": {"storage": "100Gi"}},
        })


if __name__ == "__main__":
    raise SystemExit(main())
