"""Demo fixtures for the local Kubernetes API stand-in.

With ``MOCK_SEED=1`` the mock server starts pre-populated with the scenario an
operator would meet during a monitoring rebuild:

* a ``Released`` ``Retain`` volume that used to belong to a Prometheus claim
  whose PVC has already been deleted,
* a replacement PVC that is pre-bound to that volume but not yet Bound,
* a rival claim and a ``Delete``-policy volume to demonstrate the rejection
  paths.

UIDs are stable, so the JSON bodies in the README and the Swagger examples can
be copied verbatim.
"""

from __future__ import annotations

from typing import Any

from mock_kube.store import InMemoryKube

PROM_PV = "pvc-5f0c1c9e-aaaa-4bbb-8ccc-0123456789ab"
PROM_OLD_UID = "6b1d0f2e-1111-4c2a-9c33-aa11bb22cc33"
PROM_NEW_UID = "9c2e4a71-2222-4d3b-8e55-bb33cc44dd55"
RIVAL_UID = "3d8f6b20-3333-4a7c-9d11-cc55dd66ee77"

DELETE_PV = "pvc-delete-policy-0000-0000-000000000000"
DELETE_OLD_UID = "7a2f3c11-4444-4b5c-8d66-dd77ee88ff99"

VOLUME_MODE_PV = "pvc-wrong-mode-1111-1111-111111111111"
WRONG_MODE_OLD_UID = "8b3a4d22-5555-4c6d-9e77-ee88ff99aa00"

NAMESPACE = "monitoring"
OLD_CLAIM = "prometheus-data"
NEW_CLAIM = "prometheus-data-rebuilt"
RIVAL_CLAIM = "prometheus-data-rival"
WRONG_MODE_CLAIM = "prometheus-data-block"

# --- expansion scenario ------------------------------------------------------
EXPAND_NAMESPACE = "monitoring"
EXPAND_CLASS = "fast-ssd"
EXPAND_CLASS_FROZEN = "slow-hdd"
PVC_A = "prometheus-data-0"
PVC_A_UID = "4a1c9e70-aaaa-4bbb-8ccc-00000000000a"
PVC_B = "prometheus-data-1"
PVC_B_UID = "5b2da081-bbbb-4ccc-8ddd-00000000000b"
PVC_FROZEN = "thanos-compact-0"
PVC_FROZEN_UID = "6c3eb192-cccc-4ddd-9eee-00000000000c"


def _pv(
    name: str,
    *,
    claim_name: str,
    claim_uid: str,
    phase: str = "Released",
    policy: str = "Retain",
    capacity: str = "200Gi",
    storage_class: str = "fast-ssd",
    access_modes: list[str] | None = None,
    volume_mode: str = "Filesystem",
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": name},
        "spec": {
            "capacity": {"storage": capacity},
            "accessModes": access_modes or ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": policy,
            "storageClassName": storage_class,
            "volumeMode": volume_mode,
            "claimRef": {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "namespace": NAMESPACE,
                "name": claim_name,
                "uid": claim_uid,
            },
        },
        "status": {"phase": phase},
    }


def _pvc(
    name: str,
    uid: str,
    *,
    volume_name: str,
    capacity: str = "150Gi",
    storage_class: str = "fast-ssd",
    access_modes: list[str] | None = None,
    volume_mode: str = "Filesystem",
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": name, "namespace": NAMESPACE, "uid": uid},
        "spec": {
            "accessModes": access_modes or ["ReadWriteOnce"],
            "resources": {"requests": {"storage": capacity}},
            "storageClassName": storage_class,
            "volumeMode": volume_mode,
            "volumeName": volume_name,
        },
        "status": {"phase": "Pending"},
    }


def _bound_pvc(
    name: str,
    uid: str,
    *,
    capacity: str,
    storage_class: str = EXPAND_CLASS,
    access_modes: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": name, "namespace": EXPAND_NAMESPACE, "uid": uid},
        "spec": {
            "accessModes": access_modes or ["ReadWriteOnce"],
            "resources": {"requests": {"storage": capacity}},
            "storageClassName": storage_class,
            "volumeMode": "Filesystem",
            "volumeName": f"pv-{name}",
        },
        "status": {"phase": "Bound", "capacity": {"storage": capacity}},
    }


def _storage_class(name: str, allow_expansion: bool) -> dict[str, Any]:
    return {
        "apiVersion": "storage.k8s.io/v1",
        "kind": "StorageClass",
        "metadata": {"name": name},
        "provisioner": "mock.io/local",
        "allowVolumeExpansion": allow_expansion,
        "volumeBindingMode": "WaitForFirstConsumer",
        "reclaimPolicy": "Retain",
    }


DEMO_BODIES = {
    "handover": {
        "pv_name": PROM_PV,
        "old_pvc_uid": PROM_OLD_UID,
        "new_pvc": {"namespace": NAMESPACE, "name": NEW_CLAIM, "uid": PROM_NEW_UID},
    },
    "rival": {
        "pv_name": PROM_PV,
        "old_pvc_uid": PROM_OLD_UID,
        "new_pvc": {"namespace": NAMESPACE, "name": RIVAL_CLAIM, "uid": RIVAL_UID},
    },
    "delete_policy": {
        "pv_name": DELETE_PV,
        "old_pvc_uid": DELETE_OLD_UID,
        "new_pvc": {"namespace": NAMESPACE, "name": NEW_CLAIM, "uid": PROM_NEW_UID},
    },
    "wrong_volume_mode": {
        "pv_name": VOLUME_MODE_PV,
        "old_pvc_uid": WRONG_MODE_OLD_UID,
        "new_pvc": {"namespace": NAMESPACE, "name": WRONG_MODE_CLAIM, "uid": RIVAL_UID},
    },
    # --- expansion demo: both claims live on the same expandable class ------
    "expand_a": {
        "request_id": "exp-20261120-prom-001",
        "namespace": EXPAND_NAMESPACE,
        "name": PVC_A,
        "uid": PVC_A_UID,
        "target_capacity": "250Gi",
    },
    "expand_b": {
        "request_id": "exp-20261120-prom-002",
        "namespace": EXPAND_NAMESPACE,
        "name": PVC_B,
        "uid": PVC_B_UID,
        "target_capacity": "250Gi",
    },
    "expand_frozen": {
        "request_id": "exp-20261120-thanos-001",
        "namespace": EXPAND_NAMESPACE,
        "name": PVC_FROZEN,
        "uid": PVC_FROZEN_UID,
        "target_capacity": "100Gi",
    },
    # Same request id as expand_a, but a different target: must be rejected.
    "expand_a_changed": {
        "request_id": "exp-20261120-prom-001",
        "namespace": EXPAND_NAMESPACE,
        "name": PVC_A,
        "uid": PVC_A_UID,
        "target_capacity": "300Gi",
    },
}


def seed(store: InMemoryKube) -> None:
    store.reset()

    store.create_pv(_pv(PROM_PV, claim_name=OLD_CLAIM, claim_uid=PROM_OLD_UID))
    store.create_pvc(
        NAMESPACE,
        _pvc(NEW_CLAIM, PROM_NEW_UID, volume_name=PROM_PV, capacity="150Gi"),
    )
    store.create_pvc(
        NAMESPACE,
        _pvc(RIVAL_CLAIM, RIVAL_UID, volume_name=PROM_PV, capacity="150Gi"),
    )

    store.create_pv(
        _pv(
            DELETE_PV,
            claim_name="loki-data",
            claim_uid=DELETE_OLD_UID,
            policy="Delete",
        )
    )

    store.create_pv(
        _pv(
            VOLUME_MODE_PV,
            claim_name="thanos-data",
            claim_uid=WRONG_MODE_OLD_UID,
            volume_mode="Block",
        )
    )
    store.create_pvc(
        NAMESPACE,
        _pvc(
            WRONG_MODE_CLAIM,
            RIVAL_UID,
            volume_name=VOLUME_MODE_PV,
            volume_mode="Filesystem",
        ),
    )

    # Expansion demo: one expandable class with two live claims plus a class
    # that has expansion disabled.
    store.create_storage_class(_storage_class(EXPAND_CLASS, True))
    store.create_storage_class(_storage_class(EXPAND_CLASS_FROZEN, False))
    store.create_pvc(
        EXPAND_NAMESPACE, _bound_pvc(PVC_A, PVC_A_UID, capacity="100Gi")
    )
    store.create_pvc(
        EXPAND_NAMESPACE, _bound_pvc(PVC_B, PVC_B_UID, capacity="100Gi")
    )
    store.create_pvc(
        EXPAND_NAMESPACE,
        _bound_pvc(
            PVC_FROZEN,
            PVC_FROZEN_UID,
            capacity="50Gi",
            storage_class=EXPAND_CLASS_FROZEN,
        ),
    )
