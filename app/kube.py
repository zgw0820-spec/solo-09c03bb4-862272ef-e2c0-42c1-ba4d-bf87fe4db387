"""Kubernetes API access used by the handover service.

Two backends are provided:

``KubernetesClient``
    Talks to a real API server using the official Python client. ``patch``
    calls use JSON merge patches, so a 409 from the server means the resource
    version or the UID precondition failed.

``InMemoryKube`` (see ``mock_kube.store``)
    Same duck-typed surface, used by the mock API server and the test suite.

Both raise the small exception hierarchy defined here, so the service logic
never needs to care which backend answered.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Protocol

import urllib3
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.client.exceptions import ApiException


class KubeError(Exception):
    """Base class for Kubernetes access errors."""


class NotFound(KubeError):
    pass


class Conflict(KubeError):
    """The object changed underneath us, or a precondition failed."""


class Invalid(KubeError):
    """The API server rejected the request as malformed/semantically wrong."""


class Unavailable(KubeError):
    """The API server could not be reached."""


class Ambiguous(KubeError):
    """A write failed without a trustworthy answer.

    The API server (or a proxy on the way to it) may have applied the write
    even though the caller did not receive the receipt: timeouts, dropped
    connections and 5xx responses all land here. Callers must re-read the
    object before deciding whether the write happened.
    """


class KubeClient(Protocol):
    def get_pv(self, name: str) -> dict[str, Any]: ...
    def get_pvc(self, namespace: str, name: str) -> dict[str, Any]: ...
    def update_pv(self, name: str, body: dict[str, Any]) -> dict[str, Any]: ...
    def update_pvc(
        self, namespace: str, name: str, body: dict[str, Any]
    ) -> dict[str, Any]: ...
    def get_storage_class(self, name: str) -> dict[str, Any]: ...
    def get_config_map(self, namespace: str, name: str) -> dict[str, Any]: ...
    def create_config_map(
        self, namespace: str, body: dict[str, Any]
    ) -> dict[str, Any]: ...
    def update_config_map(
        self, namespace: str, name: str, body: dict[str, Any]
    ) -> dict[str, Any]: ...


def _translate(exc: ApiException, what: str) -> KubeError:
    status = exc.status or 0
    detail = ""
    try:
        payload = json.loads(exc.body) if exc.body else {}
        detail = payload.get("message") or payload.get("reason") or ""
    except (ValueError, AttributeError):
        detail = str(exc.body or "")
    message = f"{what}: {detail or exc.reason or status}"
    if status == 404:
        return NotFound(message)
    if status == 409:
        return Conflict(message)
    if status in (400, 422):
        if "immutable" in message:
            # Our merge patch carries metadata.uid as a second precondition;
            # the API server reports a mismatch as an immutable-field error.
            return Conflict(message)
        return Invalid(message)
    if status in (401, 403):
        return Invalid(message)
    if status >= 500 or status == 0:
        return Unavailable(message)
    return KubeError(message)


def _translate_write(exc: Exception, what: str) -> KubeError:
    """Classify a failed write.

    4xx answers are trustworthy: the API server rejected the write, so it did
    not happen. Everything else (timeouts, connection resets, 5xx from the
    server or a proxy) is ambiguous and must be verified by re-reading.
    """
    if isinstance(exc, ApiException):
        status = exc.status or 0
        if status == 0 or status >= 500:
            return Ambiguous(f"{what}: no trustworthy response (status {status or 'n/a'})")
        translated = _translate(exc, what)
        if 400 <= status < 500 and not isinstance(translated, (Conflict, NotFound)):
            # Any other definitive rejection (400/401/403/415/422...) proves
            # the write was not applied.
            return Invalid(str(translated))
        return translated
    if isinstance(exc, urllib3.exceptions.HTTPError):
        return Ambiguous(f"{what}: {type(exc).__name__}: {exc}")
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return Ambiguous(f"{what}: {type(exc).__name__}: {exc}")
    raise exc


@dataclass
class KubernetesClient:
    """Thin wrapper around the official client, scoped to what we need."""

    core: k8s_client.CoreV1Api
    request_timeout: float = 10.0
    _api_client: k8s_client.ApiClient | None = None
    _storage: k8s_client.StorageV1Api | None = None

    def __post_init__(self) -> None:
        api_client = getattr(self.core, "api_client", None)
        if api_client is None:
            api_client = k8s_client.ApiClient()
        self._api_client = api_client

    @classmethod
    def from_config(
        cls,
        kubeconfig: str | None = None,
        context: str | None = None,
        in_cluster: bool | None = None,
        request_timeout: float = 10.0,
    ) -> "KubernetesClient":
        try:
            if in_cluster is None:
                in_cluster = not kubeconfig and _running_in_cluster()
            if in_cluster:
                k8s_config.load_incluster_config()
            else:
                k8s_config.load_kube_config(
                    config_file=kubeconfig, context=context
                )
        except Exception as exc:  # noqa: BLE001 - surfaced as Unavailable
            raise Unavailable(f"could not load Kubernetes configuration: {exc}") from exc
        return cls(core=k8s_client.CoreV1Api(), request_timeout=request_timeout)

    def get_pv(self, name: str) -> dict[str, Any]:
        try:
            obj = self.core.read_persistent_volume(
                name, _request_timeout=self.request_timeout
            )
        except ApiException as exc:
            raise _translate(exc, f"reading PersistentVolume {name!r}") from exc
        return self._to_dict(obj)

    def get_pvc(self, namespace: str, name: str) -> dict[str, Any]:
        try:
            obj = self.core.read_namespaced_persistent_volume_claim(
                name, namespace, _request_timeout=self.request_timeout
            )
        except ApiException as exc:
            raise _translate(
                exc, f"reading PersistentVolumeClaim {namespace}/{name}"
            ) from exc
        return self._to_dict(obj)

    def list_pvs(self, limit: int | None = None) -> list[dict[str, Any]]:
        try:
            result = self.core.list_persistent_volume(
                limit=limit, _request_timeout=self.request_timeout
            )
        except ApiException as exc:
            raise _translate(exc, "listing PersistentVolumes") from exc
        items = getattr(result, "items", None) or []
        return [self._to_dict(item) for item in items]

    def update_pv(self, name: str, body: dict[str, Any]) -> dict[str, Any]:
        """JSON-merge-patch a PV.

        The body is expected to carry ``metadata.resourceVersion`` and
        ``metadata.uid`` so the API server enforces optimistic concurrency.
        """
        try:
            obj = self.core.patch_persistent_volume(
                name,
                body,
                _content_type="application/merge-patch+json",
                _request_timeout=self.request_timeout,
            )
        except ApiException as exc:
            raise _translate(exc, f"updating PersistentVolume {name!r}") from exc
        return self._to_dict(obj)

    def update_pvc(
        self, namespace: str, name: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        """JSON-merge-patch a PVC (used only for capacity expansion)."""
        try:
            obj = self.core.patch_namespaced_persistent_volume_claim(
                name,
                namespace,
                body,
                _content_type="application/merge-patch+json",
                _request_timeout=self.request_timeout,
            )
        except Exception as exc:  # noqa: BLE001 - classified below
            raise _translate_write(
                exc, f"updating PersistentVolumeClaim {namespace}/{name}"
            ) from exc
        return self._to_dict(obj)

    def get_storage_class(self, name: str) -> dict[str, Any]:
        try:
            obj = self._storage_api().read_storage_class(
                name, _request_timeout=self.request_timeout
            )
        except ApiException as exc:
            raise _translate(exc, f"reading StorageClass {name!r}") from exc
        return self._to_dict(obj)

    def _storage_api(self) -> k8s_client.StorageV1Api:
        if self._storage is None:
            assert self._api_client is not None
            self._storage = k8s_client.StorageV1Api(self._api_client)
        return self._storage

    def get_config_map(self, namespace: str, name: str) -> dict[str, Any]:
        try:
            obj = self.core.read_namespaced_config_map(
                name, namespace, _request_timeout=self.request_timeout
            )
        except ApiException as exc:
            raise _translate(exc, f"reading ConfigMap {namespace}/{name}") from exc
        return self._to_dict(obj)

    def create_config_map(
        self, namespace: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            obj = self.core.create_namespaced_config_map(
                namespace, body, _request_timeout=self.request_timeout
            )
        except Exception as exc:  # noqa: BLE001 - classified below
            raise _translate_write(
                exc, f"creating ConfigMap {namespace}/{body.get('metadata', {}).get('name')}"
            ) from exc
        return self._to_dict(obj)

    def update_config_map(
        self, namespace: str, name: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            obj = self.core.patch_namespaced_config_map(
                name,
                namespace,
                body,
                _content_type="application/merge-patch+json",
                _request_timeout=self.request_timeout,
            )
        except Exception as exc:  # noqa: BLE001 - classified below
            raise _translate_write(
                exc, f"updating ConfigMap {namespace}/{name}"
            ) from exc
        return self._to_dict(obj)

    def _to_dict(self, obj: Any) -> dict[str, Any]:
        """Serialise a client model back to the API's camelCase JSON shape."""
        assert self._api_client is not None
        return _camelise(self._api_client.sanitize_for_serialization(obj))


def _running_in_cluster() -> bool:
    import os

    return bool(os.environ.get("KUBERNETES_SERVICE_HOST"))


_CAMEL = re.compile(r"_([a-z0-9])")


def _camelise(value: Any) -> Any:
    """Convert snake_case keys from client models back to API JSON keys."""
    if isinstance(value, dict):
        return {_CAMEL.sub(lambda m: m.group(1).upper(), key): _camelise(item)
                for key, item in value.items()}
    if isinstance(value, list):
        return [_camelise(item) for item in value]
    return value
