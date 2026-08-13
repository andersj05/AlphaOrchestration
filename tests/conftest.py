from __future__ import annotations

import os
import socket
from collections.abc import Generator

import pytest

LIVE_NETWORK_ENV = "ALPHA_ALLOW_LIVE_NETWORK"
_TRUTHY = frozenset({"1", "true", "yes"})


@pytest.fixture(autouse=True)
def block_external_network(monkeypatch: pytest.MonkeyPatch) -> Generator[None]:
    """Keep the default suite hermetic while preserving local socketpair/Unix IPC."""

    if os.getenv(LIVE_NETWORK_ENV, "").strip().lower() in _TRUTHY:
        yield
        return

    original_connect = socket.socket.connect

    def guarded_connect(client: socket.socket, address: object) -> None:
        if client.family in {socket.AF_INET, socket.AF_INET6}:
            raise RuntimeError(
                f"network disabled by the offline test harness; set {LIVE_NETWORK_ENV}=1 "
                "only for an explicit live test"
            )
        original_connect(client, address)  # type: ignore[arg-type]

    def blocked_create_connection(*args: object, **kwargs: object) -> socket.socket:
        del args, kwargs
        raise RuntimeError(
            f"network disabled by the offline test harness; set {LIVE_NETWORK_ENV}=1 "
            "only for an explicit live test"
        )

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket, "create_connection", blocked_create_connection)
    yield
