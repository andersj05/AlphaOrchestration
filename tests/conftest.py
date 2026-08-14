from __future__ import annotations

import os
import socket
from collections.abc import Generator
from typing import Any

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
    original_connect_ex = socket.socket.connect_ex
    original_bind = socket.socket.bind
    original_sendto = socket.socket.sendto
    original_sendmsg = getattr(socket.socket, "sendmsg", None)

    def network_error(operation: str) -> RuntimeError:
        return RuntimeError(
            f"network disabled by the offline test harness ({operation}); set "
            f"{LIVE_NETWORK_ENV}=1 only for an explicit live test"
        )

    def guarded_connect(client: socket.socket, address: object) -> None:
        if client.family in {socket.AF_INET, socket.AF_INET6}:
            raise network_error("socket.connect")
        original_connect(client, address)  # type: ignore[arg-type]

    def guarded_connect_ex(client: socket.socket, address: object) -> int:
        if client.family in {socket.AF_INET, socket.AF_INET6}:
            raise network_error("socket.connect_ex")
        return original_connect_ex(client, address)  # type: ignore[arg-type]

    def guarded_bind(client: socket.socket, address: object) -> None:
        if client.family in {socket.AF_INET, socket.AF_INET6}:
            raise network_error("socket.bind")
        original_bind(client, address)  # type: ignore[arg-type]

    def guarded_sendto(client: socket.socket, *args: Any) -> int:
        if client.family in {socket.AF_INET, socket.AF_INET6}:
            raise network_error("socket.sendto")
        return original_sendto(client, *args)

    def guarded_sendmsg(client: socket.socket, *args: Any) -> int:
        if client.family in {socket.AF_INET, socket.AF_INET6}:
            raise network_error("socket.sendmsg")
        if original_sendmsg is None:  # pragma: no cover - unavailable platforms
            raise AttributeError("socket.sendmsg is unavailable")
        return original_sendmsg(client, *args)

    def blocked_create_connection(*args: object, **kwargs: object) -> socket.socket:
        del args, kwargs
        raise network_error("socket.create_connection")

    def blocked_network_operation(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise network_error("Internet socket or DNS operation")

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", guarded_connect_ex)
    monkeypatch.setattr(socket.socket, "bind", guarded_bind)
    monkeypatch.setattr(socket.socket, "sendto", guarded_sendto)
    if original_sendmsg is not None:
        monkeypatch.setattr(socket.socket, "sendmsg", guarded_sendmsg)
    monkeypatch.setattr(socket, "create_connection", blocked_create_connection)
    monkeypatch.setattr(socket, "create_server", blocked_network_operation)
    monkeypatch.setattr(socket, "getaddrinfo", blocked_network_operation)
    monkeypatch.setattr(socket, "gethostbyname", blocked_network_operation)
    monkeypatch.setattr(socket, "gethostbyname_ex", blocked_network_operation)
    monkeypatch.setattr(socket, "gethostbyaddr", blocked_network_operation)
    yield
