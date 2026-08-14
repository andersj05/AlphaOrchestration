"""Deny Internet networking in Python processes launched by the offline gate."""

from __future__ import annotations

import os
import socket
from typing import Any

_INTERNET_FAMILIES = frozenset({socket.AF_INET, socket.AF_INET6})
_ACTIVE_ENV = "ALPHA_OFFLINE_GUARD_ACTIVE"


class OfflineNetworkError(RuntimeError):
    """Raised before an offline-gate process can use an Internet socket or DNS."""


def _blocked(operation: str) -> OfflineNetworkError:
    return OfflineNetworkError(
        f"network disabled by the offline verification harness: {operation}"
    )


if os.environ.get("ALPHA_VERIFY_OFFLINE") == "1":
    _original_connect = socket.socket.connect
    _original_connect_ex = socket.socket.connect_ex
    _original_bind = socket.socket.bind
    _original_sendto = socket.socket.sendto
    _original_sendmsg = getattr(socket.socket, "sendmsg", None)

    def _guarded_connect(client: socket.socket, address: object) -> None:
        if client.family in _INTERNET_FAMILIES:
            raise _blocked("socket.connect")
        _original_connect(client, address)  # type: ignore[arg-type]

    def _guarded_connect_ex(client: socket.socket, address: object) -> int:
        if client.family in _INTERNET_FAMILIES:
            raise _blocked("socket.connect_ex")
        return _original_connect_ex(client, address)  # type: ignore[arg-type]

    def _guarded_bind(client: socket.socket, address: object) -> None:
        if client.family in _INTERNET_FAMILIES:
            raise _blocked("socket.bind")
        _original_bind(client, address)  # type: ignore[arg-type]

    def _guarded_sendto(client: socket.socket, *args: Any) -> int:
        if client.family in _INTERNET_FAMILIES:
            raise _blocked("socket.sendto")
        return _original_sendto(client, *args)

    def _guarded_sendmsg(client: socket.socket, *args: Any) -> int:
        if client.family in _INTERNET_FAMILIES:
            raise _blocked("socket.sendmsg")
        if _original_sendmsg is None:  # pragma: no cover - unavailable platforms
            raise AttributeError("socket.sendmsg is unavailable")
        return _original_sendmsg(client, *args)

    def _blocked_create_connection(*args: object, **kwargs: object) -> socket.socket:
        del args, kwargs
        raise _blocked("socket.create_connection")

    def _blocked_create_server(*args: object, **kwargs: object) -> socket.socket:
        del args, kwargs
        raise _blocked("socket.create_server")

    def _blocked_dns(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise _blocked("DNS resolution")

    socket.socket.connect = _guarded_connect
    socket.socket.connect_ex = _guarded_connect_ex
    socket.socket.bind = _guarded_bind
    socket.socket.sendto = _guarded_sendto
    if _original_sendmsg is not None:
        socket.socket.sendmsg = _guarded_sendmsg
    socket.create_connection = _blocked_create_connection
    socket.create_server = _blocked_create_server
    socket.getaddrinfo = _blocked_dns
    socket.gethostbyname = _blocked_dns
    socket.gethostbyname_ex = _blocked_dns
    socket.gethostbyaddr = _blocked_dns
    os.environ[_ACTIVE_ENV] = "1"
