from __future__ import annotations

import socket

import pytest


def test_default_suite_blocks_ip_network_operations_and_dns() -> None:
    with (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client,
        pytest.raises(RuntimeError, match="network disabled by the offline test harness"),
    ):
        client.connect(("127.0.0.1", 9))

    with (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client,
        pytest.raises(RuntimeError, match="socket.connect_ex"),
    ):
        client.connect_ex(("127.0.0.1", 9))

    with (
        socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client,
        pytest.raises(RuntimeError, match="socket.sendto"),
    ):
        client.sendto(b"offline", ("127.0.0.1", 9))

    with (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client,
        pytest.raises(RuntimeError, match="socket.bind"),
    ):
        client.bind(("127.0.0.1", 0))

    with pytest.raises(RuntimeError, match="network disabled by the offline test harness"):
        socket.create_connection(("127.0.0.1", 9), timeout=0.01)

    with pytest.raises(RuntimeError, match="Internet socket or DNS"):
        socket.getaddrinfo("example.test", 443)


def test_default_suite_keeps_socketpair_ipc_available() -> None:
    left, right = socket.socketpair()
    with left, right:
        left.sendall(b"offline")
        assert right.recv(7) == b"offline"
