from __future__ import annotations

import socket

import pytest


def test_default_suite_blocks_ip_network_connections() -> None:
    with (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client,
        pytest.raises(RuntimeError, match="network disabled by the offline test harness"),
    ):
        client.connect(("127.0.0.1", 9))

    with pytest.raises(RuntimeError, match="network disabled by the offline test harness"):
        socket.create_connection(("127.0.0.1", 9), timeout=0.01)


def test_default_suite_keeps_socketpair_ipc_available() -> None:
    left, right = socket.socketpair()
    with left, right:
        left.sendall(b"offline")
        assert right.recv(7) == b"offline"
