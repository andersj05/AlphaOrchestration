"""Self-test the Python-process network denial used by the offline gate."""

from __future__ import annotations

import os
import socket


def _must_be_blocked(name: str, operation: object) -> None:
    try:
        operation()  # type: ignore[operator]
    except RuntimeError as exc:
        if "network disabled by the offline verification harness" not in str(exc):
            raise RuntimeError(f"{name} raised an unexpected error: {exc}") from exc
    else:
        raise RuntimeError(f"offline network guard did not block {name}")


def main() -> int:
    if os.environ.get("ALPHA_VERIFY_OFFLINE") != "1":
        raise RuntimeError("offline verification marker is missing")
    if os.environ.get("ALPHA_OFFLINE_GUARD_ACTIVE") != "1":
        raise RuntimeError("sitecustomize offline network guard did not load")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as stream:
        _must_be_blocked("connect_ex", lambda: stream.connect_ex(("127.0.0.1", 9)))
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as datagram:
        _must_be_blocked("sendto", lambda: datagram.sendto(b"offline", ("127.0.0.1", 9)))
    _must_be_blocked("DNS", lambda: socket.getaddrinfo("example.test", 443))

    left, right = socket.socketpair()
    with left, right:
        left.sendall(b"ipc")
        if right.recv(3) != b"ipc":
            raise RuntimeError("socketpair IPC did not round-trip")

    print("Offline network isolation passed; local socketpair IPC remains available.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
