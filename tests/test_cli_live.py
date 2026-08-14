from __future__ import annotations

from typing import Any

import pytest

from alpha_orchestration import cli


def test_plain_live_routes_normalized_tickers_and_bounded_spec(monkeypatch, tmp_path) -> None:
    captured: dict[str, Any] = {}
    runtime = object()

    def fake_live_runtime(args, tickers):
        captured["runtime_tickers"] = tickers
        captured["cache"] = args.live_cache
        return runtime

    async def fake_plain_run(args, spec, selected_runtime):
        captured["spec"] = spec
        captured["selected_runtime"] = selected_runtime
        return 0

    monkeypatch.setattr(cli, "_live_runtime", fake_live_runtime)
    monkeypatch.setattr(cli, "_plain_run", fake_plain_run)

    result = cli.main(
        [
            "--live",
            "--plain",
            "--tickers",
            " aapl,MSFT,AAPL ",
            "--active-slots",
            "8",
            "--live-cache",
            str(tmp_path / "cache"),
        ]
    )

    spec = captured["spec"]
    assert result == 0
    assert captured["runtime_tickers"] == ("AAPL", "MSFT")
    assert captured["selected_runtime"] is runtime
    assert captured["cache"] == tmp_path / "cache"
    assert spec.mode == "live"
    assert spec.universe_size == 2
    assert spec.agent_budget == 2
    assert spec.active_slots == 2


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["--live", "--plain"], "--live requires --tickers"),
        (["--tickers", "AAPL", "--plain"], "--tickers requires --live"),
        (["--live", "--tickers", "AAPL", "--provider-slots", "9"], "provider-slots"),
        (
            ["--live", "--tickers", "A,B,C,D,E,F,G,H,I", "--plain"],
            "between 1 and 8",
        ),
    ],
)
def test_live_cli_rejects_invalid_composition(arguments, message, capsys) -> None:
    with pytest.raises(SystemExit) as raised:
        cli.main(arguments)

    assert raised.value.code == 2
    assert message in capsys.readouterr().err


def test_live_tui_receives_readiness_factory_and_exact_universe(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class FakeApp:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        def run(self) -> None:
            captured["ran"] = True

    monkeypatch.setattr(cli, "AlphaApp", FakeApp)
    monkeypatch.setattr(
        cli,
        "live_environment_readiness",
        lambda: {"sec_identity_configured": True, "yfinance_installed": True},
    )

    assert cli.main(["--live", "--tickers", "nvda,msft"]) == 0
    assert captured["ran"] is True
    assert captured["initial_tickers"] == ("NVDA", "MSFT")
    assert captured["initial_spec"].mode == "live"
    assert captured["live_readiness"].ready is True
    assert callable(captured["live_runtime_factory"])


def test_plain_live_initialization_failure_never_uses_demo(monkeypatch, capsys) -> None:
    def fail_live_runtime(args, tickers):
        del args, tickers
        raise RuntimeError("live provider preflight failed")

    def forbidden_demo(*args, **kwargs):
        del args, kwargs
        raise AssertionError("live mode must not fall back to the demo runtime")

    monkeypatch.setattr(cli, "_live_runtime", fail_live_runtime)
    monkeypatch.setattr(cli, "DemoRuntime", forbidden_demo)

    with pytest.raises(SystemExit) as raised:
        cli.main(["--live", "--tickers", "AAPL", "--plain"])

    assert raised.value.code == 2
    assert "live provider preflight failed" in capsys.readouterr().err
