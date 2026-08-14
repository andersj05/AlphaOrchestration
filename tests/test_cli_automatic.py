from __future__ import annotations

from typing import Any

import pytest

from alpha_orchestration import cli
from alpha_orchestration.tui.app import AUTOMATIC_LIVE_MODE


def _capture_app(monkeypatch) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    class FakeApp:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        def run(self) -> None:
            captured["ran"] = True

    monkeypatch.setattr(cli, "AlphaApp", FakeApp)
    return captured


def test_no_arguments_launches_ready_automatic_live_screen(monkeypatch) -> None:
    captured = _capture_app(monkeypatch)
    runtime = object()
    monkeypatch.setattr(
        cli,
        "live_environment_readiness",
        lambda: {"sec_identity_configured": True, "yfinance_installed": True},
    )
    monkeypatch.setattr(cli, "_automatic_runtime", lambda args: runtime)

    assert cli.main([]) == 0

    spec = captured["initial_spec"]
    assert captured["ran"] is True
    assert spec.mode == AUTOMATIC_LIVE_MODE
    assert spec.sector == "U.S. large, liquid equities"
    assert spec.universe_size == 300
    assert spec.agent_budget == 8
    assert spec.active_slots == 8
    assert captured["startup_mode"] == AUTOMATIC_LIVE_MODE
    assert captured["automatic_readiness"].ready is True
    assert captured["automatic_readiness"].analysis_label == "RULE-BASED SCREEN (NO MODEL DILIGENCE)"
    assert captured["automatic_runtime_factory"](spec) is runtime


def test_no_arguments_fail_closed_to_preflight_when_readiness_is_missing(monkeypatch) -> None:
    captured = _capture_app(monkeypatch)
    monkeypatch.setattr(
        cli,
        "live_environment_readiness",
        lambda: {"sec_identity_configured": False, "yfinance_installed": True},
    )

    def forbidden_runtime(args):
        del args
        raise AssertionError("blocked startup must not construct a runtime")

    def forbidden_demo(*args, **kwargs):
        del args, kwargs
        raise AssertionError("automatic startup must not fall back to fixtures")

    monkeypatch.setattr(cli, "_automatic_runtime", forbidden_runtime)
    monkeypatch.setattr(cli, "DemoRuntime", forbidden_demo)

    assert cli.main([]) == 0

    readiness = captured["automatic_readiness"]
    assert captured["ran"] is True
    assert captured["initial_spec"] is None
    assert captured["startup_mode"] == AUTOMATIC_LIVE_MODE
    assert readiness.ready is False
    assert readiness.sec_identity_configured is False
    assert readiness.runtime_available is True
    assert readiness.blocker == "Automatic live prerequisites are incomplete; no research has started"


def test_plain_automatic_routes_expert_policy_and_limits(monkeypatch, tmp_path) -> None:
    captured: dict[str, Any] = {}
    runtime = object()

    def fake_builder(**kwargs):
        captured["builder"] = kwargs
        return runtime

    async def fake_plain_run(args, spec, selected_runtime):
        captured["spec"] = spec
        captured["selected_runtime"] = selected_runtime
        return 0

    monkeypatch.setattr(cli, "build_automatic_live_runtime", fake_builder)
    monkeypatch.setattr(cli, "_plain_run", fake_plain_run)

    result = cli.main(
        [
            "--automatic",
            "--plain",
            "--target-size",
            "120",
            "--minimum-universe-size",
            "100",
            "--max-inspected",
            "150",
            "--active-slots",
            "2",
            "--provider-slots",
            "3",
            "--collection-batch-size",
            "10",
            "--candidate-limit",
            "7",
            "--diligence-limit",
            "4",
            "--diligence-slots",
            "2",
            "--live-cache",
            str(tmp_path / "cache"),
        ]
    )

    spec = captured["spec"]
    builder = captured["builder"]
    policy = builder["policy"]
    assert result == 0
    assert captured["selected_runtime"] is runtime
    assert spec.mode == AUTOMATIC_LIVE_MODE
    assert spec.universe_size == 120
    assert spec.agent_budget == 8
    assert spec.active_slots == 2
    assert policy.profile_id == "US_LARGE_LIQUID_V1"
    assert policy.target_size == 120
    assert policy.minimum_size == 100
    assert policy.max_screened == 150
    assert builder["cache_root"] == tmp_path / "cache"
    assert builder["provider_slots"] == 3
    assert builder["collection_batch_size"] == 10
    assert builder["candidate_limit"] == 7
    assert builder["diligence_limit"] == 4
    assert builder["diligence_slots"] == 2


def test_default_diligence_cap_never_exceeds_candidate_cap(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    runtime = object()

    def fake_builder(**kwargs):
        captured.update(kwargs)
        return runtime

    monkeypatch.setattr(cli, "build_automatic_live_runtime", fake_builder)
    args = cli.build_parser().parse_args(["--automatic", "--candidate-limit", "3"])

    assert cli._automatic_runtime(args) is runtime
    assert captured["diligence_limit"] == 3


def test_explicit_demo_preserves_fixture_mode(monkeypatch) -> None:
    captured = _capture_app(monkeypatch)
    monkeypatch.setattr(
        cli,
        "live_environment_readiness",
        lambda: {"sec_identity_configured": False, "yfinance_installed": False},
    )

    assert cli.main(["--demo"]) == 0

    assert captured["initial_spec"].mode == "synthetic_demo"
    assert captured["initial_spec"].sector == "Semiconductors"
    assert captured["startup_mode"] == "mission"


def test_automatic_policy_errors_are_safe_cli_errors(capsys) -> None:
    with pytest.raises(SystemExit) as raised:
        cli.main(
            [
                "--automatic",
                "--target-size",
                "150",
                "--minimum-universe-size",
                "200",
            ]
        )

    assert raised.value.code == 2
    assert "minimum <= target" in capsys.readouterr().err


def test_help_leads_with_no_argument_automatic_path() -> None:
    help_text = cli.build_parser().format_help()
    normalized_help = " ".join(help_text.split())

    assert "With no arguments" in help_text
    assert "python -m alpha_orchestration" in help_text
    assert "--live --tickers AAPL,MSFT,NVDA" in help_text
    assert "--demo" in help_text
    assert "uninspected matches are not exclusions" in normalized_help
