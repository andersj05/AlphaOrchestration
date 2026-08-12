import asyncio

import pytest

from alpha_orchestration.ports import ToolCall
from alpha_orchestration.tools.finance import build_financial_tool_registry, financial_tools_for_agent
from alpha_orchestration.tools.registry import ToolDefinition, ToolRegistry


def test_arithmetic_tool_batches_exact_operations_and_preserves_lineage() -> None:
    registry = build_financial_tool_registry()
    call = ToolCall(
        name="finance.calculate",
        call_id="calc-1",
        arguments={
            "operations": [
                {"id": "growth", "operation": "percent_change", "current": 125, "prior": 100},
                {"id": "margin_move", "operation": "basis_point_change", "current": 0.24, "prior": 0.20},
                {"id": "mix", "operation": "weighted_average", "values": [10, 20], "weights": [1, 3]},
            ],
            "source_ids": ["sec:revenue:2025", "sec:revenue:2024"],
            "context": {"current_period": "FY2025", "prior_period": "FY2024"},
            "precision": 4,
        },
    )

    result = asyncio.run(registry.execute(call))

    assert result.payload["ok"] is True
    assert result.source_ids == ("sec:revenue:2025", "sec:revenue:2024")
    rows = result.payload["data"]["results"]
    assert [row["value"] for row in rows] == [0.25, 400.0, 17.5]
    assert result.payload["data"]["context"]["current_period"] == "FY2025"


def test_tool_validation_returns_a_compact_repairable_error() -> None:
    registry = build_financial_tool_registry()
    result = asyncio.run(
        registry.execute(
            ToolCall(
                name="finance.calculate",
                call_id="bad-number",
                arguments={
                    "operations": [{"id": "growth", "operation": "percent_change", "current": True, "prior": 100}]
                },
            )
        )
    )

    assert result.payload["ok"] is False
    assert result.payload["error"]["code"] == "invalid_schema"
    assert result.payload["error"]["details"]["phase"] == "schema"
    assert "expected number" in result.payload["error"]["message"]
    assert result.retryable is False


def test_semantic_math_error_does_not_emit_nan_or_infinity() -> None:
    registry = build_financial_tool_registry()
    result = asyncio.run(
        registry.execute(
            ToolCall(
                name="finance.calculate",
                call_id="zero-denominator",
                arguments={"operations": [{"id": "bad_ratio", "operation": "ratio", "numerator": 5, "denominator": 0}]},
            )
        )
    )

    assert result.payload["ok"] is False
    assert result.payload["error"]["code"] == "invalid_arguments"
    assert result.payload["error"]["details"]["phase"] == "semantic"
    assert "denominator must not be zero" in result.payload["error"]["message"]


def test_call_ids_are_idempotent_but_cannot_be_rebound() -> None:
    registry = build_financial_tool_registry()
    original = ToolCall(
        name="finance.calculate",
        call_id="stable-call",
        arguments={"operations": [{"id": "total", "operation": "sum", "values": [1, 2, 3]}]},
    )

    first = asyncio.run(registry.execute(original))
    replay = asyncio.run(registry.execute(original))
    conflict = asyncio.run(
        registry.execute(
            ToolCall(
                name="finance.calculate",
                call_id="stable-call",
                arguments={"operations": [{"id": "total", "operation": "sum", "values": [4, 5]}]},
            )
        )
    )

    assert replay is first
    assert conflict.payload["ok"] is False
    assert conflict.payload["error"]["code"] == "call_id_conflict"


def test_scoped_executor_enforces_controller_owned_allowlist() -> None:
    registry = build_financial_tool_registry()
    executor = registry.scoped(financial_tools_for_agent("universe"))
    call = ToolCall(
        name="finance.discounted_cash_flow",
        call_id="policy-block",
        arguments={
            "cash_flows": [10],
            "discount_rate": 0.1,
            "terminal": {"method": "perpetuity_growth", "growth_rate": 0.02},
        },
    )

    result = asyncio.run(executor.execute(call))

    assert executor.names == ("finance.rank",)
    assert result.payload["ok"] is False
    assert result.payload["error"]["code"] == "tool_not_allowed"
    assert result.payload["error"]["details"]["allowed_tools"] == ["finance.rank"]


def test_registry_bounds_large_results() -> None:
    definition = ToolDefinition(
        name="test.large",
        description="fixture",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        handler=lambda _: {"text": "x" * 2_000},
    )
    registry = ToolRegistry([definition], max_result_bytes=1_024)

    result = asyncio.run(registry.execute(ToolCall(name="test.large", call_id="large", arguments={})))

    assert result.payload["ok"] is False
    assert result.payload["error"]["code"] == "result_too_large"


def test_catalog_is_explicit_versioned_and_narrowable() -> None:
    registry = build_financial_tool_registry()

    assert registry.names == (
        "finance.calculate",
        "finance.discounted_cash_flow",
        "finance.forecast_growth",
        "finance.market_statistics",
        "finance.metrics",
        "finance.rank",
    )
    contract = registry.contracts(["finance.metrics"])[0]
    assert contract["name"] == "finance.metrics"
    assert contract["version"] == "1.0.0"
    assert contract["annotations"] == {"read_only": True, "idempotent": True}


def test_every_financial_tool_contract_executes_with_canonical_arguments() -> None:
    registry = build_financial_tool_registry()
    calls = (
        ToolCall(
            name="finance.calculate",
            call_id="integration-calculate",
            arguments={"operations": [{"id": "sum", "operation": "sum", "values": [1, 2]}]},
        ),
        ToolCall(
            name="finance.metrics",
            call_id="integration-metrics",
            arguments={
                "values": {"revenue": 100, "cost_of_revenue": 60},
                "metrics": ["gross_margin"],
            },
        ),
        ToolCall(
            name="finance.forecast_growth",
            call_id="integration-forecast",
            arguments={
                "base_value": 100,
                "periods": 2,
                "period_labels": ["FY27", "FY28"],
                "scenarios": [{"name": "base", "growth_rates": [0.1]}],
            },
        ),
        ToolCall(
            name="finance.discounted_cash_flow",
            call_id="integration-dcf",
            arguments={
                "cash_flows": [10, 12],
                "period_labels": ["FY27", "FY28"],
                "discount_rate": 0.1,
                "terminal": {"method": "exit_multiple", "multiple": 5, "metric_value": 15},
            },
        ),
        ToolCall(
            name="finance.market_statistics",
            call_id="integration-market",
            arguments={
                "returns": [0.1, -0.05],
                "periods_per_year": 12,
                "annual_risk_free_rate": 0.02,
            },
        ),
        ToolCall(
            name="finance.rank",
            call_id="integration-rank",
            arguments={
                "rows": [
                    {"id": "A", "metrics": {"score": 2}},
                    {"id": "B", "metrics": {"score": 1}},
                ],
                "criteria": [{"metric": "score", "direction": "higher", "weight": 1}],
                "missing_policy": "exclude",
            },
        ),
    )

    async def execute_all() -> list:
        return [await registry.execute(call) for call in calls]

    results = asyncio.run(execute_all())

    assert {result.payload["tool"] for result in results} == set(registry.names)
    assert all(result.payload["ok"] is True for result in results), [result.payload for result in results]


def test_metrics_registry_accepts_accounts_payable_fallback_for_dpo() -> None:
    registry = build_financial_tool_registry()
    result = asyncio.run(
        registry.execute(
            ToolCall(
                name="finance.metrics",
                call_id="accounts-payable-dpo",
                arguments={
                    "values": {
                        "accounts_payable": 60,
                        "cost_of_revenue": 400,
                    },
                    "metrics": ["days_payables_outstanding"],
                    "precision": 2,
                },
            )
        )
    )

    assert result.payload["ok"] is True
    assert result.payload["data"]["values"]["days_payables_outstanding"] == 54.75
    assert result.payload["data"]["details"]["days_payables_outstanding"]["inputs"] == {
        "average_accounts_payable": 60.0,
        "cost_of_revenue": 400.0,
        "days_in_period": 365.0,
    }


def test_scoped_executor_rejects_untrusted_source_ids_before_execution() -> None:
    registry = build_financial_tool_registry()
    executor = registry.scoped(
        ["finance.calculate"],
        allowed_source_ids=["sec:trusted"],
    )
    result = asyncio.run(
        executor.execute(
            ToolCall(
                name="finance.calculate",
                call_id="source-policy-block",
                arguments={
                    "operations": [{"id": "sum", "operation": "sum", "values": [1, 2]}],
                    "source_ids": ["sec:trusted", "web:untrusted"],
                },
            )
        )
    )

    assert result.payload["ok"] is False
    assert result.payload["error"]["code"] == "source_not_allowed"
    assert result.payload["error"]["details"]["unknown_source_ids"] == ["web:untrusted"]


def test_tool_schema_snapshot_cannot_be_mutated_through_public_contracts() -> None:
    schema = {
        "type": "object",
        "required": ["value"],
        "properties": {"value": {"type": "number"}},
        "additionalProperties": False,
    }
    definition = ToolDefinition(
        name="test.required",
        description="fixture",
        input_schema=schema,
        handler=lambda arguments: {"value": arguments["value"]},
    )
    registry = ToolRegistry([definition])
    exposed = registry.contracts(["test.required"])[0]

    definition.input_schema["required"].clear()
    exposed["input_schema"]["required"].clear()

    fresh = registry.contracts(["test.required"])[0]
    result = asyncio.run(registry.execute(ToolCall(name="test.required", call_id="missing-value", arguments={})))

    assert fresh["input_schema"]["required"] == ["value"]
    assert result.payload["ok"] is False
    assert result.payload["error"]["code"] == "invalid_schema"


def test_tool_definition_rejects_unsupported_schema_keywords() -> None:
    with pytest.raises(ValueError, match="unsupported schema keywords"):
        ToolDefinition(
            name="test.unsupported-schema",
            description="fixture",
            input_schema={
                "type": "object",
                "properties": {},
                "oneOf": [],
            },
            handler=lambda _: {},
        )
