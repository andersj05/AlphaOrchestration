import pytest

from alpha_orchestration.actions import (
    ActionErrorCode,
    ActionParseError,
    FinalAction,
    ToolCallsAction,
    parse_action,
)


def test_parses_exact_tool_call_and_final_envelopes() -> None:
    tool_action = parse_action(
        '{"kind":"tool_calls","calls":[{"name":"finance.metrics","arguments":{"values":{"revenue":100}}}]}'
    )
    final_action = parse_action('{"kind":"final","payload":{"summary":"bounded"}}')

    assert isinstance(tool_action, ToolCallsAction)
    assert tool_action.calls[0].name == "finance.metrics"
    assert tool_action.calls[0].arguments == {"values": {"revenue": 100}}
    assert isinstance(final_action, FinalAction)
    assert final_action.payload == {"summary": "bounded"}


@pytest.mark.parametrize(
    "raw",
    [
        '{"kind":"final","kind":"tool_calls","payload":{}}',
        '{"kind":"final","payload":{"summary":"a","summary":"b"}}',
        '{"kind":"tool_calls","calls":[{"name":"x","arguments":{"n":1,"n":2}}]}',
    ],
)
def test_duplicate_keys_are_rejected_at_every_depth(raw: str) -> None:
    with pytest.raises(ActionParseError) as raised:
        parse_action(raw)

    assert raised.value.code is ActionErrorCode.DUPLICATE_KEY
    assert raised.value.repairable is True


@pytest.mark.parametrize(
    "raw",
    [
        '{"kind":"final","payload":{},"commentary":"ignore me"}',
        '{"kind":"tool_calls","calls":[{"name":"x","arguments":{},"call_id":"model-owned"}]}',
        '{"kind":"final"}',
        '[]',
    ],
)
def test_missing_extra_and_wrong_shape_fields_are_rejected(raw: str) -> None:
    with pytest.raises(ActionParseError) as raised:
        parse_action(raw)

    assert raised.value.code is ActionErrorCode.INVALID_ENVELOPE


@pytest.mark.parametrize(
    "raw",
    [
        '{"kind":"final","payload":{}} trailing prose',
        '```json\n{"kind":"final","payload":{}}\n```',
        '{"kind":"final","payload":{"score":NaN}}',
    ],
)
def test_non_json_wrappers_trailing_text_and_non_finite_numbers_are_rejected(raw: str) -> None:
    with pytest.raises(ActionParseError) as raised:
        parse_action(raw)

    assert raised.value.code is ActionErrorCode.INVALID_JSON
    assert raised.value.repairable is True


def test_utf8_byte_and_call_count_bounds_are_nonrepairable_policy_failures() -> None:
    oversized = '{"kind":"final","payload":{"text":"é"}}'
    with pytest.raises(ActionParseError) as size_error:
        parse_action(oversized, max_bytes=len(oversized))
    with pytest.raises(ActionParseError) as call_error:
        parse_action(
            '{"kind":"tool_calls","calls":['
            '{"name":"a","arguments":{}},{"name":"b","arguments":{}}]}',
            max_calls=1,
        )

    assert size_error.value.code is ActionErrorCode.ACTION_TOO_LARGE
    assert size_error.value.repairable is False
    assert call_error.value.code is ActionErrorCode.TOOL_CALL_LIMIT
    assert call_error.value.repairable is False


def test_unknown_action_kind_is_a_compact_repairable_failure() -> None:
    with pytest.raises(ActionParseError) as raised:
        parse_action('{"kind":"delegate","payload":{}}')

    assert raised.value.code is ActionErrorCode.UNKNOWN_KIND
    assert raised.value.repairable is True
