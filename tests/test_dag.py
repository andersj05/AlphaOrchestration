import pytest

from alpha_orchestration.dag import (
    DagErrorCode,
    DagValidationError,
    TaskDefinition,
    WorkflowDefinition,
)


def task(task_id: str, *depends_on: str, tools: tuple[str, ...] = ()) -> TaskDefinition:
    return TaskDefinition(
        task_id=task_id,
        agent_id=f"agent-{task_id}",
        depends_on=depends_on,
        allowed_tools=tools,
    )


def test_topological_order_is_stable_by_declaration_order() -> None:
    workflow = WorkflowDefinition(
        workflow_id="equity-research",
        version="1.0.0",
        tasks=(
            task("publish", "analysis", "risk"),
            task("risk", "facts"),
            task("analysis", "facts"),
            task("facts"),
        ),
        active_slots=2,
    )

    assert workflow.task_ids == ("publish", "risk", "analysis", "facts")
    assert workflow.topological_task_ids == ("facts", "risk", "analysis", "publish")
    assert tuple(item.task_id for item in workflow.topological_tasks) == workflow.topological_task_ids


def test_unknown_dependencies_fail_closed() -> None:
    with pytest.raises(DagValidationError) as raised:
        WorkflowDefinition("research", "1", (task("analysis", "missing"),))

    assert raised.value.code is DagErrorCode.UNKNOWN_DEPENDENCY
    assert "missing" in str(raised.value)


def test_cycles_include_a_deterministic_path() -> None:
    with pytest.raises(DagValidationError) as raised:
        WorkflowDefinition(
            "research",
            "1",
            (task("a", "b"), task("b", "c"), task("c", "a")),
        )

    assert raised.value.code is DagErrorCode.DEPENDENCY_CYCLE
    assert "a -> b -> c -> a" in str(raised.value)


def test_duplicate_task_dependency_and_tool_names_are_rejected() -> None:
    with pytest.raises(DagValidationError) as duplicate_task:
        WorkflowDefinition("research", "1", (task("same"), task("same")))
    with pytest.raises(DagValidationError, match="dependencies contain duplicates"):
        task("analysis", "facts", "facts")
    with pytest.raises(DagValidationError, match="allowed tools contain duplicates"):
        task("analysis", tools=("finance.metrics", "finance.metrics"))

    assert duplicate_task.value.code is DagErrorCode.DUPLICATE_TASK


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_turns", 0),
        ("max_tool_calls", 65),
        ("max_calls_per_turn", 9),
        ("max_new_tokens", 0),
        ("max_action_bytes", 127),
        ("repair_budget", 2),
    ],
)
def test_task_budgets_are_strictly_bounded(field: str, value: int) -> None:
    arguments = {"task_id": "bounded", "agent_id": "agent", field: value}

    with pytest.raises(DagValidationError) as raised:
        TaskDefinition(**arguments)

    assert raised.value.code is DagErrorCode.INVALID_FIELD


def test_task_contract_requires_object_output_and_consistent_tool_budget() -> None:
    with pytest.raises(DagValidationError, match="JSON object"):
        TaskDefinition("analysis", "agent", output_schema={"type": "array"})
    with pytest.raises(DagValidationError, match="positive max_tool_calls"):
        TaskDefinition("analysis", "agent", allowed_tools=("finance.metrics",), max_tool_calls=0)
    with pytest.raises(DagValidationError, match="cannot exceed"):
        TaskDefinition("analysis", "agent", max_tool_calls=2, max_calls_per_turn=3)


def test_workflow_validates_tool_catalog_and_read_only_policy() -> None:
    workflow = WorkflowDefinition(
        "research",
        "1",
        (task("analysis", tools=("finance.metrics",)),),
    )
    workflow.validate_tools(["finance.metrics"], read_only_tools=["finance.metrics"])

    with pytest.raises(DagValidationError) as unknown:
        workflow.validate_tools(["finance.calculate"])
    with pytest.raises(DagValidationError) as mutating:
        workflow.validate_tools(["finance.metrics"], read_only_tools=[])

    assert unknown.value.code is DagErrorCode.UNKNOWN_TOOL
    assert mutating.value.code is DagErrorCode.TOOL_NOT_READ_ONLY


def test_plan_hash_is_canonical_and_changes_with_policy() -> None:
    first = WorkflowDefinition("research", "1", (task("facts"),), active_slots=1)
    equivalent = WorkflowDefinition("research", "1", [task("facts")], active_slots=1)
    changed = WorkflowDefinition("research", "1", (task("facts"),), active_slots=2)

    assert len(first.plan_hash) == 64
    assert first.plan_hash == equivalent.plan_hash
    assert first.plan_hash != changed.plan_hash
    assert first.to_dict()["tasks"][0]["task_id"] == "facts"


def test_task_policy_snapshot_is_stable_after_nested_schema_mutation() -> None:
    schema = {
        "type": "object",
        "required": ["summary"],
        "properties": {"summary": {"type": "string"}},
        "additionalProperties": False,
    }
    definition = TaskDefinition("facts", "agent", output_schema=schema)
    workflow = WorkflowDefinition("research", "1", (definition,))
    original_hash = workflow.plan_hash

    definition.output_schema["required"].clear()
    definition.output_schema["properties"].clear()

    assert definition.output_schema_copy()["required"] == ["summary"]
    assert definition.to_dict()["output_schema"]["required"] == ["summary"]
    assert workflow.plan_hash == original_hash


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("required", "yes"),
        ("allow_failed_dependencies", 1),
    ],
)
def test_task_boolean_policies_require_actual_booleans(field: str, value: object) -> None:
    with pytest.raises(DagValidationError, match="must be a boolean"):
        TaskDefinition("facts", "agent", **{field: value})


@pytest.mark.parametrize("keyword", ["oneOf", "requireed"])
def test_task_rejects_unsupported_or_misspelled_schema_keywords(keyword: str) -> None:
    with pytest.raises(DagValidationError, match="unsupported schema keywords"):
        TaskDefinition(
            "facts",
            "agent",
            output_schema={
                "type": "object",
                "properties": {},
                keyword: [],
            },
        )
