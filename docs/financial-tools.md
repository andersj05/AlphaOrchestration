# Financial tools

## Status and design boundary

AlphaOrchestration now has a provider-neutral, read-only finance calculation suite:
six compact agent-facing tools backed by deterministic Python handlers and strict JSON
contracts. The suite calculates and sorts; it does not fetch data, normalize provider
facts, choose assumptions, or make an investment judgment.

The intended division of work is:

- the orchestrator supplies the role-specific tool allowlist and the relevant evidence;
- the agent selects a calculation, names explicit assumptions, and interprets the result;
- the tool validates inputs and owns the arithmetic, sorting, denominator checks,
  rounding, and formula metadata;
- a later validator decides whether the result is sufficiently grounded for a report.

The fixed-DAG runtime now injects task-scoped contracts into model requests, parses
model-authored action envelopes, preflights a complete call batch, executes accepted
calls through a scoped registry, and journals proposed/resolved arguments and exact
result envelopes. A task's bounded `EvidencePacket` supplies the observation records
and controller-owned source allowlist.

For normalized finance tasks, the model refers to observation IDs. The controller owns
fact resolution, compatibility checks, source IDs, and context fields, so a model cannot
replace a packet value with its own number. This binding is implemented for arithmetic,
metrics, growth forecasts, DCF, and market statistics. Ranking remains available as a
deterministic registry tool, but normalized fixed-DAG calls to `finance.rank` fail
closed until trusted task-output row binding exists.

## Compact catalog

Each public contract is versioned independently from its formulas. The initial contract
version is `1.0.0`; contracts are read-only and idempotent.

| Tool | Agent supplies | Deterministic result and coverage |
| --- | --- | --- |
| `finance.calculate` | A batch of named operations and their operands | Up to 100 results with value, unit, and formula. Operations are `sum`, `difference`, `product`, `ratio`, `percent_change`, `basis_point_change`, `cagr`, `mean`, `median`, and `weighted_average`. It never evaluates model-authored expressions. |
| `finance.metrics` | Normalized financial facts plus an optional list of named metrics | Calculated values, per-metric category/unit/formula/input detail, and explicit unavailable reasons. Omitting `metrics` requests the whole 47-metric catalog, subject to available inputs. |
| `finance.forecast_growth` | A positive base, period count, and one or more named scenarios containing a constant rate or one rate per period | A compounded period schedule, ending value, total growth, and CAGR for each scenario. The agent owns the assumptions; the tool owns every roll-forward. |
| `finance.discounted_cash_flow` | Forecast cash flows, discount rate, terminal method, and optional timing, net-debt, and share inputs | Forecast PV schedule, perpetuity-growth or exit-multiple terminal value, enterprise value, equity value, and optional per-share value. |
| `finance.market_statistics` | Exactly one price or simple-return series, explicit periods per year and annual risk-free rate, plus an optional aligned benchmark-return series | Total and annualized return, annualized volatility, Sharpe, Sortino, maximum drawdown, best/worst-period return, positive-period ratio, and optional beta, annualized alpha, and correlation. |
| `finance.rank` | Entity rows, weighted criteria, a higher/lower direction for each criterion, and a missing-value policy | Tie-aware metric ranks and percentile scores, normalized weights, composite scores, a stable result order, and excluded-row reasons. |

The catalog is deliberately coarse-grained. For example, 47 named statement metrics
share one `finance.metrics` contract rather than consuming 47 tool names, and generic
arithmetic is batched through one bounded operation enum.

## Formula and coverage map

The named metric catalog covers the following first-slice categories:

| Category | Implemented metrics or formulas |
| --- | --- |
| Derived amounts and per-share values | Gross profit; free cash flow; working capital; net debt; enterprise value; earnings per share; book value per share. |
| Growth | Revenue, gross-profit, operating-income, net-income, and EPS growth, each `(current - prior) / prior`. |
| Margins | Gross, operating, EBIT, EBITDA, net-income, operating-cash-flow, and free-cash-flow margins; capex intensity; incremental operating margin. |
| Liquidity and leverage | Current and quick ratios; debt/equity; debt/EBITDA; net debt/EBITDA; interest coverage. |
| Efficiency and returns | Asset turnover; cash-conversion ratio; return on assets, equity, and invested capital. |
| Working-capital cycle | Days sales outstanding, days inventory outstanding, days payables outstanding, and cash conversion cycle. |
| Valuation and yields | P/E, P/S, P/B, EV/revenue, EV/EBITDA, EV/EBIT, free-cash-flow yield, earnings yield, dividend yield, and payout ratio. |
| Composite | Rule of 40 as decimal revenue growth plus decimal EBITDA margin. |
| Scenario and intrinsic value | Period compounding, total growth, CAGR, discounted cash flows, Gordon-growth terminal value, exit-multiple terminal value, enterprise-to-equity bridge, and per-share value. |
| Market return and risk | Compounded/annualized return, sample annualized volatility, Sharpe, Sortino, drawdown, beta, CAPM-style annualized alpha, and correlation. |
| Cross-sectional ordering | Direction-aware, tie-aware percentile scoring; normalized weighted composite score; deterministic ID tie-break. |

Several named metrics can derive common components when the normalized input permits it.
Examples include gross profit from revenue and cost of revenue, market capitalization
from price and diluted shares, EBIT from operating income, EBITDA from EBIT plus D&A,
NOPAT from EBIT and a decimal effective tax rate, and enterprise value from market cap,
debt, preferred stock, minority interest, and cash. Direct normalized values take
precedence when supplied. These conveniences do not replace statement normalization or
accounting judgment.

## Role allowlists

The trusted application, not the model, selects a role's allowlist. The current policy
map is:

| Agent role | Allowed finance tools |
| --- | --- |
| `universe` | `finance.rank` |
| `filings` | `finance.calculate` |
| `market` | `finance.calculate`, `finance.market_statistics`, `finance.rank` |
| `fundamentals` | `finance.calculate`, `finance.metrics`, `finance.forecast_growth` |
| `valuation` | `finance.calculate`, `finance.metrics`, `finance.forecast_growth`, `finance.discounted_cash_flow` |
| `catalysts` | `finance.calculate`, `finance.market_statistics` |
| `risk` | `finance.metrics`, `finance.forecast_growth`, `finance.discounted_cash_flow`, `finance.rank` |
| `lead` | `finance.rank` |

The registry exposes only selected public contracts and a scoped executor rejects
out-of-scope calls. A `TaskDefinition.allowed_tools` tuple is the authoritative
fixed-DAG allowlist inserted into a model turn. The role map above is a reusable policy
default, not authority inferred from a model-provided role. Role policies that include
`finance.rank` still require a trusted non-observation row-binding path before that
tool can run inside the normalized fixed-DAG runtime.

## Input, output, and evidence lineage

All tool inputs use strict object schemas. Unknown properties, booleans in numeric
positions, non-finite numbers, over-sized arrays, and malformed enums are rejected.
There is no free-form expression evaluator and no implicit provider access. Shared input
fields are:

- `source_ids`: up to 100 unique evidence IDs for all sourced facts used by the call;
- `context`: optional `currency`, `scale`, `current_period`, `prior_period`, and
  `period_type` metadata, plus optional `ticker`, `entity_id`, and `as_of` identity/as-of
  labels;
- `precision`: output decimal places from 0 through 12, defaulting to 6.

On success, the registry returns a `ToolResult` with the original `call_id`, a compact
`payload`, and the supplied `source_ids` copied unchanged into the result's lineage
field. The calculator never invents evidence IDs. `context` is echoed in calculation
data so a downstream explanation can retain unit and period scope.

In normalized fixed-DAG execution, `ObservationLedger` creates a self-contained
`EvidencePacket`. The controller resolves each proposed observation reference, derives
the exact source IDs from that observation, and injects context from its unit and period.
Unknown observations, incompatible entity/currency/scale combinations, model-supplied
controller fields, and citations outside the derived allowlist are rejected. Manual
opaque source IDs are disabled by default; the explicit compatibility flag
`allow_unverified_sources=True` is required to use them.

The success payload has the common outer shape
`{"ok": true, "tool": "...", "data": {...}}`. Invalid inputs, unknown or disallowed
tools, conflicting call IDs, calculation failures, and over-sized results use compact
structured errors instead of Python exceptions or non-finite JSON. Result size defaults
to a 256 KB maximum.

Within one registry instance, replaying the same `call_id`, tool name, and arguments
returns the cached result. Reusing that ID with different content returns
`call_id_conflict`. This provides execution idempotency. Separately, fixed-DAG
lifecycle events durably journal the complete result envelope and its canonical hash;
journal replay projects those recorded events and never re-executes the tool.

## Financial conventions

### Rates and percentages

Rates, returns, growth, margins, yields, tax rates, and DCF rates use decimal form:
`0.10` means 10%, not 10. `percent_change` likewise returns a decimal ratio. A
`basis_point_change` expects two decimal rates and multiplies their difference by
10,000; moving from `0.20` to `0.24` is `400` basis points.

Growth rates and public-tool simple returns must be greater than -100%. DCF discount
rates must also be greater than -100%, and a perpetuity growth rate must be lower than
its discount rate.

The market tool treats the risk-free input as an annual decimal rate and divides it by
the explicit `periods_per_year` for periodic comparisons.

### Capital expenditures

`capital_expenditures` is a positive cash outlay. Consequently:

```text
free_cash_flow = operating_cash_flow - capital_expenditures
capex_intensity = capital_expenditures / revenue
```

A provider that reports cash-flow-statement capex as a negative number must be
normalized to a positive outlay before calling `finance.metrics`. Passing the provider's
negative sign through would incorrectly add capex to free cash flow.

### Net debt

Net debt is positive when debt exceeds cash:

```text
net_debt = total_debt - cash_and_equivalents
equity_value = enterprise_value - net_debt
```

A negative `net_debt` represents net cash and therefore increases the DCF equity value
when subtracted. DCF defaults omitted net debt to zero; callers should rely on that only
when the assumption is intentional and visible.

### Units and periods

The tools do not convert units, currencies, share scales, or fiscal calendars. Normalize
amount inputs to one currency and scale and compare like periods before calculation. For
example, using USD millions for income-statement values requires compatible USD-million
balance-sheet values; per-share math also requires a compatible share-count scale.

Use `context` to retain the currency, scale, current/prior labels, and period type. The
optional ticker, entity ID, and as-of fields retain identity and observation-time labels.
The free-form `unit` accepted by forecast and DCF is descriptive and echoed, not
interpreted. It cannot repair mixed inputs.

Forecast labels describe forecast periods only; the base value sits immediately before
period 1. DCF uses end-of-period timing by default. With `mid_year: true`, period `t` and
the terminal value are discounted at `t - 0.5`. Market data frequency is never inferred:
the caller must supply `periods_per_year`, and benchmark returns must already align
one-for-one with asset returns. Working-capital day metrics default to 365 days unless
`days_in_period` is supplied, so quarterly or stub-period analysis should set it
explicitly.

## Missing values and zeros

Missing is not the same as zero:

- `finance.metrics` marks only affected metrics unavailable and includes a stable reason;
  other requested metrics still calculate. Omitted metric names request the full catalog.
- A zero denominator makes the affected named metric unavailable. In the generic
  calculator, a zero denominator is an invalid operation and rejects that batch rather
  than emitting infinity.
- Enterprise-value derivation treats omitted preferred stock and minority interest as
  zero. Quick-assets derivations may treat omitted prepaid expenses or short-term
  investments as zero. These are narrow, documented derivation defaults, not a general
  missing-to-zero rule.
- DCF returns `per_share_value: null` when shares are omitted and rejects a zero share
  count. An exit multiple defaults its terminal metric to the final forecast cash flow
  only when no explicit terminal metric is supplied.
- Market statistics return `value: null` plus `unavailable_reason` for undefined results,
  such as Sharpe with zero volatility or beta without a benchmark. Misaligned series are
  invalid inputs rather than missing data.
- Ranking represents a missing criterion by an absent metric key. `exclude` removes the
  row, `worst` assigns a zero percentile score for that criterion, and `neutral` assigns
  50; the output records missing metrics and exclusions. Public-tool criterion weights
  are positive and normalized, and their sum must be more than zero.

No tool emits NaN or infinity. Structurally bad requests fail as a whole; mathematically
unavailable named metrics use the local partial-result behavior described above.

## Formula versions and rounding

Contract version and formula version serve different purposes: the former describes the
call schema, while the latter identifies the calculation semantics carried in results.

| Tool | Formula version |
| --- | --- |
| `finance.calculate` | `finance-arithmetic-v1` |
| `finance.metrics` | `finance-metrics-v1` |
| `finance.forecast_growth` | `finance-growth-forecast-v1` |
| `finance.discounted_cash_flow` | `finance-dcf-v1` |
| `finance.market_statistics` | `market-statistics-v1` |
| `finance.rank` | `entity-ranking-v1` |

Results include `rounding.mode: "half_even"` and the requested decimal places.
Arithmetic, statement metrics, forecasts, and DCF use high-precision decimal arithmetic;
forecast and DCF schedules explicitly calculate from unrounded intermediate values.
Market statistics and ranking use finite numeric routines and apply decimal half-even
rounding at the result boundary. Per-operation, per-metric, forecast, DCF, and market
outputs carry formula text where applicable; ranking publishes its algorithm version
and component scores. Downstream prose should cite returned values and versions rather than recreate
the math.

## One action/result example

This direct registry call asks the tool to calculate two facts whose lineage has already
been resolved by a trusted caller:

```json
{"name":"finance.calculate","call_id":"calc-17","arguments":{"operations":[{"id":"revenue_growth","operation":"percent_change","current":125,"prior":100},{"id":"margin_move","operation":"basis_point_change","current":0.24,"prior":0.20}],"source_ids":["sec:issuer:FY2025:revenue","sec:issuer:FY2024:revenue"],"context":{"current_period":"FY2025","prior_period":"FY2024","period_type":"year"},"precision":4}}
```

The deterministic result is equivalent to:

```json
{"call_id":"calc-17","payload":{"ok":true,"tool":"finance.calculate","data":{"results":[{"id":"revenue_growth","operation":"percent_change","value":0.25,"unit":"ratio","formula":"(current - prior) / prior"},{"id":"margin_move","operation":"basis_point_change","value":400.0,"unit":"basis_points","formula":"(current - prior) * 10000"}],"context":{"current_period":"FY2025","prior_period":"FY2024","period_type":"year"},"rounding":{"mode":"half_even","decimal_places":4},"formula_version":"finance-arithmetic-v1"}},"source_ids":["sec:issuer:FY2025:revenue","sec:issuer:FY2024:revenue"],"retryable":false}
```

The agent's job is to explain that revenue grew 25% and the selected margin expanded
400 basis points, while retaining the evidence IDs and period scope. It should not
recalculate either value in prose. In normalized fixed-DAG mode, the model instead
proposes `{"observation_id":"..."}` references for `current` and `prior`; the
controller produces the numeric call above and records both forms.

## Why this pattern fits Qwen3.5 0.8B

A 0.8B model is much more reliable at choosing among a few bounded actions and
interpreting structured results than at carrying long arithmetic chains in its hidden
state. Six compact tools reduce tool-selection and schema entropy. Enums replace
open-ended expressions, batched operations reduce turn count, and role allowlists remove
irrelevant choices before decoding.

Deterministic precomputation also returns the intermediate schedule, formula, units,
periods, unavailable reasons, and version that the model needs to write a grounded
answer. That moves fragile arithmetic, sorting, tie handling, compounding, and repeated
discounting out of the LLM while preserving the small amount of judgment it should own:
which facts are comparable, which scenario assumptions are defensible, which ranking
direction matters, and what the result means.

## Next slices

1. **Trusted ranking inputs:** bind `finance.rank` rows to normalized task outputs or
   controller-built records instead of accepting model-authored row values.
2. **Sensitivity, IRR, and XIRR:** add bounded one- and two-dimensional sensitivity
   tables, deterministic scenario grids, periodic IRR, date-aware XIRR, explicit day-count
   conventions, root-selection rules, and convergence/error metadata.
3. **Statement bridges:** add audited income-statement, balance-sheet, and cash-flow
   bridges, including EBITDA-to-NOPAT-to-unlevered-FCF, working-capital changes,
   enterprise-to-equity adjustments, and diluted-share roll-forwards.
4. **Production execution controls:** add global workflow time/cost budgets, task and
   provider timeouts, durable tool-idempotency state, concurrent fixed-DAG scheduling,
   and the supervised KernelCubed model adapter.
