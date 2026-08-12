# Normalized financial data

## Boundary and record model

Provider payloads are normalized before they reach an agent or a finance
calculator. The normalization layer is deterministic and contains no investment
judgment. It owns names, signs, units, exact periods, provider locators,
deduplication, and malformed-record handling; later orchestration code chooses
which comparable observations to give an agent.

An `ObservationBatch` is a self-contained strict-JSON envelope with three lists:

- `FinancialObservation` records contain a canonical name, finite integer or
  float value, entity identity, optional ticker, `FinancialUnit`,
  `FinancialPeriod`, evidence IDs, and provider metadata.
- `EvidenceRecord` records contain a stable evidence ID, provider, source kind,
  exact provider locator (including the reported value), source URL, observed and
  retrieval timestamps, and a SHA-256 content hash.
- `NormalizationIssue` records describe malformed supported provider values.
  Issues are capped at 100 per batch and the final record reports truncation.

Every type has explicit `to_dict()` and `from_dict()` methods. Serialization uses
finite JSON numbers rather than `Decimal`, NaN, or infinity so a batch can be
journaled and replayed using the existing `JsonValue` contract. Timestamps are
timezone-aware and normalized to UTC. Accounting periods use ISO dates and are
explicitly either `instant` or `duration`; duration periods require both a start
and end date.

Evidence and observation IDs are derived from canonical sorted JSON and SHA-256,
not random UUIDs. Reprocessing the same provider fact with the same locator and
content produces the same IDs. A content change produces a new evidence ID.
Every observation references evidence included in its batch, and batches reject
duplicate or unresolved IDs.

## Units and signs

The normalized unit kinds are `currency`, `shares`, `currency_per_share`,
`ratio`, and `count`. Provider values currently remain at the `units` scale;
there is no implicit thousands/millions conversion or currency conversion.
SEC ISO currency codes are retained separately, so otherwise identical USD and
EUR facts remain distinct observations.

The SEC mapper preserves reported signs except for canonical cash outlays that
the deterministic finance tools define as positive:

- `capital_expenditures`
- `dividends_paid`
- `dividends_per_share`

For these names, the normalized value is the absolute reported value. The raw
value and the `absolute` sign policy remain in the observation metadata and
evidence locator.

## SEC company-facts mapping

`map_sec_company_facts(payload, retrieved_at=..., forms=..., ticker=...)`
maps a bounded, explicit set of `us-gaap` and `dei` concepts. Coverage includes
income-statement, balance-sheet, cash-flow, per-share, share-count, working
capital, debt-component, tax, D&A, dividend, and equity facts needed by the first
finance-tool slice. Unknown extension concepts are ignored rather than emitted as
thousands of low-value issues.

The mapper preserves:

- CIK identity and optional ticker/entity name;
- taxonomy, concept, label, provider unit, form, accession, filing date, frame,
  and the reported value;
- exact start/end dates, fiscal year, and fiscal-period label;
- instant versus duration context.

SEC company facts frequently repeat comparative periods in subsequent filings.
For an equal canonical name, unit, and exact start/end period, the latest filing
wins. An explicit concept priority resolves legacy aliases in the same filing.
This selects later restatements while retaining distinct quarterly, year-to-date,
and annual durations. No arithmetic aggregation occurs in the mapper: current
debt, long-term debt, and short-term debt remain components unless the SEC reports
a supported total-debt concept directly.

Supported forms default to `10-K`, `10-Q`, `10-K/A`, and `10-Q/A`. Other forms
are silently outside the mapping scope. Malformed known facts produce bounded
issues for missing forms/accessions, bad dates or numbers, invalid unit arrays,
and unsupported units. Root payloads without a valid CIK fail because durable
entity identity cannot be inferred.

## yfinance mapping

`map_yfinance_snapshot(snapshot, observed_at=..., retrieved_at=...)` maps the
existing `MarketSnapshot` projection to canonical `share_price` and `market_cap`
observations. Currency is required; the mapper reports `missing_currency` and
emits no monetary observations rather than guessing.

`map_yfinance_history(ticker, rows, currency=..., auto_adjust=...,
retrieved_at=..., interval="1d")` accepts the provider-neutral plain records
already returned by `YFinanceClient.history`. It maps:

| yfinance field | Canonical name | Unit |
| --- | --- | --- |
| Open / High / Low / Close | `open_price`, `high_price`, `low_price`, `close_price` | currency per share |
| Adj Close | `adjusted_close_price` | currency per share |
| Volume | `volume` | shares |
| Dividends | `dividends_per_share` | currency per share |
| Stock Splits | `stock_split_ratio` | ratio |
| Capital Gains | `capital_gains_per_share` | currency per share |

Date and Datetime keys are accepted. ISO offsets are converted to UTC; a daily
date or a naive provider datetime is treated as UTC and the policy is visible in
the evidence timestamp. `auto_adjust`, interval, and whether an OHLC value was
adjusted remain explicit metadata. Zero dividends, splits, and capital gains are
no-event placeholders and are omitted; zero volume remains valid. Non-finite or
domain-invalid cells become issues. If duplicate rows provide the same canonical
field and timestamp, the later row wins and a duplicate issue is recorded.

## Observation ledger and evidence packets

`ObservationLedger` atomically ingests one or more normalized batches. Re-ingesting
an identical stable ID is idempotent; reusing an evidence or observation ID for
different immutable content raises `LedgerCollisionError` before any part of the
batch is added. Exact entity/name and entity/name/period selectors retain complete
unit, period, provider metadata, and evidence references.

`ObservationLedger.evidence_packet(...)` resolves selected observations and every
referenced `EvidenceRecord` into a deterministic, self-contained `EvidencePacket`.
It raises rather than silently truncating when the requested observation, evidence,
or encoded-byte bound is exceeded. A packet's sorted evidence IDs are the
controller-owned source allowlist for that task.

`FixedDagRuntime` accepts packets through `evidence_packets_by_task`. For normalized
finance calls, the model proposes observation references, not copied provider numbers.
The controller resolves the values, rejects observations outside the packet or
incompatible entities/currencies/scales, injects unit/period/as-of context and exact
source IDs, validates the resolved arguments against the trusted tool schema, and only
then executes the call. Every call in a multi-call action is preflighted before any tool
runs. Manual opaque source IDs are rejected by default and require an explicit
`allow_unverified_sources=True` compatibility opt-in.

This binding path covers `finance.calculate`, `finance.metrics`,
`finance.forecast_growth`, `finance.discounted_cash_flow`, and
`finance.market_statistics`. `finance.rank` fails closed with
`binding_not_supported` until trusted task-output row binding is implemented.

Normalization itself still does not compute totals, averages, TTM series, FX
conversion, market returns, or financial ratios. Deterministic tools own those
operations; journal events retain proposed/resolved arguments, evidence lineage, and
the returned result envelope. SEC and yfinance fetches remain explicit adapter calls:
building or executing an evidence-bound finance task never performs a provider request.

## Verification harness

The focused tests cover:

- strict-JSON round trips and stable content-derived IDs;
- unit, period, finite-number, and evidence-reference invariants;
- SEC instant/duration contexts, restatements, concept priority, currencies,
  capex sign normalization, malformed values, and issue bounding;
- yfinance snapshot/history fields, timezone handling, corporate actions,
  adjustment metadata, duplicate rows, missing currency, non-finite values, and
  deterministic output;
- ledger idempotency, atomic collision rejection, deterministic selection and
  serialization, packet completeness, hard limits, and offline adapter re-exports; and
- controller-owned observation binding, fabricated/untrusted-reference rejection,
  compatibility checks, and source-lineage journaling.

All mapper fixtures are offline provider-shaped dictionaries. Tests and the fixed-DAG
harness never depend on live SEC or Yahoo availability. See
[Testing harness](testing-harness.md) for exact commands.
