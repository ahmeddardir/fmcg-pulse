# Development Notes

Design decisions, tradeoffs, and implementation rationale for `fmcg-pulse`. The following are mostly personal notes that I'm documenting for my own learning journey. Just me thinking out loud, explaining my decisions as I add more to the project.

---

## Configuration models (`config.py`)

Config comes from PyYAML, which deserializes `config.yaml` into plain Python primitives. This means `__post_init__` has real coercion work to do: `"2025-01-01"` needs to become a `date`, and `{"raw_dir": "data/raw", ...}` needs to become a `PathsConfig`.

Validation also belongs here. Config is loaded once at startup, and bad values should abort immediately with a clear error before the pipeline does any work. Misconfigured data discovered mid-run is harder to diagnose than one caught at startup, because by then the pipeline has already read files, allocated memory, and potentially written partial output. Failing early keeps the failure surface small and the error message unambiguous.

### Coercion pattern: prefer Pattern B

Two approaches exist for conditional coercion:

```python
# Pattern A: coerce if it's a str
if isinstance(self.start_date, str):
    self.start_date = date.fromisoformat(self.start_date)

# Pattern B: coerce unless already the right type (preferred)
if not isinstance(self.start_date, date):
    self.start_date = date.fromisoformat(self.start_date)
```

Pattern A only handles the `str` case. If something unexpected comes in (say, an `int` timestamp or a `datetime` object), it slips through silently and the field ends up with the wrong type. Downstream code that calls `.strftime()` or does date arithmetic will then fail with a confusing `AttributeError` far from the actual source of the problem.

Pattern B makes the assertion explicit: "this field must be a `date` by the time construction finishes." Anything that isn't already a `date` triggers coercion, and if coercion fails, it fails immediately and clearly at construction time.

### `Report`, `ReportFilters`, and `TimeGrain`

`Report` is the most complex config dataclass. It owns four concerns: grouping dimensions, partition columns for window-based metrics, time grain for period-level aggregation, and date range filters.

`partition_by` is normalized to a list in `__post_init__` so `transforms.py` never has to handle the `str | list[str]` union. An empty list is normalized to `None`. Every element is validated against `dimensions` to catch config errors early.

`TimeGrain` uses `StrEnum` for the same reason `LogLevel` does: constrained string values that serialize cleanly and produce clear error messages on invalid input.

`ReportFilters` is a separate dataclass rather than inline fields on `Report` for two reasons. First, it groups related fields (`date_from`, `date_to`) into a named unit that is self-documenting in config. Second, it keeps `Report.__post_init__` from becoming responsible for date coercion logic on top of everything else it already does.

`time_grain` and `dimensions` are intentionally independent. The pipeline prepends the derived time column to the groupby automatically when `time_grain` is set; requiring the user to also include it in `dimensions` would be redundant. Time grain always appears as the outermost grouping column in the output.

`time_grain` also affects `partition_by` at transform time. If `time_grain` is set, the derived time column is prepended to `partition_by` automatically (if not already present), so market share is always computed within the correct time period.

### What not to validate

Two examples from this project:

`market` in `PipelineConfig` is a free-form label used in the run manifest. Validating it would mean maintaining an exhaustive list of valid market names in code, which breaks every time a new market is onboarded. The value causes no silent incorrect behavior if wrong; it just shows up as an unexpected string in the output, which is immediately visible.

`standard_format` and `json_format` in `LoggingConfig` are Python logging format strings. Validating them properly would mean parsing `%()s` placeholders and checking each field name against the `LogRecord` attribute list. This is significant complexity for minimal gain. If the format string is malformed, the logging system raises a `ValueError` at startup when it tries to apply it, which is the same outcome as explicit validation but without the maintenance burden.

> Validation occurs where bad values would cause silent incorrect behavior, and is skipped where failures are already loud or the maintenance cost outweighs the risk.

---

## Data models (`data.py`)

Data models are constructed by our own code. By the time a `Product` or `Transaction` is instantiated, we control what's going in. This is different from config, where an external tool (PyYAML) controls what types arrive.

This means:

- **No coercion.** Data model dataclasses have no `__post_init__`. The generator always passes correctly typed values, and the ingestion layer uses Polars scan functions.
- **Type aliases** are for clarity only. `Barcode`, `TrnId`, and `StoreId` are all `str` at runtime.
- **Row-level validation** is intentionally absent from `__post_init__`. See below.

### `data.py` as the schema source of truth

`data.py` also owns the Polars schema for `Product` and `Transaction`. A module-level `PY_TO_PL` constant maps Python types to Polars dtypes. A `_schema_from_dataclass()` helper uses `typing.get_type_hints()` to resolve field annotations (including type aliases like `TrnId`) to their underlying Python types, then maps each to its Polars equivalent.

`get_type_hints()` is necessary here because `dataclasses.fields()` returns the raw annotation string for type aliases rather than the resolved type. For example, `trn_id: TrnId` would yield `"TrnId"` from `field.type`, not `str`. `get_type_hints()` resolves this to `str` before the mapping runs, so `PY_TO_PL` only needs to know about `str`, `int`, `float`, `bool`, and `date`.

Both `Product` and `Transaction` expose a `get_schema()` classmethod that calls `_schema_from_dataclass(cls)`. `readers.py` calls these methods directly rather than defining its own schema dicts. `generators.py` uses `dataclasses.fields(Transaction)` to derive the CSV header and row values. This means a field rename in either dataclass propagates automatically.

### Validation layer separation

| Layer                       | When                     | Why                                             |
| --------------------------- | ------------------------ | ----------------------------------------------- |
| `config.py` `__post_init__` | Startup, once            | Bad config should be fatal immediately          |
| `validators.py`             | After ingestion, in bulk | High volume; failures are reportable, not fatal |

`Transaction` and `Product` are constructed once per row. Running validation checks inside `__post_init__` means thousands of individual Python function calls, each checking conditions one row at a time.

Instead, `validators.py` uses Polars to run the same checks across the entire dataset in a single vectorized operation. Beyond performance, this produces better output: instead of raising on the first invalid row and halting the pipeline, the validator collects all violations and reports them together. That information feeds directly into `RunManifest`, giving operators a clear picture of data quality per run.

---

## Logging (`logging_config.py`)

`setup_logging()` configures the root logger once at pipeline startup (handlers, formatters, levels). That's its only job. It does not return a logger.

Individual modules obtain their own logger at module level:

```python
import logging
logger = logging.getLogger(__name__)
```

`__name__` resolves to the fully qualified module name (`fmcg_pulse.transforms`, `fmcg_pulse.readers`, etc.). Because all module loggers inherit from the root logger, they automatically use the handlers and formatters configured by `setup_logging()`.

### Why three handlers

A pipeline run produces two kinds of log consumers: humans and machines.

- **Console:** immediate feedback during development and manual runs.
- **Rotating file (standard):** persistent human-readable record. Useful when a run finishes and an operator needs to review what happened without having watched the terminal. Rotation prevents log files from growing unbounded across many runs.
- **Rotating file (JSON):** structured output where every log entry is a parseable JSON object. This enables post-hoc analysis with standard tools, log aggregation pipelines, or automated alerting.

Both file handlers use `RotatingFileHandler` with a 10MB cap and 5 backups, bounding total log disk usage to ~100MB per log type regardless of how many runs accumulate.

### Log filename formatting

Log files are named `pipeline_YYYY-MM-DD_HHMMSS.log` so each run gets its own file and runs never overwrite each other. This makes it possible to correlate a log file with a specific `run_manifest.json` by timestamp.

`str(datetime.now())` produces `2026-03-04 14:30:22.123456`. The space and colons are invalid in filenames on some systems, so `strftime("%Y-%m-%d_%H%M%S")` is used instead to produce a clean, sortable, filesystem-safe string.

### Directory creation

`setup_logging()` does not create the log directory. This is a deliberate separation of concerns.

Directory creation is a filesystem side effect with its own failure modes. It belongs in the pipeline entrypoint, where all startup side effects are handled in one place and in a predictable order.

---

## Decorators (`decorators.py`)

### `log_execution_time`

A simple timing decorator. Wraps a function, measures wall-clock duration via `time.perf_counter()`, and logs the result at `DEBUG` level. Uses `functools.wraps` to preserve the original function's metadata.

### `retry_on_failure`

A parameterized decorator factory that retries the wrapped function on any exception. Uses exponential backoff (`base_delay * 2^attempt`) with a random jitter term and a configurable `max_delay` cap. Sleeps are skipped on the final attempt to avoid a pointless delay before raising.

If all attempts fail, the decorator raises `RetriesExhaustedError` (a custom exception defined in the same module) chained to the last caught exception via `from last_exc`. This preserves the original traceback while giving callers a single exception type to catch when retries are exhausted.

---

## Product catalog (`catalog.py`)

### `CatalogEntry` frozen dataclass

Catalog entries were originally plain dicts (`list[dict]`). They are now `CatalogEntry` frozen dataclass instances (`list[CatalogEntry]`). This gives attribute access instead of string-keyed dict access, catches typos at the type checker level, and makes the catalog immutable so entries cannot be accidentally mutated during generation.

### `ref_price` on `Product`, not in the catalog

Price is transactional in real FMCG data. This project preserves that: `unit_price` lives in transactions, never in the product catalog. `ref_price` on `Product` is a generation artifact only. Without it, the same barcode would have random prices across transactions and `avg_selling_price` would be noise.

Each catalog entry defines a `price_range: tuple[float, float]`: the regular shelf price band in EUR for the **smallest size** in the entry's `sizes` list, based on 2024/2025 German supermarket pricing. The generator samples a `base_price` from this range, then scales it by pack size to derive `ref_price`. Transactions apply &plusmn;15% jitter around `ref_price`.

### Pack size scaling

Each catalog entry carries a `sizes` list of unitless quantities in the natural unit for the product type (litres for liquids, kg for solids, count for tablets/pouches/nappies). Sizes are ordered smallest to largest; `sizes[0]` is the pricing base.

The generator scales price sublinearly across sizes:

```python
multiplier = (size / entry.sizes[0]) ** 0.85
ref_price = round(base_price * multiplier, 2)
```

The `0.85` exponent reflects real bulk discount behavior: larger packs are cheaper per unit, but not proportionally so. This constant lives in `generators.py`, not here.

### `descriptors` and product name construction

Each entry carries a `descriptors` list of variant names like `"Universal Gel"` or `"Lemon Liquid"`. The generator samples one descriptor per product, then constructs the product name as `f"{brand} {descriptor} {formatted_size}"`. Size formatting is handled by the generator based on keyword inference: liquids format as `"500ml"` or `"1L"`, solids as `"100g"` or `"1kg"`, count-based products as `"40 Tabs"`.

### `is_private_label`

`is_private_label` is a boolean on `Product` and carried per entry in the catalog. Private label share vs. manufacturer brand share is a standard FMCG split and makes the brand performance report more analytically meaningful. A small number of catalog entries represent private label archetypes with `is_private_label: True` and lower price ranges.

---

## Data generation (`generators.py`)

### Unit type inference: two-pass keyword/subcategory approach

`_infer_unit_type(descriptor, sub_category)` classifies each product as `"liquid"`, `"solid"`, or `"count"` to determine how to format its size string. It runs two passes in order:

1. Descriptor keyword match: checks individual words in the descriptor against three `frozenset[str]` constants (`LIQUID_KEYWORDS`, `SOLID_KEYWORDS`, `COUNT_KEYWORDS`). Returns immediately on first match.
2. Sub-category fallback: checks the sub-category string against three corresponding subcategory frozensets. Covers cases where the descriptor contains no classifiable keywords (e.g. `"Naturjoghurt 3.5%"` has no keyword match but `"Yoghurt"` is in `SOLID_SUBCATEGORIES`).

If neither pass resolves, the function logs a warning and returns `"unknown"`. Products with an `"unknown"` unit type are skipped during generation. This is preferable to assigning a silent default, which would produce malformed size strings with no indication anything went wrong.

### `build_products` returns a list, not a generator

`build_products` is named to distinguish it from `generate_transactions`, which is a Python generator. The distinction is intentional: `build_products` constructs and returns a complete `list[Product]`; `generate_transactions` yields one `Transaction` at a time.

Products are returned as a list because `generate_all()` needs the full set twice: once to serialize to NDJSON and once to pass to `generate_transactions()`, which samples from it randomly. A generator would require buffering the full set anyway, so a list is the natural fit. The catalog is bounded at a few hundred entries at most, so memory is not a concern.

### Duplicate product name guard and attempt cap

`build_products` samples catalog entries one at a time in a `while` loop until it has produced exactly `n_products` unique products. Two failure modes needed guarding against:

- **Duplicate names:** the same archetype can be sampled multiple times, producing identical names. A `seen_names: set[str]` tracks constructed names and skips collisions. Collisions do not count against the attempt limit; they are expected and harmless.
- **Unresolvable unit types:** if `_infer_unit_type` returns `"unknown"`, the entry is skipped and the attempt counter increments. If too many catalog entries produce `"unknown"` returns, the loop could spin indefinitely. A cap of `n_products * 3` bounds this. If the cap is hit, the function logs a warning and returns however many products were generated.

The maximum number of possible unique products is validated upfront via `sum(len(entry.descriptors) * len(entry.sizes) for entry in catalog)`. If `n_products` exceeds this, the function raises immediately rather than hitting the attempt cap silently.

### `generate_transactions` is a generator to avoid list accumulation

`generate_transactions` yields one `Transaction` at a time rather than accumulating a `list[Transaction]` in memory. At 50,000 transactions the list would be manageable, but the volume is unbounded by design.

`generate_all` consumes the generator and writes each row to CSV immediately, so the full transaction list never exists in memory.

### `generate_all` owns all file I/O

`build_products` and `generate_transactions` are pure generation functions: they do not open files, write to disk, or know where output lands. `generate_all` is the coordinator. It receives `AppConfig`, resolves output paths, serializes products to NDJSON, and streams transactions to CSV row by row.

This separation keeps the generation functions testable in isolation (no filesystem required) and gives `generate_all` a single, clear responsibility: coordinate and persist.

### NDJSON output for products

`generate_all` writes products as newline-delimited JSON (one JSON object per line) rather than a JSON array. This is required for `pl.scan_ndjson()` in the ingestion layer.

---

## Data ingestion (`readers.py`)

`readers.py` provides two functions that form the ingestion boundary: `scan_products()` and `scan_transactions()`. Both return `pl.LazyFrame` via `pl.scan_ndjson` and `pl.scan_csv` respectively.

`generators.py` writes its output to disk as NDJSON and CSV. `readers.py` scans those files back as the ingestion layer's input.

This separation exists for three reasons. First, the pipeline can run ingestion independently of generation. If the data already exists on disk from a previous run, generation is skipped entirely and ingestion picks up from the file. Second, it mirrors how a real pipeline works: in production, product catalogs arrive as files from upstream systems, not as live Python objects. Third, `RunManifest` tracks `unique_products` and `unmatched_barcodes` against what was actually read from disk. Those counts are only meaningful if ingestion is a real, fallible step with its own audit boundary.

### Why LazyFrame instead of eager readers

The initial design used a Python generator (`read_transactions`) that yielded `Transaction` dataclasses one at a time and a `read_products` function that returned `list[Product]`. After some research, this was replaced with Polars scan functions because Polars is a columnar, in-memory store. Its internal representation is a set of contiguous column arrays. Building those arrays requires all values for a given column to be known upfront so the buffer can be allocated and filled. A Python generator feeds rows one at a time, which is at odds with that layout. The only path in from a generator is to collect all rows into a Python list first, which means the full dataset has to exist in memory.

`pl.scan_csv()` and `pl.scan_ndjson()` give Polars ownership of the read step itself, enabling a lazy plan from disk to output. Predicate pushdown applies date filters before rows are loaded. Projection pushdown avoids loading columns that are dropped before `.collect()` is called.

The Python generator pattern remains valid for row-oriented targets: writing to CSV line by line, inserting into a database row by row, or processing records through a transformation pipeline without accumulating the full set.

### Schema enforcement at the scan boundary

Both scan functions pass an explicit schema derived from `Product.get_schema()` and `Transaction.get_schema()`. Providing the schema at the scan boundary fails early if the file does not match expectations, consistent with the project's general principle of catching errors as close to their source as possible.

---

## Transformation layer (`transforms.py`)

### Two public functions, no coordinator

`transforms.py` exposes two functions: `build_enriched()` and `build_report()`. There is no wrapper that calls both in sequence. The pipeline layer owns that coordination because validation and null row filtering happen between the two calls, and those concerns are outside the scope of this module.

### Left join over inner join in `build_enriched()`

Transactions are joined to products with a left join. Unmatched barcodes (transactions with no corresponding product entry) are kept as rows with null product columns rather than silently dropped. This separates two concerns that are genuinely distinct: the join itself (a transformation) and the decision of what to do with unmatched rows (a data quality concern). The pipeline layer passes the enriched DataFrame (with nulls still present) to the validator, then drops the null rows before calling `build_report()`. An inner join would collapse these two concerns into a single silent drop with no audit trail.

### `build_enriched()` returns a tuple

`build_enriched()` returns `tuple[pl.DataFrame, int]`: the enriched DataFrame and the count of unmatched barcodes. The unmatched count is computed internally as part of the join boundary logging and returned rather than discarded. The pipeline layer needs this count for `RunManifest.stats.unmatched_barcodes` and for computing `rejected_transactions` and `valid_transactions`.

### `ref_price` dropped at the join boundary

`ref_price` exists on `Product` solely to keep synthetic transaction prices internally consistent during generation. It has no analytical meaning. Dropping it immediately after the join in `build_enriched()` ensures it never appears in any downstream output and does not need to be excluded report by report.

### Collect boundary at `build_enriched()`

Both scan functions in `readers.py` return `pl.LazyFrame`. `build_enriched()` collects the result into a `DataFrame` after the join. This is the single collect boundary for the pipeline. Everything upstream (scan, join, drop) executes as a lazy plan with predicate and projection pushdown. Everything downstream (`build_report()`) works on an already-materialized DataFrame, which is appropriate since the full enriched dataset is needed for every report.

### `_GRAIN_MAP` at module level

The grain-to-expression mapping is a static constant with no dependency on runtime state. Defining it at module level avoids reconstructing the dict on every `build_report()` call and makes it easier to extend when a new `TimeGrain` variant is needed. A `ValueError` is raised if `time_grain` is set but not present in `_GRAIN_MAP`.

### `period` is always a string

All four time grain branches produce a string `"period"` column. The `"day"` branch casts `Date` to `String` explicitly rather than leaving it as a date type. This keeps the column dtype consistent across all grains so downstream code, including CSV writers and any future callers, never needs to branch on the period dtype.

### `partition_cols` vs `report.partition_by`

`report.partition_by` is config state owned by the `Report` object. `build_report()` builds a local `partition_cols` list and prepends `"period"` to it when `time_grain` is set, without mutating `report.partition_by`. Mutating config state inside a transformation function would cause incorrect behavior if the same `Report` object were used across multiple calls.

---

## Data validation (`validators.py`)

### One function, three checks

`validators.py` exposes one public function: `validate_enriched()`. It receives the enriched DataFrame and `QualityConfig` thresholds. It returns a `QualityChecks` dataclass with three booleans.

The three checks are:

1. **Null rate:** the count of rows with null product attributes must not exceed `max_null_pct * total_rows`.
2. **Minimum transactions:** the total row count must be at least `min_transactions`.
3. **Price range:** every `unit_price` must fall within `[min_price, max_price]`.

All three are bulk Polars operations. No row-by-row Python iteration.

### Does not raise on failure

The validator reports results; it does not decide what to do about them. A failing check sets the corresponding boolean to `False` on `QualityChecks`, and the pipeline layer decides whether to mark the run as `FAILURE` or continue. This keeps the validator's responsibility narrow.

### Computes its own null count

`build_enriched()` also computes the unmatched barcode count for `RunManifest`, but the validator does not accept it as an argument. It computes its own null count independently. This is consistent with the project's defensive design: the validator's job is to verify data quality, so it should not trust values computed by another function.

### Logging approach

Computed values (thresholds, actual counts) are logged at `DEBUG` level. Pass/fail results for each check are logged at `INFO` level. The validator does not log at `WARNING` or `ERROR` because it does not consider failures fatal. The pipeline layer owns that judgment.
