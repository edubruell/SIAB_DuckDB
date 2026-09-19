# benchmark — the prep at the size of a real delivery

Every scale claim this project makes rests on the FDZ test delivery, which is
577,776 rows: about one one-hundred-and-fortieth of a real SIAB 7523 v2 core
file, and small enough that it never leaves memory. The questions the design was
decided on need a larger delivery than anyone here has. This folder builds one
out of the test delivery and times the prep over it.

Three arms, five configurations:

| configuration | what runs |
|---|---|
| `r-duckdb` | `R/run_testdata.R`: dplyr and dbplyr, DuckDB doing the work |
| `py-duckdb-memory` | `python/main.py` over a DuckDB store, table handed over in memory |
| `py-duckdb-parquet` | the same, handing each table over as a Parquet file |
| `py-parquet` | `python/main.py` over a folder of Parquet tables |
| `stata` | the original prep, through `benchmark/stata/bench_pipeline.do` |

## What a synthetic delivery is

`make_delivery.py` stacks N renumbered copies of the test delivery. Copy `c`
carries

- `persnr` and `betnr` in a range of their own, so N copies give N times the
  persons and N times the establishments;
- every date of a person moved by the same whole number of days, drawn once per
  person in [-15, +15] and clamped to the delivery's first and last day, which
  keeps a spell's four boundaries in order and moves a few spells across a year
  boundary;
- `gebjahr` moved by up to two years, drawn once per person;
- every wage moved by up to two percent, drawn per row and rounded back to the
  two decimals the delivery reports wages on.

The establishment files are replicated the same way, each copy carrying its own
`betnr` range, so the two merges join against a universe that grew with the
person side.

The jitter is what keeps a stacked delivery from compressing to a size no real
delivery has, and it keeps every step doing work at every copy. Two consequences
to know when reading a result:

- **Row counts move a little between copies.** A shifted date can carry a spell
  out of the 1975 to 2023 window, and a shifted wage can cross the assessment
  ceiling. A copy is the test delivery's workload, not its exact output.
- **A stacked delivery still compresses better than a real one.** The
  categorical variables repeat across copies unchanged. Disk figures from this
  fixture are a floor.

Wages stay on a two-decimal grid because the one-time-payment step asserts that
its reallocation never lowers a daily wage. Off the grid that assertion fires:
the step recomputes the wage from spell earnings and rounds to two decimals,
which lands below an unrounded original about half the time.

## Running it

The fixtures are large and belong outside any folder that syncs. `SIAB_BENCH_ROOT`
names where they go, and `--root` overrides it.

Build one delivery and both stores:

```
uv run --project python python benchmark/make_delivery.py --copies 10 \
    --store "$SIAB_BENCH_ROOT/siab_10x.duckdb" \
    --delivery "$SIAB_BENCH_ROOT/delivery_10x"
```

`--source` is the delivery to copy, and falls back to `SIAB_TEST_DATA`.
`--core-dta` writes the core spell file as one Stata file per copy, which the
Stata arm needs and the other two do not.

Sweep several sizes and record what each run cost:

```
uv run --project python python benchmark/run_benchmark.py --copies 1 10 50 143 \
    --configs r-duckdb py-duckdb-memory py-duckdb-parquet py-parquet stata
```

Each run is a subprocess under `/usr/bin/time -l`, with its own clone of the
store, because a run overwrites the table it works in. On APFS that clone costs
nothing until one of the two is written. Fixtures are deleted once a size is
done, which is what makes a sweep fit on one disk; `--keep-fixtures` keeps them.

Two files land in the results folder:

- `runs.csv` — one row per run: size, rows in, rows out, wall clock, peak
  resident memory, store size before and after, and whether it finished.
- `steps.csv` — one row per step, read off the timestamps all three arms write
  into their per-step logs. A step's `seconds` is its own finish minus the
  finish of the step before it. `log_span` is the distance from a log's first
  line to its last, which is the step's own duration in the Python and Stata
  arms and not in the R arm: `R/run_testdata.R` builds the chain as one pipe,
  and R evaluates a pipe's argument only where the function uses it, so every R
  step writes its opening line while the pipe is being built, in reverse order.
  Finish stamps are in pipeline order in all three.

## Sizes

| copies | rows | what it is |
|---|---|---|
| 1 | 577,776 | the FDZ test delivery |
| 10 | 5,777,760 | still comfortably in memory |
| 143 | 82,621,968 | the real SIAB 7523 v2 core file, 82,389,923 rows |

A row of the core file costs 204 bytes in memory, so the real delivery is about
16.8 GB held whole. On a 16 GB machine that size is already the out-of-core
case.

## What the numbers do not cover

- **The R arm runs one step the other two do not.** `R/run_testdata.R` calls the
  annual establishment merge; `python/main.py` and the reference master leave it
  off, because the files behind it are requested separately from the SIAB.
  `steps.csv` is where that difference is read off and subtracted.
- **The Stata chain skips `09_restrictions.do`**, as the fixture generator
  `make_fixtures.do` does. In this reference that step carries one project's own
  sample cut, which takes the test data from 505,050 rows to 83,817. Neither
  arm ports it.
- **Per-step times are to the second.** All three arms stamp their logs to the
  second, which is nothing at the sizes this folder exists for.
- **A written Stata file is about half as large again as the source.**
  pyreadstat's writer has no `byte` or `int` of its own and widens every integer
  to 32 bits. The generator still hands it the narrow type, without which a
  column holding one missing value goes out as a double or a string.
- **The Stata arm is bounded by memory, at generation time and at run time.**
  The per-copy core files are appended by Stata itself, which holds the result
  whole. A delivery Stata cannot append is one it could not have prepared, and
  the harness records that as a failed append rather than treating it as an
  error.
- **The stacked delivery has no AKM blocks.** No test product carries them, and
  neither runner merges them, so the benchmark chain stops where both runners
  stop: at the yearly panel.
