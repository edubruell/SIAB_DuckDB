# tests

The suite checks the R preparation against the Stata reference. Run it from the
project root. It takes a few seconds.

```
Rscript tests/testthat.R
```

The suite needs no Stata installation and no FDZ test data, so it runs on a
fresh clone. Tests that compare against the reference skip themselves with a
printed reason when the data they need is absent.

A single file, interactively:

```
testthat::test_file("tests/testthat/test-06_wages_deflation.R")
```

`tests/testthat/helper-siab.R` is sourced first. It loads the packages the
preparation needs, sources `R/functions/` the way `R/siab_main.R` does, and defines
the helpers that hand a step function a small in-memory DuckDB to work on.

## The two kinds of test

**Synthetic tests** build a handful of rows in memory, run one step function
over them and check the result against values worked out by hand. They cover the
helpers in `R/functions/00_common_functions.R`, the lookup tables in
`classifications/`, and the behaviour of each step at its edges. A missing code,
a year outside the statutory table, a spell that runs over a year boundary.
These are the fast half and they always run.

**Reference tests**, in `test-reference-*.R`, compare the R pipeline against the
Stata preparation row by row on the FDZ test data. They are named after the
reference do-file, so `test-reference-08_wages_deflation.R` checks the R port of
`08_wages_deflation.do`. Each one needs two parquet files and skips when either
is missing.

`test-reference-unported.R` holds one skipped test per reference step that has
no R counterpart yet. The skip reasons are the to-do list, printed on every run.

## The committed fixtures

`tests/testthat/fixtures/` carries the Stata half of each comparison, one
parquet file per reference step, 76 MB in total. A file holds the key plus the
columns that step writes, taken from the unmodified reference preparation run
over the FDZ test data. A step is compared on those columns, not on the ones it
carries through untouched.

Both halves are keyed on `persnr`, `spell` and `begepi`. The spell counter alone
stops being unique at step 01, which cuts a spell running over a year boundary
into one row per calendar year. Three late steps drop part of that key and are
joined on what is left. `15_parallel_episodes.do` keeps one episode per person
and episode start, `16_yearly_panel.do` one per person and year, and
`16_monthly_panel.do` expands to one row per person and calendar month.

Producing a fixture needs Stata 17, the reference do-files and the FDZ test
data, so the scripts that do it are kept outside this repo. Comments here and in
the test files name three of them. `make_fixtures.do` runs the reference
preparation and dumps the dataset after each step, `make_fixtures.R` cuts each
dump down to the key plus the touched columns, and `make_synth_akm.do`
fabricates the two AKM files step 12 wants, which the FDZ supplies separately.
Two patch files go with them, recording the tie-break lines added to
`03_SIAB_bio.do` and `15_parallel_episodes.do`. The reference sorts on a key
that has ties there, and the row order is then arbitrary. Ask if you want them.

## The R half of a comparison

The R half is untracked, because it regenerates from the test database in about
half a minute.

```
Rscript tests/fixtures/make_r_dumps.R
```

This is `R/run_testdata.R` broken open. The same steps in the same order over the
same test database, with a parquet dump of the touched columns after each one.
Write the database first with `R/stata_to_db_batch_read.R`.

Three environment variables set the folders, each with a fallback.
`SIAB_TEST_DB` is the DuckDB file, `SIAB_TEST_DATA` the folder holding the FDZ
test data and `SIAB_R_DUMP` the folder the dumps are written to. The reference
tests read `SIAB_R_DUMP` as well, through `helper-siab.R`, so set it for the
script and the suite together or for neither.

## Comparing exactly, and when not to

Exact comparison is the default. Integers, dates, recodes and lookups are
compared bit for bit with no tolerance.

Two things get a stated tolerance, and both are named in the test that uses
them. The reference generates its variables with plain `gen`, which gives a
Stata float of four bytes and about seven decimal digits. Where the R port reads
the same figure from `classifications/`, it rounds to that precision on read-in
with `stata_float()`, and the comparison is then exact. Where the reference
stores a computed result as a float and the R port keeps a double, as with the
three deflated wage variables, the test uses a relative tolerance of 1e-6.

Three columns are compared as distributions rather than row by row, because each
is built from the imputed wage and the two sides draw their own random terms.
`siab_column_moments()` returns the mean and both quartiles of each half, and
the test bounds the gap between them.

## Adding a test for a newly ported step

1. Add the step's key and its new columns to the `touched` list in
   `tests/fixtures/make_r_dumps.R`, under the name of the reference do-file.
   The Stata side keeps the same list and is extended with it.
2. Regenerate both halves and commit the new Stata fixture.
3. Write `tests/testthat/test-reference-NN_<step>.R` using
   `siab_reference_query()` and `siab_column_diff()` from
   `tests/testthat/helper-siab.R`.
4. Delete the matching skip from `test-reference-unported.R`.
