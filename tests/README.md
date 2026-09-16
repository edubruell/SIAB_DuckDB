# tests — how the R preparation is checked against the Stata reference

Run everything from the project root:

```
Rscript tests/testthat.R
```

That takes a few seconds, needs no Stata installation and no FDZ test data, and
is safe to run on any clone of the repo.

## The two kinds of test

**Synthetic tests** build a handful of rows in an in-memory DuckDB, run one step
function over them, and check the result against values worked out by hand.
They cover the helpers in `functions/00_common_functions.R`, the lookup tables in
`classifications/`, and the behaviour of each step at its edges: a missing code,
a year the statutory table does not cover, a spell that runs over a year
boundary. These are the fast half and they always run.

**Reference tests**, in `test-reference-*.R`, compare the R pipeline's output
against the Stata preparation row by row, on the FDZ test data. They are named
after the reference do-file, so `test-reference-08_wages_deflation.R` checks the
R port of `08_wages_deflation.do`. Each one needs two parquet files, and skips
with an explanation when either is missing.

`test-reference-unported.R` holds one skipped test per reference step that has
no R counterpart yet. The skip reasons are the to-do list, printed on every run.

## Producing the two halves of a reference comparison

The Stata half is committed, under `tests/testthat/fixtures/`, about 25 MB in
total. Regenerate it only when the reference or the test data changes:

```
/Applications/Stata/StataMP.app/Contents/MacOS/stata-mp -b do tests/fixtures/make_fixtures.do
Rscript tests/fixtures/make_fixtures.R
```

The first command runs the unmodified preparation from
`local_context/stata_reference/origin_EastGermanWageStructure/` over the test
data and saves the whole dataset after each step into
`local_context/stata_fixtures/dump/`. Stata exits 0 even when a do-file errors,
so read `local_context/stata_fixtures/log/make_fixtures.log` afterwards. The
second command cuts each dump down to the key plus the columns that step writes
and saves it as parquet.

The R half is not committed, because it is regenerated from the test database in
about half a minute:

```
Rscript tests/fixtures/make_r_dumps.R
```

Both halves are keyed on `persnr`, `spell` and `begepi`. The spell counter alone
stops being unique at step 01, which cuts a spell running over a year boundary
into one row per calendar year.

## Comparing exactly, and when not to

Exact comparison is the default. Integers, dates, recodes and lookups are
compared bit for bit with no tolerance.

Two things get a stated tolerance, and both are named in the test that uses
them. The reference generates its variables with plain `gen`, which gives a
Stata float: four bytes, about seven decimal digits. Where the R port reads the
same figure from `classifications/`, it rounds to that precision on read-in with
`stata_float()`, and the comparison is then exact. Where the reference stores a
computed result as a float and the R port keeps a double, as with the three
deflated wage variables, the test uses a relative tolerance of 1e-6.

The wage imputation will need a third kind of tolerance when it is compared,
because Stata's `intreg` and R's `survival::survreg` are different
implementations. Nothing compares it yet.

## Adding a test for a newly ported step

1. Add the step's key and its new columns to the `touched` list in both
   `tests/fixtures/make_fixtures.R` and `tests/fixtures/make_r_dumps.R`, under
   the name of the reference do-file.
2. Regenerate both halves.
3. Write `tests/testthat/test-reference-NN_<step>.R` using
   `siab_reference_query()` and `siab_column_diff()` from
   `tests/testthat/helper-siab.R`.
4. Delete the matching skip from `test-reference-unported.R`.
