"""
07.) Imputation of right-censored wages

Port of 10_wages_imputation.do.

In the original Dauth and Eppelsheimer code a two-step imputation procedure
similar to Dustmann et al. (2009) and Card et al. (2013) is used: the first step
imputes on observables following Gartner (2005), the second adds leave-one-out
mean wages per worker and per plant.

Generates the variables:
  - cens:     1 if the wage is right-censored and therefore imputed, 0 otherwise
              (4 EUR below the assessment ceiling)
  - wage:     daily wage, not imputed, top-coded wages replaced by the
              assessment ceiling (-4 EUR), deflated to 2015
  - wage_imp: imputed daily wage, deflated to 2015

The censored normal fit
-----------------------
Stata fits the model with `intreg` and the R arm with `survival::survreg`. The
two agree to ten digits, so there is one number to hit. The Python scientific
stack has no equivalent: `statsmodels` 0.15 ships no censored linear model, its
Tobit work sits on an unmerged branch, and `lifelines`' `LogNormalAFTFitter`
misses `survreg` by about a thousandth relative, runs twenty-five times slower
and pulls a plotting stack into a preparation pipeline. So the likelihood is
written out here and maximised with `scipy.optimize`. Measured against
`survreg` on a synthetic cell of 4,000 rows with 26 percent censoring, this
reaches the same log-likelihood and the same coefficients to 1.2e-9 relative,
in about 11 milliseconds per fit.

`scipy` is needed either way: the draw below wants the normal distribution
function and its inverse, and numpy carries neither.

Where this departs from the reference
-------------------------------------
Two cases, neither of which the FDZ test data reaches, both settled with the
user on 2026-09-17:

  * **A cell that cannot be fitted contributes its uncensored wages and no
    draws.** The reference wraps every estimation command in `capture noisily`,
    so a cell too thin to fit leaves the previous cell's `e()` in place and
    imputes from a model of different data. The R arm refuses to copy that and
    so does this one.
  * **Collinear regressors are dropped and the prediction comes from the
    reduced model**, which is what Stata does: it omits the aliased terms and
    predicts a number for every row. R's `survreg` fits the same reduced model
    but `predict()` then returns `NA` for the whole cell, so no censored wage in
    it would be imputed at all. This side follows Stata.

The reference's own formula for the draw is kept, including its behaviour in
the far tail, where `invnorm()` of a probability that rounds to one returns
missing and the imputed wage falls through to the first step's value. That
fallback is why the reference has its `replace wage_imp = wage_imp_int if
missing(wage_imp)` line.

Unlike the R arm, each cell is sorted before it draws, on `persnr` and `spell`,
which is the order the reference sorts the whole dataset into before it seeds,
and then on `begepi`, which this port adds because the reference's two keys do
not name a row: episode splitting turns one spell into several episodes. With
the full key the draw a row gets is settled by the data, so a seeded run of
this step reproduces no matter what order the step is handed. The R arm's does
not reproduce at all.

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Wolfgang Dauth and
Johann Eppelsheimer, following Gartner (2005)

Version: 1.0
Created: 2026-09-17

References:
  Gartner, H. (2005). The imputation of wages above the contribution limit with
    the German IAB employment sample. FDZ-Methodenreport 02/2005.
  Dustmann, C., J. Ludsteck and U. Schönberg (2009). Revisiting the German wage
    structure. The Quarterly Journal of Economics 124 (2), 843-881.
  Card, D., J. Heining and P. Kline (2013). Workplace heterogeneity and the rise
    of West German wage inequality. The Quarterly Journal of Economics 128 (3),
    967-1015.
  Drechsler, J., J. Ludsteck and A. Moczall (2023). Imputation der
    rechtszensierten Tagesentgelte für die BeH. FDZ-Methodenreport 05/2023.
"""

from __future__ import annotations

import atexit
import os
from pathlib import Path
from typing import NamedTuple, Sequence

import numpy as np
import polars as pl
from scipy.optimize import minimize
from scipy.special import log_ndtr, ndtr, ndtri
from scipy.stats import norm

from siab.common import open_store, pl_stata_gt, spill_dir, step_logger

__all__ = ["impute_wages"]


# 10_wages_imputation.do:
#   global controls frau teilzeit old age age_sq age_old age_sq_old tage_job tenure_sq
CONTROLS = ["frau", "teilzeit", "old", "age", "age_sq",
            "age_old", "age_sq_old", "tage_job", "tenure_sq"]

# What the second step adds to them.
LEAVE_ONE_OUT = ["ln_wage_mean_worker", "only_one_obs",
                 "ln_wage_mean_firm", "only_one_worker"]

# The same list 10_wages_imputation.do drops, so the step leaves behind the
# three variables its header names: cens, wage and wage_imp.
CLEANUP = ["educ_tmp", "old", "age_sq", "age_old", "age_sq_old", "tenure_sq",
           "ln_wage_mean_worker", "only_one_obs", "ln_wage_mean_firm",
           "only_one_worker", "limit_assess4", "ln_limit_assess4", "ln_wage",
           "ln_wage_cens", "wage_imp_int", "ln_wage_imp", "ln_wage_imp2"]


# ======================================================================
#  The censored normal regression
# ======================================================================

class CensoredNormalFit(NamedTuple):
    """What one `intreg` call leaves behind, in the parts the step uses.

    `kept` indexes the design's columns that survived the collinearity check,
    and `coefficients` is as long as `kept`, so a prediction is the product of
    the two. `scale` is Stata's `e(sigma)`.
    """

    kept: np.ndarray
    coefficients: np.ndarray
    scale: float
    n_observations: int


def _independent_columns(design: np.ndarray, tolerance: float = 1e-7) -> np.ndarray:
    """Index the design's columns that are not collinear with earlier ones.

    Stata's `_rmcoll` keeps the first member of a collinear set and omits the
    rest, so this walks the columns in order and accepts one when the part of it
    orthogonal to everything already accepted is a large enough share of its own
    length. A pivoted QR would be shorter but reorders the columns by norm,
    which can drop a different member of the set than Stata drops.
    """
    kept: list[int] = []
    basis: list[np.ndarray] = []

    for column in range(design.shape[1]):
        residual = design[:, column].astype(float).copy()
        length = np.linalg.norm(residual)
        for direction in basis:
            residual -= (direction @ residual) * direction
        if length == 0.0 or np.linalg.norm(residual) <= tolerance * length:
            continue
        basis.append(residual / np.linalg.norm(residual))
        kept.append(column)

    return np.array(kept, dtype=int)


def fit_censored_normal(design: np.ndarray,
                        response: np.ndarray,
                        censored: np.ndarray) -> CensoredNormalFit | None:
    """Maximum likelihood fit of a right-censored normal, as `intreg` does it.

    An uncensored row contributes the normal density at its observed value and a
    censored one the probability of lying above it, which is the interval
    regression of `intreg ln_wage ln_wage_cens` where the upper bound is missing
    exactly on the censored rows.

    The columns are scaled to unit root-mean-square before the optimiser sees
    them and the coefficients are scaled back afterwards. The prep's regressors
    run from a zero/one dummy to a squared tenure in the hundred-thousands, and
    without that step the line search stalls short of the optimum on a problem
    this badly conditioned.

    Hands back `None` when the cell cannot be fitted, which the caller reports
    and treats as a cell that contributes its uncensored wages and no draws.
    """
    if response.size == 0:
        return None

    kept = _independent_columns(design)
    if kept.size == 0 or response.size <= kept.size:
        return None

    retained = design[:, kept].astype(float)
    scale = np.sqrt((retained ** 2).mean(axis=0))
    scale[scale == 0.0] = 1.0
    scaled = retained / scale

    uncensored = ~censored
    if not uncensored.any():
        return None

    # survreg starts from an ordinary least squares fit on the uncensored rows,
    # and so does this.
    start_coefficients, *_ = np.linalg.lstsq(scaled[uncensored],
                                             response[uncensored], rcond=None)
    residual_sd = np.std(response[uncensored] - scaled[uncensored] @ start_coefficients)
    start = np.append(start_coefficients, np.log(max(residual_sd, 1e-8)))

    def negative_log_likelihood(theta: np.ndarray) -> float:
        coefficients, log_sigma = theta[:-1], theta[-1]
        sigma = np.exp(log_sigma)
        standardised = (response - scaled @ coefficients) / sigma
        contribution = np.where(
            censored,
            log_ndtr(-standardised),
            -0.5 * standardised ** 2 - 0.5 * np.log(2 * np.pi) - log_sigma,
        )
        return -float(contribution.sum())

    def score(theta: np.ndarray) -> np.ndarray:
        coefficients, log_sigma = theta[:-1], theta[-1]
        sigma = np.exp(log_sigma)
        standardised = (response - scaled @ coefficients) / sigma
        # The inverse Mills ratio, through logs so the far tail stays finite.
        mills = np.exp(norm.logpdf(standardised) - log_ndtr(-standardised))
        derivative = np.where(censored, mills, standardised)
        return np.append(-(derivative / sigma) @ scaled,
                         -(derivative * standardised).sum() + uncensored.sum())

    result = minimize(negative_log_likelihood, start, jac=score, method="BFGS",
                      options=dict(gtol=1e-8, maxiter=500))

    # `result.success` is not the test. BFGS reports a precision loss whenever
    # its line search runs into machine epsilon at the optimum, which on this
    # likelihood is the normal way to arrive. The score is the test, and at a
    # real optimum it sits eleven orders of magnitude below the threshold here.
    if not np.all(np.isfinite(result.x)):
        return None
    if np.abs(result.jac).max() > 1e-6 * response.size:
        return None

    return CensoredNormalFit(
        kept=kept,
        coefficients=result.x[:-1] / scale,
        scale=float(np.exp(result.x[-1])),
        n_observations=int(response.size),
    )


def predict_censored_normal(design: np.ndarray, fit: CensoredNormalFit) -> np.ndarray:
    """`predict xbn, xb` over the columns the fit kept."""
    return design[:, fit.kept] @ fit.coefficients


def draw_above_the_ceiling(expected: np.ndarray,
                           sigma: float,
                           ceiling: np.ndarray,
                           uniform: np.ndarray) -> np.ndarray:
    """The reference's draw from the normal truncated at the assessment ceiling.

        gen eta = (ln_limit_assess4 - xbn) / $sdi
        xbn + $sdi * invnorm(normal(eta) + uniform()*(1-normal(eta)))

    written out unchanged, including its behaviour far out in the tail: once
    `normal(eta)` rounds to one the argument of `invnorm` does too, Stata
    returns missing and the reference's own fallback carries the first step's
    wage over. `ndtri` returns an infinity there instead of a missing, so the
    non-finite draws are turned into NaN, which is what a missing is here.
    """
    eta = (ceiling - expected) / sigma
    below = ndtr(eta)
    drawn = expected + sigma * ndtri(below + uniform * (1.0 - below))
    return np.where(np.isfinite(drawn), drawn, np.nan)


# ======================================================================
#  The database this step works in
# ======================================================================
#
# Three things this step does are what a database is for and none of them is a
# polars expression the streaming engine can run: the whole dataset is sorted
# once, a maximum likelihood fit reads one year/education/east cell at a time,
# and the leave-one-out means are window functions over every row. Written to a
# file, those windows peak higher than collecting the frame does, measured on
# 2026-09-21 at 4.51 GB against 3.57 on a stand-in of five million rows.
#
# So the step loads the prepared frame into a DuckDB database of its own and
# hands back a scan of what it writes out. DuckDB sorts, filters and windows
# out of core, under `SIAB_DUCKDB_MEMORY_LIMIT` where a run sets one, which is
# what the R arm has always done and why the R arm survives a 4 GB cap where
# this one used to hold about 14 GB at ten copies of the test delivery.
#
# The round trip costs no types: every store this arm runs over already writes
# each step's result into a DuckDB table or a Parquet file between steps.

_ORDERED = ("ORDER BY persnr, spell, begepi "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING")

_cleaned_up: set[Path] = set()


def _step_file(suffix: str) -> Path:
    """A working file of this step's own, in the folder the arm spills to.

    The name carries the process, so two runs in one folder do not meet, and it
    is the same on every call, so a run leaves one file per suffix behind
    rather than one per step. Whatever is there from an earlier call goes now,
    and what this call writes goes when the process ends.
    """
    path = spill_dir() / f"siab_imputation_{os.getpid()}{suffix}"
    path.unlink(missing_ok=True)
    if path not in _cleaned_up:
        atexit.register(path.unlink, missing_ok=True)
        _cleaned_up.add(path)
    return path


def _columns(store) -> list[str]:
    return [row[1] for row in store.execute("PRAGMA table_info('work')").fetchall()]


def _overwrite_work(store, query: str) -> None:
    """Rebuild `work` from a query over itself, to a new table and then a rename.

    The same shape as the R arm's `compute_and_overwrite()`, and kept explicit
    for the same reason: what a statement that reads and replaces one table in
    one breath does is a property of the engine rather than of this code.
    """
    store.execute("DROP TABLE IF EXISTS work_next")
    store.execute(f"CREATE TABLE work_next AS {query}")
    store.execute("DROP TABLE work")
    store.execute("ALTER TABLE work_next RENAME TO work")


def _load_work(store, work: pl.LazyFrame) -> None:
    """Put the prepared frame into the step's database, numbered in the sort order.

    The sort is what makes the step reproducible. The second step's regressors
    are leave-one-out mean wages, which sum over a person and over a plant in
    whatever order the rows arrive in, and floating-point addition is not
    associative: the same data in a different order gives a regressor that
    differs in its last bits, a fit that differs in its last bits, and a draw
    that usually differs by about 6e-8 but can move by whole euros where the
    inverse normal is steep. `persnr` and `spell` are the reference's own sort;
    `begepi` is this port's addition, because episode splitting means the first
    two do not name a row.

    `_row` is that order as a column. Every cell the loop below reads carries
    it, which is how a cell's draws find their rows again, and the step writes
    its result out in it, so what the next step is handed does not depend on
    how the join that put the draws back happened to come out.
    """
    handover = _step_file("_in.parquet")
    work.sink_parquet(handover, compression="zstd")
    store.execute(
        "CREATE TABLE work AS SELECT *, "
        "row_number() OVER (ORDER BY persnr, spell, begepi) - 1 AS _row "
        f"FROM read_parquet('{handover}')")
    handover.unlink(missing_ok=True)


# ======================================================================
#  One cell of one imputation step
# ======================================================================

def _impute_cell(cell: pl.DataFrame,
                 regressors: Sequence[str],
                 generator: np.random.Generator) -> tuple[np.ndarray, bool, str | None]:
    """The body of both of the reference's loops, over one year/education/east cell.

    The two loops differ only in their regressors and in the column they write,
    so `regressors` carries the four leave-one-out terms the second step adds.
    The structure follows 10_wages_imputation.do closely enough to be read
    against it:

        intreg ln_wage ln_wage_cens $controls if marginal == 0
        predict xbn if e(sample), xb
        gen eta = (ln_limit_assess4 - xbn) / $sdi if e(sample)
        gen     ln_wage_tmp = ln_wage if cens == 0
        replace ln_wage_tmp = xbn + $sdi * invnorm(normal(eta) + uniform()*(1-normal(eta))) ///
                if e(sample) & cens == 1

    Two things in that block are easy to get wrong. The estimation sample is the
    cell minus marginal employment minus any row with a missing regressor, but
    the line that carries an uncensored wage through is NOT restricted to it: a
    marginal spell, or one with a missing control, still keeps its own log wage.
    And the draw is taken only for censored rows inside the estimation sample.

    Hands back the imputed log wage for every row of the cell in the cell's own
    order, whether the cell could be fitted, and a message when something is
    worth logging. A missing value travels as NaN here, because the whole cell
    is numpy; `_run_imputation_step()` turns it back into a null.
    """
    log_wage = cell["ln_wage"].to_numpy()
    censored = cell["cens"].to_numpy() == 1

    # gen ln_wage_tmp = ln_wage if cens == 0, over the whole cell
    imputed = np.where(censored, np.nan, log_wage)

    # e(sample): `if marginal == 0` excludes a missing marginal flag, and Stata
    # drops a row with a missing dependent or regressor from the estimation on
    # top of that.
    in_sample = (cell["marginal"] == 0).fill_null(False).to_numpy() & ~cell["ln_wage"].is_null().to_numpy()
    for column in regressors:
        in_sample = in_sample & ~cell[column].is_null().to_numpy()

    estimation = cell.filter(pl.Series(in_sample))
    design = np.column_stack(
        [np.ones(estimation.height)]
        + [estimation[column].to_numpy().astype(float) for column in regressors]
    )
    fit = fit_censored_normal(design,
                              estimation["ln_wage"].to_numpy().astype(float),
                              estimation["cens"].to_numpy() == 1)

    if fit is None:
        return imputed, False, (f"no fit on {estimation.height} rows in the "
                                f"estimation sample; the censored wages in this "
                                f"cell stay unimputed")

    expected = predict_censored_normal(design, fit)
    take = (estimation["cens"].to_numpy() == 1)
    drawn = np.full(estimation.height, np.nan)
    drawn[take] = draw_above_the_ceiling(
        expected[take],
        fit.scale,
        estimation["ln_limit_assess4"].to_numpy().astype(float)[take],
        generator.random(int(take.sum())),
    )

    # replace ... if e(sample) & cens == 1
    imputed[np.flatnonzero(in_sample)[take]] = drawn[take]

    below = int(np.sum(drawn[take] < estimation["ln_wage"].to_numpy()[take]))
    overflowed = int(np.sum(np.isnan(drawn[take])))
    notes = []
    if below > 0:
        notes.append(f"{below} imputed wage(s) below the censoring limit")
    if overflowed > 0:
        notes.append(f"{overflowed} draw(s) overflowed in the tail and fall back "
                     f"on the first step")
    return imputed, True, ("; ".join(notes) if notes else None)


def _cells(store, regressors: Sequence[str], batch: int = 200_000):
    """Hand out one year/education/east cell at a time, in the reference's order.

    The database sorts the table into cell order once, on the three keys of the
    plan and then on the dataset's own, and the rows arrive as a stream of
    batches. A cell's rows are therefore next to each other, so this holds one
    cell and one batch at a time and never the dataset.

    A query per cell would be the obvious way to write it and was the first one:
    every cell is a filter the database can answer. It scans the whole table to
    do so, and with the test delivery's 294 cells that cost 17 seconds against
    the 14 of one ordered pass. One pass also keeps the count of scans off the
    delivery's size.
    """
    # Only the columns a cell is fitted, sorted and scattered back by, which is
    # twenty of the forty-odd the table carries by this point in the pipeline.
    columns = ", ".join(dict.fromkeys(
        ["_row", "persnr", "spell", "begepi", "marginal", "cens", "ln_wage",
         "ln_limit_assess4", "year", "educ_tmp", "east", *regressors]))

    stream = store.execute(
        f"SELECT {columns} FROM work WHERE east IS NOT NULL "
        f"ORDER BY year, educ_tmp, east, persnr, spell, begepi"
    ).to_arrow_reader(batch)

    key = None
    pending: list[pl.DataFrame] = []
    for arrow in stream:
        frame = pl.from_arrow(arrow)
        for part_key, part in frame.partition_by(
                ["year", "educ_tmp", "east"], as_dict=True, maintain_order=True).items():
            if key is not None and part_key != key:
                yield key, pl.concat(pending)
                pending = []
            key, pending = part_key, pending + [part]

    if pending:
        yield key, pl.concat(pending)


def _run_imputation_step(store,
                         regressors: Sequence[str],
                         target: str,
                         carry_missing_east: bool,
                         generator: np.random.Generator,
                         log) -> None:
    """One whole imputation step: every cell of the plan, plus the rows it never sees.

        keep if missing(east)
        replace ln_wage_imp = ln_wage if quelle==1

    is how the reference treats a spell with no East/West information: it is set
    aside before the loop and carries its own log wage. Only the first step does
    that; the second leaves the column missing there and picks the value back up
    from the fallback at the end.

    The reference loops year, then education group, then East/West, and runs a
    cell even when it holds nothing. The plan is the data's own distinct keys
    instead, so an empty cell is never visited, but the order is the
    reference's, and the draws follow it.
    """
    # One draw per row of the dataset, eight bytes each, and the only thing
    # here that grows with the delivery. A cell writes its own rows and the
    # join at the end puts the column back on the table.
    imputed = np.full(store.sql("SELECT count(*) FROM work").fetchone()[0], np.nan)

    cells = unfitted = 0
    for (year, educ, east), cell in _cells(store, regressors):
        values, fitted, message = _impute_cell(cell, regressors, generator)
        imputed[cell["_row"].to_numpy()] = values
        cells += 1
        unfitted += not fitted
        if message is not None:
            log.warning(f" ->  year {year}, education group {educ}, east {east}: "
                        f"{message}")

    if unfitted > 0:
        log.warning(f" ->  {unfitted} of {cells} cells could not be fitted")

    # NaN is how a missing value travelled through the cells; the table wants a
    # null, and the fallback at the end of the step reads null.
    drawn = pl.DataFrame({
        "_row": np.arange(imputed.size, dtype=np.int64),
        "value": imputed,
    }).with_columns(pl.col("value").fill_nan(None))

    carried = ("WHEN w.quelle = 1 THEN w.ln_wage " if carry_missing_east else "")
    store.register("_drawn", drawn)
    _overwrite_work(
        store,
        f"SELECT w.*, CASE WHEN w.east IS NOT NULL THEN d.value {carried}END "
        f"            AS {target} "
        f"FROM work w JOIN _drawn d ON w._row = d._row")
    store.unregister("_drawn")


# ======================================================================
#  The censoring overview the reference tabulates
# ======================================================================

def _percent(share: float | None) -> str:
    return "n/a" if share is None else f"{share:.1%}"


def _log_censoring_overview(store, log) -> None:
    """The `tab cens ...` blocks of 10_wages_imputation.do, as log lines.

    The reference prints these only under its `inspect` global. They are kept
    because the R arm keeps them, and because the share of censored wages per
    education group is the first thing that looks wrong when the assessment
    ceiling or the deflation is off.

    Every line is a count or a group mean, so the database answers them one
    small frame at a time and nothing here holds a column of the dataset.
    """
    beh = "FROM work WHERE quelle = 1 AND cens IS NOT NULL"

    counts = store.sql(f"SELECT cens, count(*) AS n {beh} "
                       f"GROUP BY cens ORDER BY cens").pl()
    total = counts["n"].sum()
    labels = {0: "below the wage assessment limit", 1: "above the wage assessment limit"}
    for censored, n in counts.iter_rows():
        log.info(f"{n} BeH wages ({_percent(n / total)}) are {labels[censored]}")
    log.info("--------------------")

    education = {0: "Missing", 1: "Low", 2: "Medium", 3: "High"}
    by_education = store.sql(f"SELECT coalesce(educ, 0) AS educ, avg(cens) AS share "
                             f"{beh} GROUP BY 1 ORDER BY 1").pl()
    for educ, share in by_education.iter_rows():
        log.info(f"{_percent(share)} of wages are censored for "
                 f"{education.get(educ, educ)} Education")
    log.info("--------------------")

    # The reference reports the high-skilled in five-year age bands, because
    # theirs is the group most of the censoring sits in. Each band is closed on
    # the left and open on the right, and the last one takes the sixtieth
    # birthday with it.
    bands = [(18, 25)] + [(lower, lower + 5) for lower in range(25, 60, 5)]
    for lower, upper in bands:
        closed = "age <= ?" if upper == 60 else "age < ?"
        share = store.execute(f"SELECT avg(cens) {beh} AND educ = 3 "
                              f"AND age >= ? AND {closed}", [lower, upper]).fetchone()[0]
        log.info(f"For the highly educated in age range {lower} to {upper} "
                 f"{_percent(share)} of wages are censored")
    log.info("--------------------")

    hours = {0: "Fulltime", 1: "Parttime", 9: "Missing FT-Info"}
    by_hours = store.sql(f"SELECT coalesce(teilzeit, 9) AS teilzeit, avg(cens) AS share "
                         f"{beh} GROUP BY 1 ORDER BY 1").pl()
    for teilzeit, share in by_hours.iter_rows():
        log.info(f"{_percent(share)} of wages are censored for "
                 f"{hours.get(teilzeit, teilzeit)} employees")
    log.info("--------------------")

    sex = {0: "Men", 1: "Women"}
    by_sex = store.sql(f"SELECT frau, avg(cens) AS share {beh} "
                       f"GROUP BY frau ORDER BY frau").pl()
    for frau, share in by_sex.iter_rows():
        log.info(f"{_percent(share)} of wages of {sex.get(frau, frau)} are censored")
    log.info("--------------------")


# ======================================================================
#  The step
# ======================================================================

def impute_wages(frame: pl.LazyFrame,
                 log_file: str | os.PathLike | None = None,
                 seed: int | None = None) -> pl.LazyFrame:
    """Impute the right-censored wages, both steps of the reference's procedure.

    `seed` is the one argument the R arm has no counterpart for. R's generator
    is global and `tests/fixtures/make_r_dumps.R` seeds it around the call;
    numpy's is an object, so it is made here. Left at `None` the draws differ
    from run to run, which is what the reference does outside its own
    `set seed 123`.

    This is the one step that works in a database of its own rather than in a
    polars plan: a likelihood is maximised per cell, in numpy, and there is no
    lazy expression for that, nor for a leave-one-out mean the streaming engine
    will run. What it holds is one cell at a time. See the block above
    `_step_file()`.

    What comes back is a scan of a file in the spill folder, and a second call
    in the same process writes over that file, so collect the frame before
    calling the step again. The pipeline does, and so does every test here.
    """
    log = step_logger("impute_wages", log_file)
    generator = np.random.default_rng(seed)

    # ------------------------------------------------------------------
    #   Modify assessment limit and flag censored wages
    # ------------------------------------------------------------------
    log.info("Modify assessment limit and flag censored wages")

    # Subtract 4 EUR from the exact assessment limit, to make sure all censored
    # wages are covered by the imputation.
    #
    # The reference generates the flag as 0 for every row and only ever raises
    # it for BeH spells:
    #   gen cens = 0
    #   replace cens = 1 if wage_defl > limit_assess4 & quelle == 1
    # so a spell off the employment history is flagged uncensored rather than
    # missing, and so is a spell whose assessment ceiling is unknown. The second
    # case is every BeH spell from 1992 on with a missing east flag: the ceiling
    # differs between East and West, so limit_assess_defl is missing there, and
    # Stata's ordering of missing makes the comparison false rather than
    # missing. pl_stata_gt() carries that ordering through.
    work = frame.with_columns(
        limit_assess4=pl.col("limit_assess_defl") - (100 * 4 / pl.col("cpi")),
    ).with_columns(
        ln_limit_assess4=pl.col("limit_assess4").log(),
        cens=((pl.col("quelle") == 1)
              & pl_stata_gt(pl.col("wage_defl"), pl.col("limit_assess4"))
              ).cast(pl.Int32),
    )

    # ------------------------------------------------------------------
    #   Prepare the dependent variable and the controls
    # ------------------------------------------------------------------
    log.info("Preparing the dependent variable and the controls for imputation")

    #   gen     wage = wage_defl     if quelle == 1
    #   replace wage = limit_assess4 if quelle == 1 & wage_defl > limit_assess4
    # Same ordering of missing as cens above, so a spell with an unknown ceiling
    # keeps its raw deflated wage instead of going missing.
    work = work.with_columns(
        wage=pl.when(pl.col("quelle") != 1).then(None)
               .when(pl_stata_gt(pl.col("wage_defl"), pl.col("limit_assess4")))
               .then(pl.col("limit_assess4"))
               .otherwise(pl.col("wage_defl")),
    ).with_columns(
        ln_wage=pl.when(pl.col("wage") != 0).then(pl.col("wage").log()).otherwise(None),
    ).with_columns(
        # Carried because the reference's `intreg ln_wage ln_wage_cens` reads it
        # as the upper bound of the interval. The fit here takes the censoring
        # from `cens` instead, which is the same statement about the same rows.
        ln_wage_cens=pl.when(pl.col("cens") == 0).then(pl.col("ln_wage")).otherwise(None),
        # Age squared in hundreds of years, to keep the coefficients readable;
        # a dummy for older workers, so their age profile can differ; tenure
        # squared on the same scale as age.
        age_sq=(pl.col("age") / 10) ** 2,
        old=(pl.col("age") > 40).cast(pl.Int32),
        tenure_sq=(pl.col("tage_job") / 10) ** 2,
        # Education groups, with missings regarded as low-skilled for the
        # imputation.
        educ_tmp=pl.col("educ").fill_null(1),
    ).with_columns(
        age_old=pl.col("age") * pl.col("old"),
        age_sq_old=pl.col("age_sq") * pl.col("old"),
    )
    # ------------------------------------------------------------------
    #   Into the step's own database
    # ------------------------------------------------------------------
    #
    # Everything above is a lazy plan the streaming engine runs; everything
    # below is the database's, because a maximum likelihood fit per cell is not
    # a polars expression and the leave-one-out means are windows over the
    # whole dataset. See the block above `_step_file()` for why that is the
    # line, and `_load_work()` for what the sort is protecting.
    output = _step_file(".parquet")
    database = _step_file(".duckdb")

    with open_store(database,
                    memory_limit=os.environ.get("SIAB_DUCKDB_MEMORY_LIMIT"),
                    temp_directory=os.environ.get("SIAB_DUCKDB_TEMP_DIR")) as store:
        _load_work(store, work)
        rows = store.sql("SELECT count(*) FROM work").fetchone()[0]
        log.info(f" ->  limit_assess4, ln_limit_assess4, cens, wage, ln_wage and "
                 f"the controls added over {rows} rows")

        _log_censoring_overview(store, log)

        cells, first_year, last_year = store.sql(
            "SELECT (SELECT count(*) FROM (SELECT DISTINCT year, educ_tmp, east "
            "        FROM work WHERE east IS NOT NULL)), min(year), max(year) "
            "FROM work").fetchone()
        log.info(f"Imputation plan: {cells} year/education/east cells, "
                 f"{first_year} to {last_year}")

        # --------------------------------------------------------------
        # Step 1: imputation with observable characteristics (Gartner 2005)
        # --------------------------------------------------------------
        log.info("Step 1: imputation on observables")
        _run_imputation_step(store, CONTROLS, "ln_wage_imp",
                             carry_missing_east=True,
                             generator=generator, log=log)
        log.info(" ->  ln_wage_imp added")

        # --------------------------------------------------------------
        # Intermediate step: leave-one-out means of the imputed wages
        # --------------------------------------------------------------
        #
        # Something like a worker and a plant fixed effect. Three details of
        # the reference decide the values and none of them is visible in the
        # formula:
        #
        #   - `egen total()` sums a group of nothing but missings to 0, which a
        #     SQL sum returns as null instead, so each one is wrapped in a
        #     coalesce, exactly as the R arm wraps it.
        #   - a worker seen once, or a plant with one sampled worker, has an
        #     empty leave-one-out set. The dummy records that BEFORE the gap is
        #     filled.
        #   - the fill is the mean over every row, taken before non-BeH spells
        #     are wiped, and for the plant it is the mean within the year.
        #
        # betnr is a real establishment number in SIAB 7523 v2, so the plant
        # means are computable; in the 2 percent sample most plants hold one
        # sampled worker and the fallback carries them.
        #
        # Every window carries the dataset's sort order and an unbounded frame,
        # so each sum is taken over its whole group in the order `_load_work()`
        # fixed. An unordered sum would be a different number in its last bits
        # on every run.
        log.info("Leave-one-out means of the imputed wages")

        overall_mean = store.sql(
            f"SELECT avg(ln_wage_imp) OVER ({_ORDERED}) FROM work LIMIT 1").fetchone()[0]

        _overwrite_work(store, f"""
            WITH windowed AS (
                SELECT *,
                       count(*) OVER person AS n_person,
                       coalesce(sum(ln_wage_imp) OVER person, 0) AS sum_person,
                       count(*) OVER plant AS n_plant,
                       coalesce(sum(ln_wage_imp) OVER plant, 0) AS sum_plant,
                       avg(ln_wage_imp) OVER years AS year_mean
                FROM work
                WINDOW person AS (PARTITION BY persnr, quelle {_ORDERED}),
                       plant AS (PARTITION BY year, betnr {_ORDERED}),
                       years AS (PARTITION BY year {_ORDERED})
            ), leave_one_out AS (
                SELECT * EXCLUDE (n_person, sum_person, n_plant, sum_plant),
                       CASE WHEN n_person > 1 AND ln_wage_imp IS NOT NULL
                            THEN (sum_person - ln_wage_imp) / (n_person - 1)
                            END AS ln_wage_mean_worker,
                       CASE WHEN n_plant > 1 AND ln_wage_imp IS NOT NULL
                            THEN (sum_plant - ln_wage_imp) / (n_plant - 1)
                            END AS ln_wage_mean_firm
                FROM windowed
            )
            SELECT * EXCLUDE (ln_wage_mean_worker, ln_wage_mean_firm, year_mean),
                   CAST(ln_wage_mean_worker IS NULL AS INTEGER) AS only_one_obs,
                   CAST(ln_wage_mean_firm IS NULL AS INTEGER) AS only_one_worker,
                   CASE WHEN quelle <> 1 THEN NULL
                        ELSE coalesce(ln_wage_mean_worker, {overall_mean!r})
                        END AS ln_wage_mean_worker,
                   CASE WHEN quelle <> 1 THEN NULL
                        ELSE coalesce(ln_wage_mean_firm, year_mean)
                        END AS ln_wage_mean_firm
            FROM leave_one_out""")

        log.info(" ->  ln_wage_mean_worker, only_one_obs, ln_wage_mean_firm and "
                 "only_one_worker added")

        # --------------------------------------------------------------
        # Step 2: extended imputation models including the leave-one-out means
        # --------------------------------------------------------------
        log.info("Step 2: imputation including the leave-one-out means")
        _run_imputation_step(store, list(CONTROLS) + LEAVE_ONE_OUT, "ln_wage_imp2",
                             carry_missing_east=False,
                             generator=generator, log=log)
        log.info(" ->  ln_wage_imp2 added")

        # --------------------------------------------------------------
        #   Imputed wages in levels, and the minor adjustments
        # --------------------------------------------------------------
        #
        # 10_wages_imputation.do takes the 99th percentile of wage_imp, the
        # level, not of its logarithm, and only after the second step has run:
        #   sum wage_imp, d
        #   global maxWage = 10 * r(p99)
        # Ten seems awfully high as a cutoff and two would be more reasonable
        # for excluding weirdly high observations, but ten is what the original
        # code uses. `summarize, detail` and a continuous quantile do not define
        # the 99th percentile the same way, so the bound differs in its last
        # digits; it is ten times a percentile and binds on almost nothing, but
        # it is a reason this column gets a tolerance rather than an exact
        # comparison.
        log.info("Add imputed wages in levels")

        # The bound is read off the column before it is built, because the two
        # are the same numbers and building it first would copy the table for
        # nothing.
        max_wage = store.sql(
            "SELECT quantile_cont(exp(ln_wage_imp2), 0.99) * 10 FROM work").fetchone()[0]
        log.info(f"Imputed wages are bounded at {max_wage:.2f} EUR per day")

        # The coalesce is what a cell that could not be fitted, a spell with no
        # East/West information, or a draw that overflowed in the tail leaves
        # behind: the first step's wage stands in for the second step's. The
        # bound is written as a comparison rather than a minimum, because a
        # minimum over a null and a number is the number, and a spell with no
        # imputed wage has to keep its missing rather than acquire the bound.
        _overwrite_work(store, f"""
            SELECT * EXCLUDE (wage_imp),
                   coalesce(wage_imp, wage_imp_int) AS wage_imp
            FROM (SELECT *,
                         CASE WHEN exp(ln_wage_imp) > {max_wage!r} THEN {max_wage!r}
                              ELSE exp(ln_wage_imp) END AS wage_imp_int,
                         CASE WHEN exp(ln_wage_imp2) > {max_wage!r} THEN {max_wage!r}
                              ELSE exp(ln_wage_imp2) END AS wage_imp
                  FROM work)""")

        log.info(" ->  wage_imp_int and wage_imp added, implausibly high wages "
                 "bounded and the second stage filled from the first")

        # --------------------------------------------------------------
        # Clean up
        # --------------------------------------------------------------
        dropped = [column for column in CLEANUP if column in _columns(store)]
        log.info("The following variables are dropped from the data for cleanup: "
                 + ", ".join(dropped[:-1]) + " and " + dropped[-1])

        store.execute(
            f"COPY (SELECT * EXCLUDE ({', '.join(dropped + ['_row'])}) "
            f"      FROM work ORDER BY _row) "
            f"TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)")

        log.info(" ->  Cleanup finished")

    # The database is the size of the dataset and everything wanted out of it
    # is in `output` by now, so it goes as soon as the connection is closed.
    # The output file itself has to outlive the step, because what comes back
    # is a scan of it; `_step_file()` takes it at the next call and at exit.
    database.unlink(missing_ok=True)
    Path(f"{database}.wal").unlink(missing_ok=True)

    log.info("Wage imputation file finished")
    return pl.scan_parquet(output)
