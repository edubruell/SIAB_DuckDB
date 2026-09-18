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

import os
from typing import NamedTuple, Sequence

import numpy as np
import polars as pl
from scipy.optimize import minimize
from scipy.special import log_ndtr, ndtr, ndtri
from scipy.stats import norm

from siab.common import pl_stata_gt, step_logger

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


def _run_imputation_step(work: pl.DataFrame,
                         regressors: Sequence[str],
                         target: str,
                         carry_missing_east: bool,
                         generator: np.random.Generator,
                         log) -> pl.DataFrame:
    """One whole imputation step: every cell of the plan, plus the rows it never sees.

        keep if missing(east)
        replace ln_wage_imp = ln_wage if quelle==1

    is how the reference treats a spell with no East/West information: it is set
    aside before the loop and carries its own log wage. Only the first step does
    that; the second leaves the column missing there and picks the value back up
    from the fallback at the end.

    The reference loops year, then education group, then East/West, and runs a
    cell even when it holds nothing. The plan is built from the data instead, so
    an empty cell is never visited, but the order is the reference's, and the
    draws follow it.
    """
    imputed = np.full(work.height, np.nan)

    cells = work.filter(pl.col("east").is_not_null()).partition_by(
        ["year", "educ_tmp", "east"], as_dict=True, maintain_order=True)

    unfitted = 0
    for key in sorted(cells):
        # The cell is already in `persnr`, `spell`, `begepi` order, because
        # the whole frame was sorted before it was cut up and `partition_by`
        # keeps that order. Sorting again is cheap and says so out loud: the
        # order a cell is drawn for is the reference's, and it is fixed by the
        # data rather than by whatever order upstream handed the step.
        cell = cells[key].sort("persnr", "spell", "begepi")
        values, fitted, message = _impute_cell(cell, regressors, generator)
        imputed[cell["_row"].to_numpy()] = values
        unfitted += not fitted
        if message is not None:
            year, educ, east = key
            log.warning(f" ->  year {year}, education group {educ}, east {east}: "
                        f"{message}")

    if unfitted > 0:
        log.warning(f" ->  {unfitted} of {len(cells)} cells could not be fitted")

    # NaN is how a missing value travelled through the cells; polars wants a
    # null, and the fallback at the end of the step reads null.
    drawn = pl.Series(target, imputed, dtype=pl.Float64).fill_nan(None)

    if carry_missing_east:
        carried = pl.when(pl.col("quelle") == 1).then(pl.col("ln_wage")).otherwise(None)
    else:
        carried = pl.lit(None, pl.Float64)

    return work.with_columns(
        pl.when(pl.col("east").is_not_null()).then(drawn).otherwise(carried).alias(target)
    )


# ======================================================================
#  The censoring overview the reference tabulates
# ======================================================================

def _percent(share: float | None) -> str:
    return "n/a" if share is None else f"{share:.1%}"


def _log_censoring_overview(work: pl.DataFrame, log) -> None:
    """The `tab cens ...` blocks of 10_wages_imputation.do, as log lines.

    The reference prints these only under its `inspect` global. They are kept
    because the R arm keeps them, and because the share of censored wages per
    education group is the first thing that looks wrong when the assessment
    ceiling or the deflation is off.
    """
    beh = work.filter((pl.col("quelle") == 1) & pl.col("cens").is_not_null())

    counts = beh.group_by("cens").len().sort("cens")
    total = counts["len"].sum()
    labels = {0: "below the wage assessment limit", 1: "above the wage assessment limit"}
    for censored, n in counts.iter_rows():
        log.info(f"{n} BeH wages ({_percent(n / total)}) are {labels[censored]}")
    log.info("--------------------")

    education = {0: "Missing", 1: "Low", 2: "Medium", 3: "High"}
    by_education = (beh.with_columns(pl.col("educ").fill_null(0))
                       .group_by("educ").agg(pl.col("cens").mean().alias("share"))
                       .sort("educ"))
    for educ, share in by_education.iter_rows():
        log.info(f"{_percent(share)} of wages are censored for "
                 f"{education.get(educ, educ)} Education")
    log.info("--------------------")

    # The reference reports the high-skilled in five-year age bands, because
    # theirs is the group most of the censoring sits in. Each band is closed on
    # the left and open on the right, and the last one takes the sixtieth
    # birthday with it.
    skilled = beh.filter(pl.col("educ") == 3)
    bands = [(18, 25)] + [(lower, lower + 5) for lower in range(25, 60, 5)]
    for lower, upper in bands:
        in_band = pl.col("age").is_between(lower, upper, closed="both" if upper == 60
                                           else "left")
        share = skilled.filter(in_band)["cens"].mean()
        log.info(f"For the highly educated in age range {lower} to {upper} "
                 f"{_percent(share)} of wages are censored")
    log.info("--------------------")

    hours = {0: "Fulltime", 1: "Parttime", 9: "Missing FT-Info"}
    by_hours = (beh.with_columns(pl.col("teilzeit").fill_null(9))
                   .group_by("teilzeit").agg(pl.col("cens").mean().alias("share"))
                   .sort("teilzeit"))
    for teilzeit, share in by_hours.iter_rows():
        log.info(f"{_percent(share)} of wages are censored for "
                 f"{hours.get(teilzeit, teilzeit)} employees")
    log.info("--------------------")

    sex = {0: "Men", 1: "Women"}
    by_sex = (beh.group_by("frau").agg(pl.col("cens").mean().alias("share"))
                 .sort("frau"))
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

    This is the one step that collects the data rather than a summary: a
    likelihood is maximised per cell, in numpy, and there is no lazy expression
    for that. The frame is collected once, at the top, and handed back lazy.
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

    # The one collection point in this step. Everything from here to the
    # cleanup is eager, because a maximum likelihood fit per cell is not a
    # polars expression.
    #
    # The sort is what makes the step reproducible, and it has to happen here
    # rather than per cell. The second step's regressors are leave-one-out mean
    # wages, which polars sums over a person and over a plant in whatever order
    # the rows arrive in, and floating-point addition is not associative: the
    # same data in a different order gives a regressor that differs in its last
    # bits, a fit that differs in its last bits, and a draw that usually
    # differs by about 6e-8 but can move by whole euros where the inverse
    # normal is steep. Sorting on the dataset's key fixes the summation order,
    # so a seeded run gives the same wages whatever order upstream handed the
    # step. `persnr` and `spell` are the reference's own sort; `begepi` is this
    # port's addition, because episode splitting means the first two do not
    # name a row.
    work = work.sort("persnr", "spell", "begepi").collect().with_row_index("_row")
    log.info(f" ->  limit_assess4, ln_limit_assess4, cens, wage, ln_wage and "
             f"the controls added over {work.height} rows")

    _log_censoring_overview(work, log)

    plan = (work.filter(pl.col("east").is_not_null())
                .select("year", "educ_tmp", "east").unique())
    log.info(f"Imputation plan: {plan.height} year/education/east cells, "
             f"{work['year'].min()} to {work['year'].max()}")

    # ------------------------------------------------------------------
    # Step 1: imputation with observable characteristics (Gartner 2005)
    # ------------------------------------------------------------------
    log.info("Step 1: imputation on observables")
    work = _run_imputation_step(work, CONTROLS, "ln_wage_imp",
                                carry_missing_east=True,
                                generator=generator, log=log)
    log.info(" ->  ln_wage_imp added")

    # ------------------------------------------------------------------
    # Intermediate step: leave-one-out means of the imputed wages
    # ------------------------------------------------------------------
    #
    # Something like a worker and a plant fixed effect. Three details of the
    # reference decide the values and none of them is visible in the formula:
    #
    #   - `egen total()` sums a group of nothing but missings to 0, which is
    #     also what a polars sum over nothing but nulls gives.
    #   - a worker seen once, or a plant with one sampled worker, has an empty
    #     leave-one-out set. The dummy records that BEFORE the gap is filled.
    #   - the fill is the mean over every row, taken before non-BeH spells are
    #     wiped, and for the plant it is the mean within the year.
    #
    # betnr is a real establishment number in SIAB 7523 v2, so the plant means
    # are computable; in the 2 percent sample most plants hold one sampled
    # worker and the fallback carries them.
    log.info("Leave-one-out means of the imputed wages")

    overall_mean = work["ln_wage_imp"].mean()

    work = work.with_columns(
        ln_wage_mean_worker=pl.when(
            (pl.len().over("persnr", "quelle") > 1) & pl.col("ln_wage_imp").is_not_null()
        ).then(
            (pl.col("ln_wage_imp").sum().over("persnr", "quelle") - pl.col("ln_wage_imp"))
            / (pl.len().over("persnr", "quelle") - 1)
        ).otherwise(None),
        ln_wage_mean_firm=pl.when(
            (pl.len().over("year", "betnr") > 1) & pl.col("ln_wage_imp").is_not_null()
        ).then(
            (pl.col("ln_wage_imp").sum().over("year", "betnr") - pl.col("ln_wage_imp"))
            / (pl.len().over("year", "betnr") - 1)
        ).otherwise(None),
        year_mean=pl.col("ln_wage_imp").mean().over("year"),
    ).with_columns(
        only_one_obs=pl.col("ln_wage_mean_worker").is_null().cast(pl.Int32),
        only_one_worker=pl.col("ln_wage_mean_firm").is_null().cast(pl.Int32),
    ).with_columns(
        ln_wage_mean_worker=pl.when(pl.col("quelle") != 1).then(None)
            .otherwise(pl.col("ln_wage_mean_worker").fill_null(overall_mean)),
        ln_wage_mean_firm=pl.when(pl.col("quelle") != 1).then(None)
            .otherwise(pl.col("ln_wage_mean_firm").fill_null(pl.col("year_mean"))),
    ).drop("year_mean")

    log.info(" ->  ln_wage_mean_worker, only_one_obs, ln_wage_mean_firm and "
             "only_one_worker added")

    # ------------------------------------------------------------------
    # Step 2: extended imputation models including the leave-one-out means
    # ------------------------------------------------------------------
    log.info("Step 2: imputation including the leave-one-out means")
    work = _run_imputation_step(work, list(CONTROLS) + LEAVE_ONE_OUT, "ln_wage_imp2",
                                carry_missing_east=False,
                                generator=generator, log=log)
    log.info(" ->  ln_wage_imp2 added")

    # ------------------------------------------------------------------
    #   Imputed wages in levels, and the minor adjustments
    # ------------------------------------------------------------------
    #
    # 10_wages_imputation.do takes the 99th percentile of wage_imp, the level,
    # not of its logarithm, and only after the second step has run:
    #   sum wage_imp, d
    #   global maxWage = 10 * r(p99)
    # Ten seems awfully high as a cutoff and two would be more reasonable for
    # excluding weirdly high observations, but ten is what the original code
    # uses. `summarize, detail` and a continuous quantile do not define the 99th
    # percentile the same way, so the bound differs in its last digits; it is
    # ten times a percentile and binds on almost nothing, but it is a reason
    # this column gets a tolerance rather than an exact comparison.
    log.info("Add imputed wages in levels")

    work = work.with_columns(
        wage_imp_int=pl.col("ln_wage_imp").exp(),
        wage_imp=pl.col("ln_wage_imp2").exp(),
    )

    max_wage = work["wage_imp"].quantile(0.99, interpolation="linear") * 10
    log.info(f"Imputed wages are bounded at {max_wage:.2f} EUR per day")

    # The second line is what a cell that could not be fitted, a spell with no
    # East/West information, or a draw that overflowed in the tail leaves
    # behind: the first step's wage stands in for the second step's.
    # Written as a comparison rather than a minimum, because a minimum over a
    # null and a number is the number, and a spell with no imputed wage has to
    # keep its missing rather than acquire the bound.
    work = work.with_columns(
        wage_imp_int=pl.when(pl.col("wage_imp_int") > max_wage)
                       .then(pl.lit(max_wage)).otherwise(pl.col("wage_imp_int")),
        wage_imp=pl.when(pl.col("wage_imp") > max_wage)
                   .then(pl.lit(max_wage)).otherwise(pl.col("wage_imp")),
    ).with_columns(
        wage_imp=pl.coalesce("wage_imp", "wage_imp_int"),
    )

    log.info(" ->  wage_imp_int and wage_imp added, implausibly high wages "
             "bounded and the second stage filled from the first")

    # ------------------------------------------------------------------
    # Clean up
    # ------------------------------------------------------------------
    dropped = [column for column in CLEANUP if column in work.columns]
    log.info("The following variables are dropped from the data for cleanup: "
             + ", ".join(dropped[:-1]) + " and " + dropped[-1])

    work = work.drop(dropped + ["_row"])

    log.info(" ->  Cleanup finished")
    log.info("Wage imputation file finished")
    return work.lazy()
