"""
Synthetic tests for the imputation of right-censored wages.

Two layers, because the step has two kinds of thing to get wrong.

The first layer tests the censored normal regression on its own, against values
that do not come from this code: a cell of 4,000 rows with 26 percent censoring,
fitted by Stata's `intreg` and by R's `survival::survreg` on the same numbers.
Both sets are written out below, because the two disagree with each other by up
to 9.3e-7 relative on the four coefficients of the age profile, which is a
weakly identified block; on everything else, on the scale and on the
log-likelihood they agree to 2e-9. The port is held to survreg at 1e-7 and to
intreg at 1e-5, and the test that compares the two references to each other
records why the second tolerance cannot be the first. The cell is generated here
from a fixed seed, so nothing has to be committed beside the test.

The second layer tests the step: which rows get an imputed wage and which do
not, what happens off the employment history, what happens to a spell with no
East/West information, and that a seeded run reproduces. The values a draw
produces are not tested for equality with anything, because they cannot be; the
comparison against the Stata fixture in test_reference_10_wages_imputation.py
bounds their distribution instead.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl
import pytest

from siab.steps import impute_wages
from siab.steps.s07_wages_imputation import (
    CONTROLS,
    draw_above_the_ceiling,
    fit_censored_normal,
    predict_censored_normal,
)

# What `intreg` returns on the cell synthetic_cell() builds: the intercept, then
# the nine controls in the order CONTROLS lists them. Stata's log-likelihood was
# -2700.813243186167 and survreg's -2700.8132494 to the digits it printed.
INTREG_COEFFICIENTS = np.array([
    3.550644847276,    # intercept
    -0.236815915508,   # frau
    -0.397073753288,   # teilzeit
    1.266227796193,    # old
    0.050875764839,    # age
    -0.054758196600,   # age_sq
    -0.060274780044,   # age_old
    0.071795557761,    # age_sq_old
    0.000169379578,    # tage_job
    -0.000002430684,   # tenure_sq
])
INTREG_SIGMA = 0.451446030162

# What `survreg` returns on the same cell, in the same order.
SURVREG_COEFFICIENTS = np.array([
    3.5506447981253,
    -0.236815919768415,
    -0.397073752387635,
    1.26622661880449,
    0.050875768368589,
    -0.0547582022272316,
    -0.0602747344025767,
    0.0717955149795267,
    0.000169379577165206,
    -2.43068397819597e-06,
])
SURVREG_SIGMA = 0.45144603098453


def synthetic_cell(n: int = 4000, seed: int = 42):
    """The design, the observed log wage and the censoring flag of one cell.

    The regressors are the prep's own: two dummies, an age profile that breaks
    at 40, and a tenure in days whose square runs into the hundred-thousands.
    That spread is the point, because it is what the fit has to stay accurate
    over.
    """
    rng = np.random.default_rng(seed)
    frau = rng.integers(0, 2, n)
    teilzeit = rng.integers(0, 2, n)
    age = rng.integers(18, 61, n)
    old = (age > 40).astype(int)
    age_sq = (age / 10) ** 2
    tage_job = rng.integers(0, 4000, n)

    design = np.column_stack([
        np.ones(n), frau, teilzeit, old, age, age_sq,
        age * old, age_sq * old, tage_job, (tage_job / 10) ** 2,
    ])
    beta = np.array([3.9, -0.25, -0.40, 0.10, 0.030, -0.020,
                     -0.004, 0.0015, 0.00012, -1.2e-6])
    latent = design @ beta + rng.normal(0, 0.45, n)

    ceiling = 4.9 + rng.normal(0, 0.05, n)
    censored = latent > ceiling
    observed = np.where(censored, ceiling, latent)
    return design, observed, censored, ceiling


# ======================================================================
#  The censored normal regression
# ======================================================================

def test_the_synthetic_cell_is_built_in_the_step_s_own_regressor_order():
    """The coefficient vectors above are positional, so the order has to hold.

    synthetic_cell() builds its design as an intercept followed by the nine
    controls. If CONTROLS is ever reordered, the hardcoded Stata and R numbers
    would silently line up with the wrong terms.
    """
    assert CONTROLS == ["frau", "teilzeit", "old", "age", "age_sq",
                        "age_old", "age_sq_old", "tage_job", "tenure_sq"]
    assert len(INTREG_COEFFICIENTS) == len(CONTROLS) + 1
    assert len(SURVREG_COEFFICIENTS) == len(CONTROLS) + 1


def test_the_fit_reproduces_intreg_and_survreg():
    design, observed, censored, _ = synthetic_cell()

    fit = fit_censored_normal(design, observed, censored)

    assert fit is not None
    assert fit.kept.tolist() == list(range(design.shape[1]))
    assert fit.n_observations == 4000
    # A relative tolerance rather than an absolute one, because the ten
    # coefficients span nine orders of magnitude. Measured, the largest gap
    # against survreg is 1.2e-9 and against intreg 9.3e-7, the latter being the
    # two references' own disagreement rather than anything this code does.
    np.testing.assert_allclose(fit.coefficients, SURVREG_COEFFICIENTS, rtol=1e-7)
    np.testing.assert_allclose(fit.coefficients, INTREG_COEFFICIENTS, rtol=1e-5)
    assert fit.scale == pytest.approx(SURVREG_SIGMA, rel=1e-8)
    assert fit.scale == pytest.approx(INTREG_SIGMA, rel=1e-8)


def test_the_two_references_disagree_where_the_design_is_weakly_identified():
    """Why the tolerance against Stata above is a hundred times the other one.

    `old`, `age_old` and `age_sq_old` are nearly collinear with `age` and
    `age_sq`, and Stata reports a standard error of 0.96 on `old` against a
    coefficient of 1.27. Two optimisers stopping at the same log-likelihood can
    land a millionth apart along a direction that flat, and they do. If this
    test ever fails, the constants above were re-measured and the tolerances in
    the test above have to be re-derived rather than widened.
    """
    gap = np.abs((SURVREG_COEFFICIENTS - INTREG_COEFFICIENTS) / INTREG_COEFFICIENTS)

    assert gap.max() < 1e-5
    assert gap.max() > 1e-7
    # The block it sits in: old, age_sq, age_old and age_sq_old.
    assert set(np.flatnonzero(gap > 1e-7).tolist()) == {3, 5, 6, 7}
    assert abs(SURVREG_SIGMA - INTREG_SIGMA) / INTREG_SIGMA < 1e-8


def test_ignoring_the_censoring_would_fail_the_same_comparison():
    """The tolerance above has to be tight enough to catch the obvious error.

    Fitting ordinary least squares on the top-coded wages is what a port that
    forgot the censoring would produce. It has to miss by far more than 1e-7,
    or the test above proves nothing.
    """
    design, observed, censored, _ = synthetic_cell()

    naive, *_ = np.linalg.lstsq(design, observed, rcond=None)

    relative_gap = np.abs((naive - INTREG_COEFFICIENTS) / INTREG_COEFFICIENTS)
    assert relative_gap.max() > 0.1


def test_a_collinear_regressor_is_dropped_and_the_rest_still_fit():
    """What Stata does: omit the aliased terms, fit and predict from the rest.

    On the cell below nobody is over 40, so `old`, `age_old` and `age_sq_old`
    are columns of zeros. Stata omits exactly those three and reports the
    others; R's survreg fits the same reduced model but then predicts NA for
    every row, which is why this side follows Stata.
    """
    design, observed, censored, _ = synthetic_cell()
    young = design[:, 3] == 0

    fit = fit_censored_normal(design[young], observed[young], censored[young])

    assert fit is not None
    assert fit.kept.tolist() == [0, 1, 2, 4, 5, 8, 9]
    # Stata's own numbers on this subsample, over the columns it kept.
    np.testing.assert_allclose(
        fit.coefficients,
        [3.604495122442, -0.238660510415, -0.396587802594, 0.050678416280,
         -0.054538258695, 0.000109634257, -0.000001128406],
        rtol=1e-6,
    )
    assert fit.scale == pytest.approx(0.448471365129, rel=1e-8)

    predicted = predict_censored_normal(design[young], fit)
    assert np.isfinite(predicted).all(), (
        "a prediction went missing on a rank-deficient cell, which is the R "
        "arm's behaviour and not the one this side chose"
    )


@pytest.mark.parametrize("rows", [0, 5])
def test_a_cell_too_thin_to_fit_is_reported_rather_than_forced(rows):
    design, observed, censored, _ = synthetic_cell()

    assert fit_censored_normal(design[:rows], observed[:rows], censored[:rows]) is None


def test_a_cell_with_nothing_uncensored_is_not_fitted():
    design, observed, censored, _ = synthetic_cell()

    assert fit_censored_normal(design, observed, np.ones(len(observed), bool)) is None


# ======================================================================
#  The draw
# ======================================================================

def test_every_draw_lands_above_the_ceiling():
    rng = np.random.default_rng(3)
    expected = np.full(10_000, 4.0)
    ceiling = np.full(10_000, 4.6)

    drawn = draw_above_the_ceiling(expected, 0.4, ceiling, rng.random(10_000))

    assert np.isfinite(drawn).all()
    assert (drawn > ceiling).all()


def test_the_draw_follows_the_truncated_normal_it_claims_to():
    """A mean check, because the whole step rests on this one line.

    For a standard normal truncated below at eta the mean is the inverse Mills
    ratio, and the draw is that scaled by sigma and shifted by the prediction.
    """
    from scipy.stats import norm

    rng = np.random.default_rng(11)
    expected, sigma, ceiling = 4.0, 0.4, 4.6
    eta = (ceiling - expected) / sigma

    drawn = draw_above_the_ceiling(np.full(200_000, expected), sigma,
                                   np.full(200_000, ceiling), rng.random(200_000))

    analytic = expected + sigma * norm.pdf(eta) / (1 - norm.cdf(eta))
    assert drawn.mean() == pytest.approx(analytic, rel=1e-3)


def test_a_draw_that_overflows_in_the_tail_comes_back_missing():
    """The reference's behaviour, kept on purpose.

    Once the ceiling sits far enough above the prediction, `normal(eta)` rounds
    to one, Stata's `invnorm()` returns missing and the step's fallback carries
    the first stage's wage over. `ndtri` returns an infinity there, so the step
    turns it into a NaN, which is what a missing is inside the cell.
    """
    drawn = draw_above_the_ceiling(np.zeros(3), 1.0, np.full(3, 9.0),
                                   np.array([0.1, 0.5, 0.9]))

    assert np.isnan(drawn).all()


# ======================================================================
#  The step
# ======================================================================

def cell_frame(n_workers: int = 120, seed: int = 5, **overrides) -> pl.LazyFrame:
    """A small pipeline table the step can run over end to end.

    Three years, three education groups and both sides of the East/West split,
    so the plan has real cells in it, with enough rows per cell to be fitted.
    """
    rng = np.random.default_rng(seed)
    n = n_workers * 9
    frame = pl.DataFrame({
        "persnr": np.repeat(np.arange(1, n_workers + 1), 9),
        "spell": np.tile(np.arange(1, 10), n_workers),
        "betnr": rng.integers(1, 20, n),
        "quelle": np.where(rng.random(n) < 0.8, 1, 2),
        "year": rng.integers(2000, 2003, n),
        "age": rng.integers(20, 60, n),
        "frau": rng.integers(0, 2, n),
        "teilzeit": rng.integers(0, 2, n),
        "tage_job": rng.integers(0, 3000, n),
        "educ": rng.integers(1, 4, n),
        "east": rng.integers(0, 2, n),
        "marginal": np.zeros(n, dtype=int),
        "cpi": np.full(n, 100.0),
        "wage_defl": np.exp(rng.normal(4.4, 0.5, n)),
        "limit_assess_defl": np.full(n, 180.0),
    }).with_columns(
        pl.col(["persnr", "spell", "betnr", "quelle", "year", "age", "frau",
                "teilzeit", "tage_job", "educ", "east", "marginal"]).cast(pl.Int32),
        begepi=pl.date(2000, 1, 1),
    )
    if overrides:
        frame = frame.with_columns(**overrides)
    return frame.lazy()


@pytest.fixture(scope="module")
def imputed() -> pl.DataFrame:
    return impute_wages(cell_frame(), seed=123).collect()


def test_the_step_leaves_behind_the_three_variables_its_header_names(imputed):
    # 10_wages_imputation.do drops every intermediate it builds. Anything from
    # the cleanup list that survives would travel on into the merges below.
    new = set(imputed.columns) - set(cell_frame().collect_schema().names())
    assert new == {"cens", "wage", "wage_imp"}
    assert "_row" not in imputed.columns


def test_the_censoring_flag_is_raised_only_on_the_employment_history(imputed):
    # gen cens = 0 / replace cens = 1 if wage_defl > limit_assess4 & quelle == 1
    off_the_beh = imputed.filter(pl.col("quelle") != 1)
    assert (off_the_beh["cens"] == 0).all()
    assert off_the_beh["cens"].null_count() == 0

    ceiling = 180.0 - 4.0
    above = imputed.filter((pl.col("quelle") == 1) & (pl.col("wage_defl") > ceiling))
    assert above.height > 0
    assert (above["cens"] == 1).all()


def test_a_censored_wage_is_topcoded_and_its_imputed_value_sits_above_it(imputed):
    censored = imputed.filter(pl.col("cens") == 1)

    assert censored.height > 0
    # wage is the ceiling less four deflated euros, not the observed wage.
    np.testing.assert_allclose(censored["wage"].to_numpy(), 176.0, rtol=1e-12)
    assert (censored["wage_imp"] > censored["wage"]).all()


def test_an_uncensored_wage_is_carried_through_unchanged(imputed):
    uncensored = imputed.filter((pl.col("quelle") == 1) & (pl.col("cens") == 0))

    assert uncensored.height > 0
    np.testing.assert_allclose(uncensored["wage_imp"].to_numpy(),
                               uncensored["wage"].to_numpy(), rtol=1e-12)


def test_nothing_off_the_employment_history_gets_a_wage(imputed):
    off_the_beh = imputed.filter(pl.col("quelle") != 1)

    assert off_the_beh.height > 0
    assert off_the_beh["wage"].null_count() == off_the_beh.height
    assert off_the_beh["wage_imp"].null_count() == off_the_beh.height


def test_a_spell_with_no_east_west_information_keeps_its_own_wage():
    """    keep if missing(east)
        replace ln_wage_imp = ln_wage if quelle==1

    The reference sets these rows aside before the loop, so they are never
    imputed, and the second step leaves them missing for the final fallback to
    fill from the first.
    """
    frame = cell_frame().with_columns(
        east=pl.when(pl.col("persnr") <= 10).then(None).otherwise(pl.col("east"))
    )

    out = impute_wages(frame, seed=123).collect()
    carried = out.filter((pl.col("persnr") <= 10) & (pl.col("quelle") == 1))

    assert carried.height > 0
    np.testing.assert_allclose(carried["wage_imp"].to_numpy(),
                               carried["wage"].to_numpy(), rtol=1e-12)


def test_a_marginal_spell_keeps_its_wage_but_is_never_drawn_for():
    """`if marginal == 0` holds the estimation sample down, and nothing else.

    The line that carries an uncensored wage through is not restricted to the
    estimation sample, so a marginal spell below the ceiling still keeps its own
    wage; one above it gets no draw and falls through to missing.
    """
    frame = cell_frame().with_columns(
        marginal=pl.when(pl.col("persnr") <= 10).then(1).otherwise(0).cast(pl.Int32)
    )

    out = impute_wages(frame, seed=123).collect()
    marginal = out.filter((pl.col("persnr") <= 10) & (pl.col("quelle") == 1))

    below = marginal.filter(pl.col("cens") == 0)
    assert below.height > 0
    np.testing.assert_allclose(below["wage_imp"].to_numpy(),
                               below["wage"].to_numpy(), rtol=1e-12)

    above = marginal.filter(pl.col("cens") == 1)
    assert above.height > 0
    assert above["wage_imp"].null_count() == above.height


def test_a_spell_with_a_missing_control_keeps_its_wage_but_is_never_drawn_for():
    # Same rule as the marginal spells above, through the other half of the
    # estimation sample: Stata drops a row with a missing regressor from the
    # fit, and `gen ln_wage_tmp = ln_wage if cens == 0` still reaches it.
    frame = cell_frame().with_columns(
        teilzeit=pl.when(pl.col("persnr") <= 10).then(None)
                   .otherwise(pl.col("teilzeit"))
    )

    out = impute_wages(frame, seed=123).collect()
    incomplete = out.filter((pl.col("persnr") <= 10) & (pl.col("quelle") == 1))

    assert incomplete.filter(pl.col("cens") == 0)["wage_imp"].null_count() == 0
    above = incomplete.filter(pl.col("cens") == 1)
    assert above.height > 0
    assert above["wage_imp"].null_count() == above.height


def test_a_zero_wage_gets_no_log_and_therefore_no_imputed_wage():
    # ln_wage is only generated where wage is non-zero, so a zero-wage BEH spell
    # leaves the step with a wage and no imputed wage. 9,304 rows of the FDZ test
    # data are in that position.
    frame = cell_frame().with_columns(
        wage_defl=pl.when(pl.col("persnr") <= 5).then(0.0)
                    .otherwise(pl.col("wage_defl"))
    )

    out = impute_wages(frame, seed=123).collect()
    zero = out.filter((pl.col("persnr") <= 5) & (pl.col("quelle") == 1))

    assert zero.height > 0
    assert (zero["wage"] == 0.0).all()
    assert zero["wage_imp"].null_count() == zero.height


def test_the_same_seed_gives_the_same_imputed_wages():
    """The one thing the R arm cannot claim.

    Each cell is sorted on persnr and spell before it draws, so the draws no
    longer depend on the order the rows happen to arrive in.
    """
    first = impute_wages(cell_frame(), seed=123).collect()
    second = impute_wages(cell_frame(), seed=123).collect()

    assert first["wage_imp"].equals(second["wage_imp"])


def test_a_different_seed_gives_different_imputed_wages():
    first = impute_wages(cell_frame(), seed=123).collect()
    other = impute_wages(cell_frame(), seed=7).collect()

    # The uncensored wages are carried through and cannot move; only the drawn
    # ones may, and they have to.
    censored = pl.col("cens") == 1
    assert not first.filter(censored)["wage_imp"].equals(other.filter(censored)["wage_imp"])
    assert first.filter(~censored)["wage_imp"].equals(other.filter(~censored)["wage_imp"])


def test_the_imputed_wage_is_bounded_at_ten_times_its_own_99th_percentile():
    #   sum wage_imp, d
    #   global maxWage = 10 * r(p99)
    out = impute_wages(cell_frame(), seed=123).collect()

    bound = out["wage_imp"].quantile(0.99, interpolation="linear") * 10
    assert out["wage_imp"].max() <= bound


def test_an_unfittable_cell_costs_only_its_own_censored_wages():
    """The reference would impute them from the previous cell's model.

    `capture noisily` leaves `e()` in place when an estimation fails, so the
    reference's next `predict` runs off a model of different data. Both arms
    refuse that: the cell keeps its uncensored wages and draws nothing.
    """
    # One worker per year/education/east cell is far below the ten parameters
    # the first step fits.
    frame = pl.LazyFrame({
        "persnr": [1, 2], "spell": [1, 1], "betnr": [10, 10],
        "quelle": [1, 1], "year": [2000, 2000], "age": [30, 45],
        "frau": [0, 1], "teilzeit": [0, 0], "tage_job": [100, 900],
        "educ": [2, 2], "east": [0, 1], "marginal": [0, 0],
        "cpi": [100.0, 100.0], "wage_defl": [80.0, 400.0],
        "limit_assess_defl": [300.0, 300.0],
        "begepi": [dt.date(2000, 1, 1)] * 2,
    })

    out = impute_wages(frame, seed=123).collect()

    uncensored = out.filter(pl.col("cens") == 0)
    assert uncensored.height == 1
    assert uncensored["wage_imp"][0] == pytest.approx(80.0)

    censored = out.filter(pl.col("cens") == 1)
    assert censored.height == 1
    assert censored["wage_imp"].null_count() == 1


def test_the_step_takes_and_returns_a_lazyframe():
    # It collects the data inside, which no other step does, so the boundary is
    # worth asserting here as well as in test_lazy_discipline.py.
    result = impute_wages(cell_frame(), seed=123)
    assert isinstance(result, pl.LazyFrame)
