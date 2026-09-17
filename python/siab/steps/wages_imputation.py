"""
07.) Imputation of right-censored wages

Port of 10_wages_imputation.do. **Not implemented.** This module holds the step's
place in the pipeline and raises `NotImplementedError` when it is called.

Generates the variables:
  - cens:     1 if the wage is right-censored and therefore imputed, 0 otherwise
              (4 EUR below the assessment ceiling)
  - wage:     daily wage, not imputed, top-coded wages replaced by the
              assessment ceiling (-4 EUR), deflated to 2015
  - wage_imp: imputed daily wage, deflated to 2015

Why it is deferred
------------------
This is the one step of the sixteen with no drop-in Python equivalent. Stata
fits the censored normal model with `intreg` and the R arm fits it with
`survival::survreg`. Neither has an obvious counterpart in the Python
scientific stack, so the choice is made once, deliberately, with the whole
ecosystem surveyed first, rather than settled by whatever library happened to
be to hand on the day.

What the survey has to cover, when it happens: `pyfixest` was named as a
candidate but is fixed-effects OLS and IV, so it is unlikely to carry a
censored normal fit; `lifelines` and `statsmodels` are the nearer guesses; a
hand-rolled likelihood through `scipy.optimize` stays available if no library
fits. None of that is checked.

What blocks on it
-----------------
`wage_imp` feeds `handle_parallel_episodes()` and both panel builders, so the
comparisons against the committed fixtures 15_parallel_episodes,
16_yearly_panel and 16_monthly_panel wait on this step. The three columns built
from the imputed wage are compared as distributions rather than row by row,
because both sides draw their own random terms from different generators.

The R arm carries the same shape at 09_restrictions.do: an unported step with a
committed fixture and a test that skips.

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Wolfgang Dauth and
Johann Eppelsheimer, following Gartner (2005)

Version: 0.0
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

import polars as pl

__all__ = ["impute_wages"]


def impute_wages(frame: pl.LazyFrame,
                 log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    """Raise, because the censored-wage imputation is not ported yet.

    The signature matches what the ported step will carry, so the call site in
    the pipeline is written once and does not move when the body arrives.
    """
    raise NotImplementedError(
        "The wage imputation is not ported to the Python arm yet. It needs a "
        "censored normal regression, which Stata does with intreg and the R arm "
        "with survival::survreg; the Python replacement is chosen after a survey "
        "of the regression ecosystem. See the module docstring."
    )
