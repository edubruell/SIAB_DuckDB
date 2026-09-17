"""
The Python arm of SIAB_DuckDB.

A second reimplementation of the Stüber-Dauth-Eppelsheimer SIAB preparation
(JLMR 2023), polars over DuckDB, answering to the same Stata fixtures the R arm
answers to. The two arms are independent readings of one reference; neither is
the specification for the other.

Seventeen of the eighteen step functions are ported. The one that is not is the
imputation of right-censored wages, which needs a censored normal regression:
`impute_wages()` raises `NotImplementedError` and says why. Every step has the
same shape, a function that takes a polars LazyFrame and hands one back, with
DuckDB owning the table in between.
"""

from siab.steps import *  # noqa: F401,F403
from siab.steps import __all__ as _step_names

__all__ = list(_step_names)

__version__ = "0.2.0"
