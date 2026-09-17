"""
The Python arm of SIAB_DuckDB.

A second reimplementation of the Stüber-Dauth-Eppelsheimer SIAB preparation
(JLMR 2023), polars over DuckDB, answering to the same Stata fixtures the R arm
answers to. The two arms are independent readings of one reference; neither is
the specification for the other.

All eighteen step functions are ported. Every step has the same shape, a
function that takes a polars LazyFrame and hands one back, with DuckDB owning
the table in between. `impute_wages()` is the one that departs from it inside:
it collects the data, because a censored normal regression per cell is not a
polars expression.
"""

from siab.steps import *  # noqa: F401,F403
from siab.steps import __all__ as _step_names

__all__ = list(_step_names)

__version__ = "0.2.0"
