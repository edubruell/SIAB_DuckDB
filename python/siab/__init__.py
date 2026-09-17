"""
The Python arm of SIAB_DuckDB.

A second reimplementation of the Stüber-Dauth-Eppelsheimer SIAB preparation
(JLMR 2023), polars over DuckDB, answering to the same Stata fixtures the R arm
answers to. The two arms are independent readings of one reference; neither is
the specification for the other.

Three of the sixteen reference steps are ported. The rest follow the same shape:
a function that takes a polars LazyFrame and hands one back, with DuckDB owning
the table in between.
"""

from siab.steps import (
    generate_biographic_variables,
    reallocate_one_time_payments,
    split_episodes,
)

__all__ = [
    "split_episodes",
    "reallocate_one_time_payments",
    "generate_biographic_variables",
]

__version__ = "0.1.0"
