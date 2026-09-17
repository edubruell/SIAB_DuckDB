"""
04.) Create broader education groups based on imputed education (ausbildung_imp)

Port of 05_educ_broad.do. The step imputes nothing of its own: it reads the
FDZ's already imputed training variable ausbildung_imp, which carries the
Fitzenberger, Osikominu and Völter (2008) procedure, and folds its six
categories into three broad ones. University and university of applied science
are combined.

Generates the variable:
  - educ: education, 1 no vocational training, 2 vocational training,
          3 university or university of applied science

The raw `ausbildung` is deliberately not read. It carries the full
administrative code list and would put most spells in the wrong group.

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Wolfgang Dauth,
Johann Eppelsheimer and Heiko Stüber

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import os

import polars as pl

from siab.common import step_logger

__all__ = ["generate_educ_variable"]


def generate_educ_variable(frame: pl.LazyFrame,
                           log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    log = step_logger("educ", log_file)
    log.info("Adding broad education categories")

    # The reference opens with `gen educ = .` and then replaces in the order
    # 3, 2, 1. The three value lists do not overlap, so the order carries no
    # meaning; it is kept anyway, because the step is compared against the
    # reference's own output.
    #
    # `inlist(ausbildung_imp, 5, 6)` is false when ausbildung_imp is missing,
    # so a missing training code leaves educ at Stata's missing. A polars
    # `is_in` returns null rather than false on a null input, and a null
    # condition in a when/then chain falls through to the next branch, so the
    # missing code reaches `otherwise` and comes out null as well. The
    # `otherwise` is the reference's `gen educ = .`, read from the other end:
    # anything the three lists do not catch, including a code outside 1 to 6,
    # stays missing.
    frame = frame.with_columns(
        educ=pl.when(pl.col("ausbildung_imp").is_in([5, 6]))
        # degree from a university or university of applied science (Uni or FH)
        .then(pl.lit(3, dtype=pl.Int32))
        # vocational training (Ausbildung)
        .when(pl.col("ausbildung_imp").is_in([2, 4]))
        .then(pl.lit(2, dtype=pl.Int32))
        # neither vocational training nor degree from university (of applied
        # science)
        .when(pl.col("ausbildung_imp").is_in([1, 3]))
        .then(pl.lit(1, dtype=pl.Int32))
        .otherwise(pl.lit(None, dtype=pl.Int32))
    )

    # The reference's `tab ausbildung_imp educ, m`, which the R arm runs as a
    # grouped count and logs. This is the one place the step leaves the lazy
    # chain: a count has to be materialised to be written to a log. Only the
    # aggregate is collected, the returned frame stays lazy, and the recode
    # above is not computed twice on anything but this one pass.
    tabulation = (
        frame.group_by("educ", "ausbildung_imp")
        .agg(n=pl.len())
        .sort("ausbildung_imp", "educ")
        .collect()
    )
    for row in tabulation.iter_rows(named=True):
        imp = "NA" if row["ausbildung_imp"] is None else row["ausbildung_imp"]
        educ = "NA" if row["educ"] is None else row["educ"]
        log.info(f"ausbildung_imp = {imp} encoded as educ = {educ} "
                 f"for {row['n']} cases")

    log.info("Educ variable generated")
    return frame
