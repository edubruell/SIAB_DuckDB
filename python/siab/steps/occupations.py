"""
03.) Add occupation classifications for the beruf variable

Port of 14_occ_blossfeld.do, and of the 2-digit KldB-88 Berufsgruppe the R arm
builds alongside it. The reference writes occ_blo as one long `recode` over the
three-digit code; both arms read the same crosswalk csv instead, which is that
recode's value list turned into a table.

Generates the variables:
  - occ_kldb88_2: the 2-digit KldB-88 Berufsgruppe of the 3-digit beruf code
  - occ_blo: Blossfeld occupations

beruf in SIAB 7523 v2 is the 3-digit KldB-88 Berufsordnung, so both merges are
exact and no occupation is dropped for want of a unique match.

Codes 555, 666, 888, 971, 981, 982, 983, 991, 995, 996 and 997 are SIAB
administrative categories rather than occupations and take occ_blo = 99.

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Wolfgang Dauth,
Johann Eppelsheimer and Heiko Stüber

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import os

import polars as pl

from siab.common import classifications_dir, step_logger

__all__ = ["generate_occupation_variables"]

# Both crosswalks are written by the R arm with `write_csv()`, which spells a
# missing value `NA`. readr reads that back as missing; polars does not unless
# it is told to, and would instead refuse to parse the column as an integer.
CSV_NULLS = ["NA"]


def generate_occupation_variables(frame: pl.LazyFrame,
                                  log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    log = step_logger("occ_vars", log_file)
    log.info("Occupation variable script started")

    # ==================================================================
    #  The 2-digit KldB-88 Berufsgruppe
    # ==================================================================

    # The crosswalk is 342 rows of reference data, not pipeline data, so it is
    # read eagerly: the administrative codes are logged out of it below, and a
    # scan would have to be collected for that anyway. The pipeline frame is
    # never collected in this step.
    log.info("Reading the KldB-88 occupation table")
    kldb88_2d = pl.read_csv(
        classifications_dir() / "kldb88_beruf.csv", null_values=CSV_NULLS
    ).select("beruf", occ_kldb88_2=pl.col("kldb88_2").cast(pl.Int32))

    # Report the codes that sit outside the KldB-88 structure and get no
    # Berufsgruppe.
    for code in kldb88_2d.filter(pl.col("occ_kldb88_2").is_null())["beruf"]:
        log.info(f"beruf = {code} is a SIAB administrative code outside KldB-88 "
                 f"and gets no Berufsgruppe")

    # A left join keeps every row, including the episodes with no beruf at all.
    # The crosswalk's key is a 64-bit integer and the pipeline's is a 32-bit
    # one; polars matches across the two and keeps the left frame's column, so
    # beruf itself comes out of the join untouched, which the comparison
    # against the reference tests directly.
    log.info("Merging the 2-digit KldB-88 Berufsgruppe to beruf")
    frame = frame.join(kldb88_2d.lazy(), on="beruf", how="left",
                       maintain_order="left")
    log.info(" -> 2-digit occupation variable (occ_kldb88_2) added")

    # ==================================================================
    #  Blossfeld occupations
    # ==================================================================

    log.info("Reading the Blossfeld walkover")
    occblo = pl.scan_csv(
        classifications_dir() / "walkover_beruf_occblo.csv", null_values=CSV_NULLS
    ).select("beruf", occ_blo=pl.col("occ_blo").cast(pl.Int32))

    # 14_occ_blossfeld.do closes its recode with `(else = 99)`, and Stata's
    # `else` covers missing values as well as unmatched ones. An episode with no
    # beruf at all, which is every benefit and job-search spell, therefore
    # leaves the reference carrying 99, "not assignable", rather than missing.
    # The fill reproduces that; without it the two sides differ on 121,073 of
    # the test data's 505,050 rows.
    log.info("Merging the Blossfeld classification to beruf")
    frame = frame.join(occblo, on="beruf", how="left",
                       maintain_order="left").with_columns(
        occ_blo=pl.col("occ_blo").fill_null(99).cast(pl.Int32)
    )
    log.info("-> occ_blo variable added")

    log.info("Occupation variables script finished")
    return frame
