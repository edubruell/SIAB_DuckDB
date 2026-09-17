"""
06.) Deflate wages, marginal part-time income threshold and contribution assessment ceiling

Port of 08_wages_deflation.do.

Consumer Price Index:
  Statistisches Bundesamt (2019)
  Preise - Verbraucherpreisindizes fuer Deutschland (Lange Reihe ab 1948)
  https://www.destatis.de/DE/Themen/Wirtschaft/Preise/Verbraucherpreisindex/_inhalt.html

Generates the variables:
  - cpi: consumer price index, 2015 = 100
  - wage_defl: daily wage, deflated
  - limit_marginal_defl: marginal part-time income threshold, deflated
  - limit_assess_defl: contribution assessment ceiling, deflated

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Wolfgang Dauth and
Johann Eppelsheimer

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import os

import polars as pl

from siab.common import classifications_dir, stata_float, step_logger

__all__ = ["deflate_wages"]


def deflate_wages(frame: pl.LazyFrame,
                  log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    log = step_logger("deflate_wages", log_file)
    log.info("Reading cpi data from csv")

    # 08_wages_deflation.do builds the index with `gen cpi = .` and a column of
    # `replace`s, and rebases the years up to 1991 with `(cpi/89.0)*65.5`, all of
    # it in a Stata float. The csv holds the result of that rebasing as a double,
    # so stata_float() is what puts it back where the reference had it. It is the
    # one lookup in the prep the reference did arithmetic on, which is why six of
    # the fifty years sit one unit in the last place away from the Stata value
    # even after this: the two arrive at neighbouring floats. The gap is 9e-08
    # relative and below anything a float resolves.
    tbl_cpi = pl.read_csv(classifications_dir() / "cpi.csv")
    tbl_cpi = tbl_cpi.with_columns(
        cpi=pl.Series("cpi", stata_float(tbl_cpi["cpi"].to_numpy()))
    ).lazy()

    log.info("Add cpi to data and generate wage_defl, limit_marginal_defl "
             "and limit_assess_defl")

    # A year the index does not cover leaves cpi missing, and the three
    # divisions carry that missing through rather than dropping the row. The
    # results are not cut back to float: the reference stores them as one, but
    # the comparison against it runs on a relative tolerance of 1e-6 that covers
    # the storage, and rounding here would only throw away digits the wage
    # itself still has.
    frame = frame.join(
        tbl_cpi, on="year", how="left", maintain_order="left"
    ).with_columns(
        wage_defl=100 * pl.col("tentgelt") / pl.col("cpi"),
        limit_marginal_defl=100 * pl.col("limit_marginal") / pl.col("cpi"),
        limit_assess_defl=100 * pl.col("limit_assess") / pl.col("cpi"),
    )

    log.info(" ->  cpi, wage_defl, limit_marginal_defl and limit_assess_defl added")
    log.info("Wage deflation file finished")
    return frame
