"""
03b.) Merge the basic Establishment History Panel (BHP) to the SIAB

Port of 04_merge_basic_BHP.do. The reference runs one `merge m:1 betnr jahr
using "${orig}/SIAB_7523_v2_bhp_basis_v1.dta", keep(master match)`, which adds
the establishment's characteristics to every SIAB episode that carries an
establishment number and leaves the rest untouched.

Generates the variables:
  - ao_bula:   the federal state of the establishment
  - w93_3_gen: the 3-digit WZ93 industry of the establishment, extrapolated
  and whatever else is named in keep_variables

Notes:
  The merge is many-to-one on betnr and year and keeps every SIAB episode,
matched or not, which is what `keep(master match)` asks for. Episodes without
an establishment number cannot match, and non-employment episodes are the bulk
of those, which is why the step logs the match rate by source (quelle) rather
than asserting on it. The reference inspects the same thing with
`tab erwstat _merge`.

  The delivery keys on betnr_siab and jahr; the prepared SIAB uses betnr and
year. read_stata() renames them on the way in.

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Wolfgang Dauth and
Johann Eppelsheimer

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Sequence

import polars as pl

from siab.common import read_stata, step_logger

__all__ = ["merge_basic_bhp"]

JOIN_KEYS = ["betnr", "year"]


def merge_basic_bhp(frame: pl.LazyFrame,
                    bhp_file: str | os.PathLike,
                    keep_variables: Sequence[str] = ("ao_bula", "w93_3_gen"),
                    log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    log = step_logger("bhp_basis", log_file)
    log.info("Basic BHP merge started")

    if bhp_file is None:
        raise ValueError("Please set bhp_file to the Basic Establishment File")
    bhp_path = Path(bhp_file)
    if not bhp_path.exists():
        raise FileNotFoundError(f"Basic Establishment File not found: {bhp_path}")

    keep_variables = list(keep_variables)

    # Read only the keys and the variables the merge wants. The establishment
    # file is wide, and `keepusing()` in the reference does the same thing.
    bhp = read_stata(
        bhp_path,
        columns=["betnr_siab", "jahr", *keep_variables],
        rename={"betnr_siab": "betnr", "jahr": "year"},
    )

    # First of the two collections in this step. `merge m:1` in Stata aborts
    # when the using file is not unique on the key, and a duplicate
    # establishment-year would silently turn this join into m:m and duplicate
    # SIAB episodes, so the check has to be materialised. The row count the R
    # arm logs comes out of the same pass.
    counts = bhp.select(
        n_rows=pl.len(),
        n_keys=pl.struct(JOIN_KEYS).n_unique(),
    ).collect()
    n_rows = counts.item(0, "n_rows")
    n_keys = counts.item(0, "n_keys")

    log.info(f"Read {n_rows} establishment-year rows from {bhp_path.name}")
    if n_keys != n_rows:
        raise ValueError(
            "The Basic Establishment File is not unique by betnr and year: "
            f"{n_rows} rows on {n_keys} establishment-years"
        )

    # Watch the join key dtypes. pyreadstat hands back Int64 for both keys,
    # while the pipeline carries betnr as Int32 (it comes out of the SIAB that
    # way) and year as Int64. polars 1.44 finds a supertype for two integer
    # widths and joins them correctly, but it refuses a key whose dtypes sit in
    # different families: a Stata column that carries any missing value comes
    # back as Float64, and Float64 against Int32 aborts the join with a
    # SchemaError rather than matching. Casting the using side onto the
    # master's dtypes covers both, and it is the direction that cannot lose a
    # value: the establishment numbers came out of the same delivery.
    master_schema = frame.collect_schema()
    for key in JOIN_KEYS:
        if key not in master_schema:
            raise ValueError(f"merge_basic_bhp() needs the join key {key}")
    bhp = bhp.with_columns(
        [pl.col(key).cast(master_schema[key]) for key in JOIN_KEYS]
    )

    log.info(f"Merging {', '.join(keep_variables)} on betnr and year")

    # `keep(master match)` is a left join: every SIAB episode survives, matched
    # or not.
    frame = frame.join(bhp, on=JOIN_KEYS, how="left")

    log.info(" -> Basic establishment variables added")

    # Second collection. Inspect the merge the way 04_merge_basic_BHP.do does,
    # by source. A rate is a count over the merged frame and cannot be had
    # lazily; the reference prints the same table with `tab erwstat _merge`.
    #
    # The R arm names ao_bula as the match indicator because it is the first of
    # the default keep_variables. Taking the first kept variable instead keeps
    # the log honest when a caller merges something else.
    match_flag = keep_variables[0]
    rates = (
        frame.group_by("quelle")
        .agg(
            n_episodes=pl.len(),
            n_matched=pl.col(match_flag).is_not_null().sum(),
        )
        .sort("quelle")
        .collect()
    )

    log.info("Match rate by source (quelle):")
    for row in rates.iter_rows(named=True):
        share = round(100 * row["n_matched"] / row["n_episodes"], 1)
        log.info(
            f"quelle = {row['quelle']}: {row['n_matched']} of "
            f"{row['n_episodes']} episodes matched ({share}%)"
        )

    log.info("Basic BHP merge finished")
    return frame
