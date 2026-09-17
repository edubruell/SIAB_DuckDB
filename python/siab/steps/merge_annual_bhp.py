"""
07b.) Merge the yearly Establishment History Panel (BHP) and the worker-flow
and entry-and-exit extension files to the SIAB

Port of 11_merge_BHP.do. Merges up to five sources, each many-to-one on betnr
and year, each keeping every SIAB episode whether it matched or not.

Generates the variables:
  - annual:  az_f, az_reg, az_azubi, az_atz, az_tz, az_f_vz, az_f_tz, az_reg_vz
             (the yearly establishment variable blocks, one file per calendar
             year)
  - inflow:  ein_ges, ein_gf, ein_vz (hirings, Worker Flows extension)
  - outflow: aus_ges, aus_gf, aus_vz (separations, the same extension)
  - entry:   eintritt, besch, besch_vor, status_vor, inflow (establishment
             entries, Entry and Exit extension)
  - exit:    austritt, besch, besch_nach, status_nach, outflow (establishment
             exits, the same extension)

Modifies the variable:
  - besch: the one column two of the five files carry. The entry merge creates
    it and the exit merge fills it in where entry left it missing.

Notes:
  The reference merges the yearly files in a loop, one `merge m:1 betnr jahr`
per year from minYear to maxYear, and each merge carries `update`. `update`
fills a missing value in the master from the using file and leaves a non-missing
one alone. Every yearly file holds exactly one calendar year and is unique on
betnr, so a SIAB episode can match at most one of them: the loop therefore has
the same result as one join against all the yearly files stacked, and that is
what this function does. The forty-nine-way loop is not reproduced.

  Between the four extension files `update` does real work, because entry and
exit both carry `besch`. The entry merge creates the column, the exit merge
fills it in wherever entry left it missing, and where both have a value the
entry one stays. The joins here run in the reference's order and coalesce any
column the incoming file shares with the data, which is what `update` without
`replace` does.

  The whole step is switched off in the reference master, because all five
files have to be requested from the FDZ separately. Passing modules = () skips
it here the same way; a module whose files are absent is skipped with a warning
rather than stopping the pipeline.

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Wolfgang Dauth and
Johann Eppelsheimer

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import logging
import os
import warnings
from pathlib import Path
from typing import Callable, Iterable, Sequence

import polars as pl

from siab.common import read_stata, step_logger

__all__ = ["merge_annual_bhp"]

KNOWN_MODULES = ("annual", "inflow", "outflow", "entry", "exit")

# The delivery keys on betnr_siab and jahr; the prepared SIAB uses betnr and
# year. persnr_siab is in the map for symmetry with the AKM step: none of the
# five establishment files carries it, and read_stata() renames only what it
# finds.
KEY_RENAME = {"betnr_siab": "betnr", "persnr_siab": "persnr", "jahr": "year"}

JOIN_KEYS = ["betnr", "year"]


def _read_bhp(path: str | os.PathLike) -> pl.LazyFrame:
    """Read one BHP file and put the delivery's keys onto the pipeline's names.

    No column subset is asked for: the reference merges every variable the file
    carries, so the needed columns are all of them. The files are narrow enough
    for that (ten columns on a yearly block, seven on the widest extension).
    """
    return read_stata(path, rename=KEY_RENAME)


def _align_keys(incoming: pl.LazyFrame, frame: pl.LazyFrame) -> pl.LazyFrame:
    """Cast the join keys of the incoming file onto the dtypes the data carries.

    pyreadstat widens a Stata int32 to a 64-bit integer, while betnr and year
    travel through the pipeline as Int32 (DuckDB INTEGER, and `year` built by
    `dt.year()`). polars will not match two integer columns of different width
    on a join, so without this cast the merge either raises or, where a plan
    coerces silently, matches nothing at all and every added column comes back
    null. `collect_schema()` resolves the plan's schema; it reads no data.
    """
    schema = frame.collect_schema()
    return incoming.with_columns(
        [pl.col(key).cast(schema[key]) for key in JOIN_KEYS if key in schema]
    )


def _check_unique(incoming: pl.LazyFrame, name: str) -> int:
    """Rows and distinct betnr-year pairs of an incoming file, checked as R does.

    The reference merges `m:1`, which fails if the using file repeats a key, so
    the R port refuses a file that is not unique by betnr and year and so does
    this. The check is the one collect per file: it materialises two numbers,
    and the file is already in memory because pyreadstat read it eagerly.
    """
    counts = incoming.select(
        n_rows=pl.len(),
        n_keys=pl.struct(JOIN_KEYS).n_unique(),
    ).collect()
    n_rows = counts.item(0, "n_rows")
    n_keys = counts.item(0, "n_keys")
    if n_rows != n_keys:
        raise ValueError(f"{name} is not unique by betnr and year")
    return n_rows


def _join_and_update(frame: pl.LazyFrame,
                     incoming: pl.LazyFrame,
                     log: logging.Logger) -> pl.LazyFrame:
    """One `merge m:1 betnr jahr ..., keep(master match match_update) update`.

    `update` without `replace`: fill a missing value in the data from the
    incoming file, leave a value that is already there alone. The incoming
    column arrives under a `_using` suffix, COALESCE picks the master value
    first, and the suffixed copy is dropped again, so the data keeps the column
    it had. A plain left join would overwrite instead of fill, which on `besch`
    is the difference between the entry value and the exit value.

    `keep(master match match_update)` is a left join: every SIAB episode stays,
    matched or not, and a using row that matches nothing is discarded.
    """
    present = frame.collect_schema().names()
    shared = [c for c in incoming.collect_schema().names()
              if c in present and c not in JOIN_KEYS]

    # maintain_order="left" keeps the episodes in the order they arrived in.
    # The reference's merge leaves the data sorted by the merge key and the
    # do-file re-sorts on persnr spell at the end, so the order is not carried
    # by this step either way; asking for it only keeps the port deterministic.
    joined = frame.join(incoming, on=JOIN_KEYS, how="left", suffix="_using",
                        maintain_order="left")

    if shared:
        log.info(f"`update` applies to {', '.join(shared)}")
        joined = joined.with_columns(
            [pl.coalesce(pl.col(c), pl.col(f"{c}_using")) for c in shared]
        ).drop([f"{c}_using" for c in shared])

    return joined


def merge_annual_bhp(frame: pl.LazyFrame,
                     bhp_folder: str | os.PathLike | Callable[..., Path] | None = None,
                     prefix: str = "SIAB_7523_v2",
                     years: Iterable[int] = range(1975, 2024),
                     modules: Sequence[str] = KNOWN_MODULES,
                     log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    log = step_logger("bhp_annual", log_file)
    log.info("Annual BHP merge started")

    modules = list(modules)
    if len(modules) == 0:
        log.info("No modules requested, step skipped")
        return frame

    unknown = [m for m in modules if m not in KNOWN_MODULES]
    if unknown:
        raise ValueError(f"Unknown BHP module: {', '.join(unknown)}")

    if bhp_folder is None:
        raise ValueError("Please set bhp_folder to the folder holding the BHP files")
    folder = Path(bhp_folder("")) if callable(bhp_folder) else Path(bhp_folder)

    # ==================================================================
    #  The yearly establishment variable blocks
    # ==================================================================

    if "annual" in modules:
        files = [folder / f"{prefix}_bhp_v1_{year}.dta" for year in years]
        files = [path for path in files if path.exists()]

        if len(files) == 0:
            warnings.warn(f"No yearly BHP files under {folder}, module skipped")
        else:
            log.info(f"Reading {len(files)} yearly BHP files")

            # Each file holds one calendar year, so stacking them and joining
            # once is the reference's year-by-year loop with the same result.
            # See the header. `diagonal` is R's bind_rows(): a year whose block
            # is missing a variable contributes nulls rather than an error.
            annual = _align_keys(
                pl.concat([_read_bhp(path) for path in files], how="diagonal"),
                frame,
            )

            n_rows = _check_unique(annual, "bhp_annual")
            log.info(f"Stacked to {n_rows} establishment-year rows")

            frame = _join_and_update(frame, annual, log)
            log.info(" -> Yearly establishment variables added")

    # ==================================================================
    #  The four extension files, in the reference's order
    # ==================================================================

    for module in [m for m in ("inflow", "outflow", "entry", "exit") if m in modules]:
        path = folder / f"{prefix}_bhp_{module}_v1.dta"
        if not path.exists():
            warnings.warn(f"{path.name} not found, {module} module skipped")
            continue

        incoming = _align_keys(_read_bhp(path), frame)
        n_rows = _check_unique(incoming, f"bhp_{module}")
        log.info(f"Read {n_rows} establishment-year rows from {path.name}")

        frame = _join_and_update(frame, incoming, log)
        log.info(f" -> {module} variables added")

    # ==================================================================
    #  Inspect the merge the way 11_merge_BHP.do does, by source
    # ==================================================================

    # The reference's `tab erwstat _merge` after every merge; the R port reports
    # it once, by source, and so does this. It is the step's only collect on the
    # data: the join chain has to run to build the summary, and only the summary
    # itself, one row per source, is materialised. The joins run again when the
    # caller writes the table back, which is the price of handing a LazyFrame on
    # rather than a collected one.
    log.info("Match rate by source (quelle):")
    present = frame.collect_schema().names()
    added = [c for c in ("az_f", "ein_ges", "aus_ges", "eintritt", "austritt")
             if c in present]
    if added:
        probe = added[0]
        rates = (
            frame.group_by("quelle")
            .agg(n_episodes=pl.len(),
                 n_matched=pl.col(probe).is_not_null().sum())
            .sort("quelle")
            .collect()
        )
        for row in rates.iter_rows(named=True):
            share = round(100 * row["n_matched"] / row["n_episodes"], 1)
            log.info(f"quelle = {row['quelle']}: {row['n_matched']} of "
                     f"{row['n_episodes']} episodes matched on {probe} ({share}%)")

    log.info("Annual BHP merge finished")
    return frame
