"""
07c.) Merge the AKM person and establishment wage effects to the SIAB

Port of 12_merge_AKM.do.

Generates, from the establishment file:
  - feff_1985_1992 ... feff_2017_2023: the establishment wage effect in each of
    the five estimation windows
and from the person file:
  - peff_1985_1992 ... peff_2017_2023: the person wage effect in the same five

Notes:
  Two many-to-one joins, the establishment effects on betnr and the person
effects on persnr, each keeping every SIAB episode whether it matched or not.
Neither file carries a year: an effect is estimated once per window, so the five
columns arrive side by side and it is the analysis that picks the window.

  The effects are estimated on the connected set of movers among regular
full-time workers aged 20 to 60, so an episode outside that group can match and
still be meaningless. The reference merges on the identifier regardless and
leaves the decision to the user, and so does this. The FDZ methodology report
puts the usable subset at erwstat 101 and teilzeit 0.

  Both effects are window-specific and centred on zero within their window, so
anything using more than one window has to normalise them first.

  The step is switched off in the reference master, because the two files have
to be requested from the FDZ separately and are not part of any delivery.
Leaving either path None skips that side here, which is what the master's two
switches do.

Reference:
  Lochner, Benjamin; Wolter, Stefanie (2025): AKM effects for German labour
  market data 1985-2023. FDZ-Methodenreport 03/2025 (en).

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Wolfgang Dauth and
Johann Eppelsheimer

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import polars as pl

from siab.common import read_stata, step_logger

__all__ = ["merge_akm"]

# The delivery keys on persnr_siab and betnr_siab; the prepared SIAB has already
# renamed both to persnr and betnr. read_stata() renames only what it finds, so
# a file that already carries the prepared name passes through untouched.
KEY_RENAME = {"betnr_siab": "betnr", "persnr_siab": "persnr", "jahr": "year"}


def _merge_side(frame: pl.LazyFrame,
                path: str | os.PathLike,
                key: str,
                side: str,
                log: logging.Logger) -> pl.LazyFrame:
    """One `merge m:1 <key> using ..., keep(master match)`.

    No `update` on this side of the prep: neither AKM file shares a column with
    the data, so the five effect columns are simply added. `keep(master match)`
    is a left join, which leaves every episode outside the connected set with
    five missing effects rather than dropping it.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"AKM {side} file not found: {path}")

    # The whole file is read: it is the key and five effects, and the reference
    # merges all five.
    incoming = read_stata(path, rename=KEY_RENAME)
    columns = incoming.collect_schema().names()
    if key not in columns:
        raise ValueError(f"The AKM {side} file has no {key} column")

    # pyreadstat widens the file's Stata int32 identifier to a 64-bit integer,
    # while persnr and betnr travel through the pipeline as Int32 (DuckDB
    # INTEGER). polars will not match two integer columns of different width on
    # a join, so the key is cast onto the dtype the data carries before the
    # join; without it the merge either raises or silently matches nothing.
    # `collect_schema()` resolves the plan's schema and reads no data.
    incoming = incoming.with_columns(pl.col(key).cast(frame.collect_schema()[key]))

    effects = [c for c in columns if c != key]

    # The reference merges `m:1`, which fails on a using file that repeats the
    # key, so the R port refuses one and so does this. The check is a collect of
    # two numbers off a file pyreadstat has already read into memory.
    counts = incoming.select(n_rows=pl.len(), n_keys=pl.col(key).n_unique()).collect()
    n_rows = counts.item(0, "n_rows")
    if n_rows != counts.item(0, "n_keys"):
        raise ValueError(f"The AKM {side} file is not unique by {key}")

    log.info(f"Read {n_rows} rows and {len(effects)} effects from {path.name}")

    # maintain_order="left" keeps the episodes in the order they arrived in.
    # Stata's merge leaves the data sorted by the merge key and the do-file
    # re-sorts on persnr spell at the end, so this step does not carry the order
    # either way; asking for it only keeps the port deterministic.
    frame = frame.join(incoming, on=key, how="left", maintain_order="left")

    # Report the share of episodes that received an effect, per window. On the
    # real files this is the merge rate the methodology report tabulates; it is
    # the only thing about this step worth reading off the data. It is also the
    # one collect per side: the join has to run to build the summary, but only
    # the summary, a single row, is materialised.
    summary = frame.select(
        [pl.col(c).is_not_null().sum().alias(c) for c in effects]
        + [pl.len().alias("n_episodes")]
    ).collect()
    n_episodes = summary.item(0, "n_episodes")
    for window in effects:
        n_matched = summary.item(0, window)
        share = round(100 * n_matched / n_episodes, 1) if n_episodes else 0.0
        log.info(f"{window}: {n_matched} of {n_episodes} episodes ({share}%)")

    log.info(f" -> AKM {side} effects added")
    return frame


def merge_akm(frame: pl.LazyFrame,
              akm_estab_file: str | os.PathLike | None = None,
              akm_pers_file: str | os.PathLike | None = None,
              log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    log = step_logger("akm", log_file)
    log.info("AKM merge started")

    if akm_estab_file is None and akm_pers_file is None:
        log.info("Neither AKM file given, step skipped")
        return frame

    if akm_estab_file is not None:
        frame = _merge_side(frame, akm_estab_file, "betnr", "establishment", log)

    if akm_pers_file is not None:
        frame = _merge_side(frame, akm_pers_file, "persnr", "person", log)

    log.info("AKM merge finished")
    return frame
