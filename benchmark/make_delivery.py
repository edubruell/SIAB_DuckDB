"""
Build a synthetic SIAB delivery of any size out of the FDZ test delivery.

Every scale claim this project makes rests on a 577,776-row test delivery, which
is about one one-hundred-and-fortieth of a real SIAB 7523 v2 and never leaves a
laptop's memory. This script stacks renumbered copies of that delivery into one
of arbitrary size, so that the questions the design was decided on -- does the
out-of-core path hold, does the Parquet handover earn its wall clock, where does
the original Stata prep stop fitting in memory -- are measured rather than
argued.

What a copy is
--------------

Copy `c` of the delivery is the delivery with

  * `persnr` and `betnr` moved into a range of their own, so that stacking N
    copies gives N times the persons and N times the establishments rather than
    one person observed N times;
  * every date of a person shifted by the same whole number of days, drawn once
    per person in [-15, +15] and clamped to the delivery's own first and last
    day, which keeps `begorig <= begepi <= endepi <= endorig` intact while
    moving a few spells across a year boundary;
  * `gebjahr` shifted by a year or two, drawn once per person;
  * every wage multiplied by a factor within two percent of one, drawn per row.

The jitter is there for two reasons. It keeps the copies from being
byte-identical, which a columnar store would otherwise compress to a size no
real delivery has, and it moves a handful of spells across the assessment
ceiling and the year boundaries, which keeps the imputation and the episode
splitting doing work at every copy. Everything else -- the categorical
variables, the establishment identities within a copy, the source restriction --
is the delivery's own.

The establishment side grows with the person side. Each copy's establishment
files carry that copy's `betnr` range, so the merges join against a universe
that is N times as large, as they would on a real delivery.

What it writes
--------------

  --store TARGET     the `orig` table both arms' pipelines read, written
                     straight into a DuckDB database or a folder of Parquet
                     files. This is the fast path: no Stata file is involved.
  --delivery DIR     a folder of Stata files under the delivery's own names,
                     which is what the two merges read and what the original
                     Stata prep needs. The establishment files are always
                     written here; the core spell file follows with --core-dta.

Both arms read the establishment files as Stata files and hold one whole, so a
run against a large delivery is bounded by the largest of them. The script
prints the estimate before it writes.

Run it with:

  uv run --project python python benchmark/make_delivery.py --copies 10 \
      --store ~/data/siab_bench/siab_10x.duckdb \
      --delivery ~/data/siab_bench/delivery_10x

Author(s): Eduard Brüll
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import polars as pl
import pyreadstat

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent
if str(PROJECT / "python") not in sys.path:
    sys.path.insert(0, str(PROJECT / "python"))

from stata_to_db_batch_read import (  # noqa: E402
    BATCH_SIZE,
    KEY_RENAMES,
    STATA_EPOCH_OFFSET,
    _DuckDBSink,
    _ParquetSink,
    column_types,
)
from siab.common import open_store  # noqa: E402

# The delivery's own file names. The prefix is the one both merges build their
# file names from, and the reference's do-files expect the same spelling.
PREFIX = "SIAB_7523_v2"
CORE_FILE = f"{PREFIX}.dta"
BASIC_BHP_FILE = f"{PREFIX}_bhp_basis_v1.dta"
MODULES = ("inflow", "outflow", "entry", "exit")
PANEL_YEARS = range(1975, 2024)

# The five date columns of the core file, and the wages a copy moves.
DATE_COLUMNS = ("begorig", "endorig", "begepi", "endepi", "alo_beg")
WAGE_COLUMNS = ("tentgelt", "tentgelt_bonus", "tentgelt_imp")

# How far a copy moves. Days for the dates, years for the birth year, a share
# of itself for a wage.
DAY_SHIFT = 15
YEAR_SHIFT = 2
WAGE_SHIFT = 0.02

# The keys of the establishment files, which are renumbered, and the column
# that says which calendar year a row belongs to, which is not.
ESTABLISHMENT_KEY = "betnr_siab"
YEAR_COLUMN = "jahr"

# Stata's long is 32 bits, and so is the pipeline's key column. Past this a
# stacked key needs 64 bits, and the script says so rather than wrapping.
INT32_MAX = 2_147_483_647

# What a Stata storage type is in pandas, with and without a missing value in
# the column. pyreadstat's writer has no `byte` or `int` of its own and widens
# every integer it is given to a 32-bit one, so a written file is about half as
# large again as the source, whatever is done here. Handing it the narrow type
# still matters: without it a column that holds one missing value goes out as a
# double or a string.
NUMPY_TYPES = {"int8": "int8", "int16": "int16", "int32": "int32",
               "float": "float32", "double": "float64"}
NULLABLE_TYPES = {"int8": "Int8", "int16": "Int16", "int32": "Int32",
                  "float": "Float32", "double": "Float64"}


# ----------------------------------------------------------------------------
# Reading the source delivery
# ----------------------------------------------------------------------------

def read_stata_frame(path: Path) -> tuple[pl.DataFrame, object]:
    """Read a whole Stata file into polars, keeping its metadata.

    The metadata is what lets a copy be written back out as a Stata file the
    reference can read: the display formats carry which columns are dates, and
    the value labels are most of what makes a delivery readable.
    """
    pandas_frame, meta = pyreadstat.read_dta(str(path))
    return pl.from_pandas(pandas_frame), meta


def stride_for(values: pl.Series) -> int:
    """How far apart two copies' key ranges sit.

    The span of the source keys, rounded up to the next power of ten, so that
    copy `c` occupies `[min + c * stride, max + c * stride]` and the ranges
    neither overlap nor interleave. Rounding keeps the numbers readable: a
    person in copy 3 has the source person's number plus three strides.
    """
    span = int(values.max()) - int(values.min()) + 1
    return int(10 ** np.ceil(np.log10(span)))


# ----------------------------------------------------------------------------
# One copy
# ----------------------------------------------------------------------------

def person_jitter(persons: pl.Series, seed: int, copy: int) -> pl.DataFrame:
    """One day shift and one birth-year shift per person, drawn reproducibly.

    A person's dates all move together, which is what keeps a spell's four
    boundaries in order. The draw depends on the seed and the copy number only,
    so the same call gives the same delivery on any machine.
    """
    generator = np.random.default_rng([seed, copy])
    count = persons.len()
    return pl.DataFrame({
        "persnr": persons,
        "_day_shift": generator.integers(-DAY_SHIFT, DAY_SHIFT + 1, count),
        "_year_shift": generator.integers(-YEAR_SHIFT, YEAR_SHIFT + 1, count),
    }).with_columns(pl.col("_day_shift").cast(pl.Int32),
                    pl.col("_year_shift").cast(pl.Int32))


def copy_of_core(source: pl.DataFrame,
                 copy: int,
                 persnr_stride: int,
                 betnr_stride: int,
                 seed: int,
                 key_dtype: pl.DataType,
                 first_day: object,
                 last_day: object) -> pl.DataFrame:
    """Copy `copy` of the core spell file, in the shape the `orig` table wants.

    The frame arrives already renamed to the pipeline's key names and already
    carrying real dates, because the caller built it the way the read-in builds
    a batch. What happens here is the renumbering and the jitter.
    """
    persons = source.get_column("persnr").unique(maintain_order=True)
    jitter = person_jitter(persons, seed, copy)

    generator = np.random.default_rng([seed, copy, 1])
    wage_factor = pl.Series(
        "_wage_factor",
        generator.uniform(1.0 - WAGE_SHIFT, 1.0 + WAGE_SHIFT, source.height))

    frame = source.join(jitter, on="persnr", how="left")

    shifted_dates = [
        pl.col(name)
        .dt.offset_by(pl.format("{}d", pl.col("_day_shift")))
        .clip(first_day, last_day)
        .alias(name)
        for name in DATE_COLUMNS if name in frame.columns
    ]
    # A wage stays on the grid the delivery reports it on: two decimals. Off
    # that grid the one-time-payment step fails its own assertion, because it
    # recomputes a daily wage from spell earnings and rounds the result to two
    # decimals, which lands below an unrounded original about half the time.
    # The delivery's `tentgelt` and `tentgelt_imp` carry no other values.
    jittered_wages = [
        (0.01 * (pl.col(name) * wage_factor / 0.01).round(0)).alias(name)
        for name in WAGE_COLUMNS if name in frame.columns
    ]

    return (
        frame
        .with_columns(shifted_dates)
        .with_columns(jittered_wages)
        .with_columns(
            (pl.col("persnr").cast(pl.Int64) + copy * persnr_stride)
            .cast(key_dtype).alias("persnr"),
            (pl.col("betnr").cast(pl.Int64) + copy * betnr_stride)
            .cast(key_dtype).alias("betnr"),
            (pl.col("gebjahr") + pl.col("_year_shift")).alias("gebjahr"),
        )
        .drop("_day_shift", "_year_shift")
        .select(source.columns)
    )


def copy_of_establishments(source: pl.DataFrame,
                           copy: int,
                           betnr_stride: int,
                           seed: int,
                           key_dtype: pl.DataType) -> pl.DataFrame:
    """Copy `copy` of one establishment file.

    The key moves with the core file's, so a copy's spells join its own
    establishments. Every other numeric column except the year is moved by up
    to two percent, for the same reason the wages are: N identical copies of a
    column compress to a size no delivery has.
    """
    generator = np.random.default_rng([seed, copy, 2])

    moved = []
    for name, dtype in source.schema.items():
        if name in (ESTABLISHMENT_KEY, YEAR_COLUMN) or not dtype.is_numeric():
            continue
        factor = pl.Series(
            f"_{name}_factor",
            generator.uniform(1.0 - WAGE_SHIFT, 1.0 + WAGE_SHIFT, source.height))
        moved.append((pl.col(name) * factor).cast(dtype).alias(name))

    return (
        source
        .with_columns(moved)
        .with_columns(
            (pl.col(ESTABLISHMENT_KEY).cast(pl.Int64) + copy * betnr_stride)
            .cast(key_dtype).alias(ESTABLISHMENT_KEY))
    )


# ----------------------------------------------------------------------------
# Writing
# ----------------------------------------------------------------------------

def batch_numbers(frame: pl.DataFrame, offset: int) -> tuple[pl.DataFrame, int]:
    """Give every row the `pn_batch` the batched read-in would have given it.

    The read-in cuts the delivery into batches of whole persons and carries the
    batch number as a column, which every later step then has as a coarse
    partition of the persons. A stacked delivery is built rather than read, so
    the number is computed here, with the read-in's own arithmetic: count the
    rows per person, run a total over them, and label each person with
    `total // batch_size + 1`. Gives back the frame and the row count so far.
    """
    runs = (
        frame.select("persnr")
        .with_columns(pl.col("persnr").rle_id().alias("_run"))
        .group_by("_run")
        .agg(pl.len().alias("_n"), pl.col("persnr").first())
        .sort("_run")
        .with_columns((pl.col("_n").cum_sum() + offset).alias("_upto"))
        .with_columns((pl.col("_upto") // BATCH_SIZE + 1)
                      .cast(pl.Int32).alias("pn_batch"))
        .select("persnr", "pn_batch")
    )
    return frame.join(runs, on="persnr", how="left"), offset + frame.height


def delivery_shape(frame: pl.DataFrame, meta) -> pl.DataFrame:
    """Turn a copy back into what a delivery file looks like on disk.

    The inverse of what the read-in does: the pipeline's key names go back to
    the delivery's, a real date goes back to Stata's count of days from 1
    January 1960, and `pn_batch`, which the read-in adds, goes away.
    """
    back = {new: old for old, new in KEY_RENAMES.items()
            if new in frame.columns and old in meta.column_names}
    dates = [
        (pl.col(name).cast(pl.Int32) - STATA_EPOCH_OFFSET).alias(name)
        for name, dtype in frame.schema.items() if dtype == pl.Date
    ]
    return (frame
            .drop("pn_batch", strict=False)
            .with_columns(dates)
            .rename(back)
            .select([name for name in meta.column_names]))


def writable_value_labels(meta, columns: list[str]) -> dict:
    """The source file's value labels, minus the ones Stata cannot be handed.

    A delivery labels its extended missing values too, and pyreadstat reads
    `.a` through `.z` as floating-point keys it then refuses to write back:
    "type of Value n must be int". Those keys are dropped and the ordinary
    labels are kept, so the synthetic delivery reads like the real one
    everywhere a value is a value. Nothing in either arm or in the reference
    branches on a label.
    """
    labels = {}
    for name, mapping in meta.variable_value_labels.items():
        if name not in columns:
            continue
        kept = {int(key): text for key, text in mapping.items()
                if isinstance(key, (int, float))
                and np.isfinite(key) and float(key).is_integer()}
        if kept:
            labels[name] = kept
    return labels


def write_stata(frame: pl.DataFrame, path: Path, meta) -> None:
    """Write a frame as a Stata file carrying the source file's metadata.

    pyreadstat takes a pandas frame and writes the whole thing in one go, so
    this is where a large synthetic delivery is bounded: the peak is the frame
    in pandas plus pyreadstat's own copy of it.
    """
    pandas_frame = frame.to_pandas()

    # Put every column back on the storage type the source file gave it.
    # pyreadstat reads a Stata `byte` that holds one missing value as a pandas
    # object column, and writing that back gives a string variable or a double
    # where the delivery had a one-byte integer. On a stacked delivery that is
    # the difference between a file that is a little larger than the source
    # times the copies and one that is three times that.
    for name in pandas_frame.columns:
        storage = meta.readstat_variable_types.get(name)
        if storage not in NUMPY_TYPES:
            continue
        target = (NULLABLE_TYPES[storage] if pandas_frame[name].isna().any()
                  else NUMPY_TYPES[storage])
        pandas_frame[name] = pandas_frame[name].astype(target)

    path.parent.mkdir(parents=True, exist_ok=True)
    columns = list(pandas_frame.columns)
    pyreadstat.write_dta(
        pandas_frame,
        str(path),
        column_labels={name: meta.column_names_to_labels[name]
                       for name in columns},
        variable_value_labels=writable_value_labels(meta, columns),
        variable_format={name: meta.original_variable_types[name]
                         for name in columns},
    )


def establishment_files(delivery: Path) -> list[str]:
    """Every establishment file of the delivery, in the order they are written.

    The basic file the first merge reads, the yearly panel and the four
    worker-flow modules the annual merge reads. A year the delivery does not
    carry is skipped, which is what both arms do when they read them.
    """
    names = [BASIC_BHP_FILE]
    names += [f"{PREFIX}_bhp_v1_{year}.dta" for year in PANEL_YEARS]
    names += [f"{PREFIX}_bhp_{module}_v1.dta" for module in MODULES]
    return [name for name in names if (delivery / name).exists()]


# ----------------------------------------------------------------------------
# The build
# ----------------------------------------------------------------------------

def build(source: Path,
          copies: int,
          store_target: str | None,
          delivery_out: Path | None,
          core_dta: bool,
          seed: int) -> None:
    core_source = source / CORE_FILE
    if not core_source.exists():
        raise SystemExit(f"No core delivery file at {core_source}")

    print(f"Reading {core_source}")
    raw, meta = read_stata_frame(core_source)

    # The read-in's own cast plan, so the stacked `orig` table holds exactly
    # what a read delivery holds: dates as dates, Stata integers as 32-bit
    # integers, everything else as a double.
    plan = column_types(meta)
    casts = []
    for name, dtype in plan.items():
        column = pl.col(name)
        if raw.schema[name] in (pl.Float32, pl.Float64):
            column = column.fill_nan(None)
        if dtype == pl.Date:
            column = (column.cast(pl.Int32, strict=False)
                      + STATA_EPOCH_OFFSET).cast(pl.Date)
        else:
            column = column.cast(dtype, strict=False)
        casts.append(column.alias(name))
    core = raw.with_columns(casts).rename(
        {old: new for old, new in KEY_RENAMES.items() if old in raw.columns})
    del raw

    persnr_stride = stride_for(core.get_column("persnr"))
    betnr_stride = stride_for(core.get_column("betnr"))
    largest_key = max(
        int(core.get_column("persnr").max()) + (copies - 1) * persnr_stride,
        int(core.get_column("betnr").max()) + (copies - 1) * betnr_stride,
    )
    key_dtype = pl.Int32 if largest_key <= INT32_MAX else pl.Int64
    if key_dtype == pl.Int64:
        print(f"Keys run to {largest_key:,}, past a 32-bit integer, so `persnr` "
              f"and `betnr` are written as 64-bit.")
        if core_dta:
            raise SystemExit(
                "Stata's `long` holds 32 bits, so a delivery with keys this "
                "large has no Stata export. Drop --core-dta, or build a "
                "smaller one.")

    first_day = core.select(pl.min_horizontal(
        [pl.col(name).min() for name in DATE_COLUMNS
         if name in core.columns])).item()
    last_day = core.select(pl.max_horizontal(
        [pl.col(name).max() for name in DATE_COLUMNS
         if name in core.columns])).item()

    print(f"{core.height:,} rows and "
          f"{core.get_column('persnr').n_unique():,} persons per copy, "
          f"{copies} copies, "
          f"{core.height * copies:,} rows in all")
    print(f"Person numbers step by {persnr_stride:,} a copy, establishment "
          f"numbers by {betnr_stride:,}")
    print(f"Dates run {first_day} to {last_day} and stay inside it")

    # ---- the core file, into the store and optionally as Stata files --------
    sink = None
    if store_target is not None:
        store = open_store(store_target)
        if hasattr(store, "path"):
            sink = _ParquetSink(store.path("orig"))
        else:
            sink = _DuckDBSink(store, "orig")

    written = 0
    offset = 0
    for copy in range(copies):
        frame = copy_of_core(core, copy, persnr_stride, betnr_stride, seed,
                             key_dtype, first_day, last_day)
        frame, offset = batch_numbers(frame, offset)

        if sink is not None:
            sink.write(frame)
            written += frame.height

        if core_dta and delivery_out is not None:
            path = delivery_out / "core" / f"{PREFIX}_copy{copy:04d}.dta"
            write_stata(delivery_shape(frame, meta), path, meta)

        print(f"  copy {copy + 1}/{copies}: {frame.height:,} rows")
        del frame

    if sink is not None:
        sink.close()
        print(f"{written:,} rows written to the `orig` table of {store_target}")

    del core

    # ---- the establishment files, always as Stata files ---------------------
    if delivery_out is None:
        return

    for name in establishment_files(source):
        establishments, file_meta = read_stata_frame(source / name)
        estimate = establishments.estimated_size() * copies / 1e9
        print(f"  {name}: {establishments.height:,} rows a copy, "
              f"about {estimate:.1f} GB in memory at {copies} copies")

        stacked = pl.concat([
            copy_of_establishments(establishments, copy, betnr_stride, seed,
                                   key_dtype)
            for copy in range(copies)
        ])
        write_stata(stacked, delivery_out / name, file_meta)
        del establishments, stacked

    print(f"Delivery written to {delivery_out}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stack renumbered copies of the FDZ test delivery.")
    parser.add_argument("--copies", type=int, required=True,
                        help="how many copies of the delivery to stack")
    parser.add_argument("--source", type=Path,
                        default=Path(os.environ.get(
                            "SIAB_TEST_DATA",
                            PROJECT / "local_context" / "testdata"
                            / "siab_7523_v2")),
                        help="the FDZ test delivery to copy")
    parser.add_argument("--store",
                        help="where the `orig` table goes: a .duckdb file, or "
                             "a folder for Parquet tables")
    parser.add_argument("--delivery", type=Path,
                        help="folder for the synthetic delivery's Stata files")
    parser.add_argument("--core-dta", action="store_true",
                        help="also write the core spell file as one Stata file "
                             "per copy, which the Stata arm appends")
    parser.add_argument("--seed", type=int, default=20260918,
                        help="the seed the jitter is drawn from")
    arguments = parser.parse_args()

    if arguments.store is None and arguments.delivery is None:
        raise SystemExit("Nothing to write: pass --store, --delivery, or both.")
    if arguments.core_dta and arguments.delivery is None:
        raise SystemExit("--core-dta needs --delivery to write into.")

    build(arguments.source, arguments.copies, arguments.store,
          arguments.delivery, arguments.core_dta, arguments.seed)


if __name__ == "__main__":
    main()
