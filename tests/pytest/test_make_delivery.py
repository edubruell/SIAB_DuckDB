"""
Synthetic tests for the benchmark delivery generator's Stata writer.

`benchmark/make_delivery.py` stacks renumbered copies of the FDZ test delivery
and writes them back as Stata files. How wide those files are decides what the
whole sweep measures: a fixture twice the source's width makes every disk and
memory figure twice a real delivery's, and the Stata arm run out of memory at a
size it would otherwise reach.

The writer is pandas' rather than pyreadstat's for that reason, and it needs one
thing put back afterwards, the display format that says a column is a date.
These tests cover both, on a small table written here rather than on the FDZ
delivery, so they run without it.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pyreadstat
import pytest

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent.parent
sys.path.insert(0, str(PROJECT / "benchmark"))

from make_delivery import (  # noqa: E402
    date_columns,
    mark_dates,
    write_stata,
)
from stata_to_db_batch_read import (  # noqa: E402
    column_types,
    read_batch,
    stata_metadata,
)

# The delivery's own format for its five date columns. Stata reads a date off
# the format and nothing else, and pyreadstat converts a column it recognises
# as a date and leaves one it does not, which is why the exact string matters.
DELIVERY_DATE_FORMAT = "%tdD_m_CY"

# 1 January 1992 as Stata counts days, from 1 January 1960.
DAY_1992_01_01 = 11688

# What a row of a file costs, by what its header says each column is stored as.
# The file size itself says nothing on a table this small, where the labels and
# the header are most of it.
STORAGE_WIDTHS = {"int8": 1, "int16": 2, "int32": 4, "float": 4, "double": 8}


def row_width(path: Path) -> int:
    _, meta = pyreadstat.read_dta(str(path), row_limit=1)
    return sum(STORAGE_WIDTHS[storage]
               for storage in meta.readstat_variable_types.values())


def set_format(path: Path, columns: list[str], name: str, display: str) -> None:
    """Overwrite one variable's display format in a written .dta header."""
    data = path.read_bytes()
    start = data.index(b"<formats>") + len(b"<formats>")
    width = (data.index(b"</formats>") - start) // len(columns)
    slot = start + columns.index(name) * width
    path.write_bytes(data[:slot]
                     + display.encode("ascii").ljust(width, b"\x00")
                     + data[slot + width:])


@pytest.fixture
def source(tmp_path):
    """A stand-in delivery, three rows wide enough to show every storage type.

    Gives back the file, its metadata and the frame the generator would hand
    the writer: keys renamed back, dates as Stata's own count of days.
    """
    frame = pd.DataFrame({
        "persnr_siab": np.array([1, 1, 2], dtype="int32"),
        "spell": np.array([1, 2, 1], dtype="int8"),
        "gebjahr": np.array([1955, 1955, 1972], dtype="int16"),
        "quelle": pd.array([1, None, 2], dtype="Int8"),
        "begepi": np.array([DAY_1992_01_01, DAY_1992_01_01 + 366,
                            DAY_1992_01_01], dtype="int16"),
        "tentgelt": np.array([100.5, 110.25, 50.0], dtype="float64"),
    })
    path = tmp_path / "source.dta"
    frame.to_stata(str(path), write_index=False, version=118,
                   variable_labels={"spell": "Satzzaehler pro Konto"},
                   value_labels={"quelle": {1: "BeH", 2: "LEH"}})
    set_format(path, list(frame.columns), "begepi", DELIVERY_DATE_FORMAT)

    _, meta = pyreadstat.read_dta(str(path), row_limit=1)
    return path, meta, pl.from_pandas(frame)


# ======================================================================
#  The pieces
# ======================================================================

def test_a_date_column_is_found_by_its_display_format(source):
    _, meta, frame = source

    assert date_columns(meta, list(frame.columns)) == {
        "begepi": DELIVERY_DATE_FORMAT}


def test_a_file_with_no_date_column_is_left_alone(tmp_path):
    path = tmp_path / "plain.dta"
    pd.DataFrame({"a": np.array([1, 2], dtype="int8")}).to_stata(
        str(path), write_index=False, version=118)
    before = path.read_bytes()

    mark_dates(path, ["a"], {})

    assert path.read_bytes() == before


def test_a_format_too_long_for_its_slot_is_refused(source):
    path, _, frame = source

    with pytest.raises(ValueError, match="does not fit"):
        mark_dates(path, list(frame.columns), {"begepi": "%" + "t" * 80})


def test_a_column_the_file_does_not_carry_is_refused(source):
    path, _, frame = source

    with pytest.raises(ValueError):
        mark_dates(path, list(frame.columns), {"nosuchcolumn": "%td"})


# ======================================================================
#  The writer
# ======================================================================

def test_every_column_goes_out_at_the_width_it_came_in(source, tmp_path):
    path, meta, frame = source
    written = tmp_path / "written.dta"

    write_stata(frame, written, meta)

    _, out = pyreadstat.read_dta(str(written), row_limit=1)
    assert out.readstat_variable_types == meta.readstat_variable_types


def test_a_written_row_is_no_wider_than_the_row_it_copies(source, tmp_path):
    """The whole point of the writer, in one assertion.

    pyreadstat widens every integer to 32 bits, which made a generated core
    file 196 bytes a row against the source delivery's 95.
    """
    path, meta, frame = source
    written = tmp_path / "written.dta"
    fat = tmp_path / "fat.dta"

    write_stata(frame, written, meta)
    pyreadstat.write_dta(frame.to_pandas(), str(fat))

    assert row_width(written) == row_width(path)
    assert row_width(written) < row_width(fat)


def test_a_date_keeps_the_delivery_s_own_format_rather_than_a_plain_one(
        source, tmp_path):
    """`%td` and `%tdD_m_CY` are not interchangeable here.

    pyreadstat converts a column it recognises as a date into timestamps on the
    way in and leaves one it does not alone. It recognises `%td`, so a file
    marked with it comes back through the read-in shifted by the 3,653 days
    between Stata's epoch and polars'.
    """
    path, meta, frame = source
    written = tmp_path / "written.dta"

    write_stata(frame, written, meta)

    _, out = pyreadstat.read_dta(str(written), row_limit=1)
    assert out.original_variable_types["begepi"] == DELIVERY_DATE_FORMAT


def test_a_written_delivery_reads_back_as_the_days_it_was_given(source,
                                                               tmp_path):
    path, meta, frame = source
    written = tmp_path / "written.dta"

    write_stata(frame, written, meta)
    read_back = read_batch(written, 0, frame.height,
                           column_types(stata_metadata(written)), 1)

    assert read_back["begepi"].to_list() == [dt.date(1992, 1, 1),
                                             dt.date(1993, 1, 1),
                                             dt.date(1992, 1, 1)]
    assert read_back["spell"].to_list() == [1, 2, 1]
    assert read_back["quelle"].to_list() == [1, None, 2]
    assert read_back["tentgelt"].to_list() == [100.5, 110.25, 50.0]


def test_the_labels_a_delivery_carries_survive_the_write(source, tmp_path):
    path, meta, frame = source
    written = tmp_path / "written.dta"

    write_stata(frame, written, meta)

    _, out = pyreadstat.read_dta(str(written), row_limit=1)
    assert out.column_names_to_labels["spell"] == "Satzzaehler pro Konto"
    assert out.variable_value_labels["quelle"] == {1: "BeH", 2: "LEH"}
