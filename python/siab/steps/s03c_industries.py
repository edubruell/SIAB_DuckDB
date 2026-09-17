"""
03c.) Generate 1-digit industry variables from the 3-digit industry (w93_3_gen)

Port of 13_industries_1digit.do. Both mappings are ranges over the WZ93
three-digit code, spelled out in the reference and copied here unchanged.

Generates the variables:
  - industry1_destatis: Industry; 1-digit; Statistisches Bundesamt (2002);
    based on w93_3_gen
  - industry1_estpanel: Industry; 1-digit; IAB establishment panel;
    based on w93_3_gen

Notes:
  w93_3_gen arrives with merge_basic_bhp() and is already coded
time-consistently, see Eberle et al. (2014). A code that falls in no range keeps
a missing industry, which is what the reference's chain of `replace ... if`
leaves behind as well. A missing w93_3_gen lands there too: Stata treats missing
as larger than any number, so `w93_3_gen >= 11 & w93_3_gen <= 20` is false for
it, and polars, whose null comparisons yield null rather than true, sends the
same row down the same final branch.

  The reference gates each mapping behind a macro, `destatis` and `estpanel`,
and runs the step when either is switched on. The `mappings` argument is that
switch: a classification the caller does not ask for is not built, and its
column is not in the frame that comes back.

  The reference attaches value labels to both columns, in German and in English.
Neither polars nor DuckDB carries a value label, so the labels sit in the tables
below and are written to the log, which is what the R port does as well. Unlike
the R port this step logs no per-category case counts: counting would force the
frame to be materialised in the middle of a step that is otherwise entirely
lazy, and DuckDB owns the table between steps anyway.

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Wolfgang Dauth,
Johann Eppelsheimer and Heiko Stüber

Version: 1.0
Created: 2026-09-17

References:
  Eberle, J., P. Jacobebbinghaus, J. Ludsteck, and J. Witter (2014):
  Generation of time-consistent industry codes in the face of classification
  changes. FDZ-Methodenreport 05/2011.
  Statistisches Bundesamt (2002): Klassifikation der Wirtschaftszweige,
  Ausgabe 1993.
"""

from __future__ import annotations

import os
from typing import Sequence

import polars as pl

from siab.common import step_logger

__all__ = ["generate_industry_variables"]

# The two classifications the step can build, in the reference's own order
MAPPING_CHOICES: tuple[str, ...] = ("destatis", "estpanel")


# The 16 categories of the Statistisches Bundesamt mapping, as (code, German
# label, English label). The German labels are the reference's own, spelling
# and abbreviations included.
INDUSTRY1_DESTATIS_LABELS: tuple[tuple[int, str, str], ...] = (
    (1,  "Land- u. Forstwirtschaft",                                  "Agriculture a. forestry"),
    (2,  "Fischerei u. Fischzucht",                                   "Fishing a. fish farming"),
    (3,  "Bergbau u. Gewinnung v. Steinen u. Erden",                  "Mining and sector"),
    (4,  "Verarbeitendes Gewerbe",                                    "Manufacturing industry"),
    (5,  "Energie- u. Wasserversorgung",                              "Energy and water supply, recycling"),
    (6,  "Baugewerbe",                                                "Construction"),
    (7,  "Handel; Instandhaltung u. Reparatur v. KFZ u. Geb.guetern", "Wholesale and retail trade; repair of motor vehicles, motorcycles and personal and household goods"),
    (8,  "Gastgewerbe",                                               "Hospitality"),
    (9,  "Verkehr und Nachrichtenuebermittlung",                      "Transport, storage, communication"),
    (10, "Kredit- u. Vers.gewerbe",                                   "Finance and insurance"),
    (11, "Grunds.- u. Wohn.wesen, Vermietung bewgl. Sachen, Dienstl. f. Untern.", "Real estate and housing, renting of movable property, business related services"),
    (12, "Oeff. Verw., Verteid., Soz.vers.",                          "Public administration, defense, social security"),
    (13, "Erziehung u. Unterricht",                                   "Education"),
    (14, "Gesundsh.-, Vet.- u. Soz.wesen",                            "Healthcare and social services"),
    (15, "Sonstige Dienstl.",                                         "Other services"),
    (16, "Private Haushalte",                                         "Private households"),
)

# The 9 categories of the IAB establishment panel mapping
INDUSTRY1_ESTPANEL_LABELS: tuple[tuple[int, str, str], ...] = (
    (1, "Land- und Forstwirtschaft, Fischerei", "Agriculture, forestry and fishing"),
    (2, "Nahrungs- und Genussmittel",           "Food and beverages"),
    (3, "Verbrauchsgueter",                     "Consumables"),
    (4, "Produktionsgueter",                    "Production goods"),
    (5, "Investitions- und Gebrauchsgueter",    "Capital and consumer goods"),
    (6, "Baugewerbe",                           "Construction"),
    (7, "Gastgewerbe",                          "Hospitality"),
    (8, "Verkehr und Lagerei",                  "Transport and storage"),
    (9, "Erziehung und Unterricht",             "Education"),
)

# The Statistisches Bundesamt ranges, as (lower, upper, code), in the order the
# reference writes its `replace` lines.
DESTATIS_RANGES: tuple[tuple[int, int, int], ...] = (
    (11,   20,  1),
    (50,   50,  2),
    (101, 145,  3),
    (151, 372,  4),
    (401, 410,  5),
    (451, 455,  6),
    (501, 527,  7),
    (551, 555,  8),
    (601, 642,  9),
    (651, 672, 10),
    (701, 748, 11),
    (751, 753, 12),
    (801, 804, 13),
    (851, 853, 14),
    (900, 930, 15),
    (950, 990, 16),
)

# The IAB establishment panel ranges. The reference writes this mapping as nine
# groups of `replace` lines, several of them made of ranges that are far apart
# in the classification. The ranges do not overlap, so the chain below
# reproduces them in one pass.
ESTPANEL_RANGES: tuple[tuple[int, int, int], ...] = (
    # Agriculture, hunting and forestry, fishing
    (11,   50, 1),
    # Mining and quarrying, energy and water supply, recycling
    (101, 145, 1),
    (371, 410, 1),
    (900, 900, 1),
    # Manufacture of food products, beverages and tobacco
    (151, 160, 2),
    # Manufacture of consumer products
    (171, 193, 3),
    (221, 223, 3),
    (361, 366, 3),
    # Manufacture of industrial goods
    (201, 212, 4),
    (231, 287, 4),
    # Manufacture of capital and consumer goods
    (291, 355, 5),
    # Construction
    (451, 455, 6),
    # Hotels and restaurants, personal services, trade and repair
    (551, 555, 7),
    (921, 930, 7),
    (501, 527, 7),
    # Transport and storage, information, finance, real estate, renting, IT,
    # liberal professions and other business related services
    (601, 634, 8),
    (641, 642, 8),
    (651, 672, 8),
    (701, 703, 8),
    (711, 714, 8),
    (721, 726, 8),
    (731, 744, 8),
    (745, 748, 8),
    # Education, health and social work, organizations, public administration
    (801, 804, 9),
    (851, 853, 9),
    (911, 913, 9),
    (751, 753, 9),
)


def _range_mapping(source: str,
                   ranges: Sequence[tuple[int, int, int]]) -> pl.Expr:
    """Turn a table of (lower, upper, code) rows into one chained expression.

    The reference builds each classification as a run of `replace ... if x >= lo
    & x <= hi`, where a later line overwrites an earlier one. The ranges never
    overlap, so a first-match chain lands on the same code, and the closing
    `otherwise` is the `gen ... = .` the reference starts from: a code in no
    range, and a missing code, keep a missing category.
    """
    expr = None
    for lower, upper, code in ranges:
        condition = (pl.col(source) >= lower) & (pl.col(source) <= upper)
        branch = pl.when(condition).then(pl.lit(code, dtype=pl.Int32))
        expr = branch if expr is None else expr.when(condition).then(pl.lit(code, dtype=pl.Int32))
    return expr.otherwise(pl.lit(None, dtype=pl.Int32))


def generate_industry_variables(frame: pl.LazyFrame,
                                mappings: Sequence[str] = ("destatis", "estpanel"),
                                log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    # The R arm validates the argument with `match.arg(..., several.ok = TRUE)`,
    # which refuses anything outside the two names and an empty selection, and
    # keeps the caller's order. Both messages are that function's own, put into
    # English: R prints them in the session locale, so the R text itself is not
    # something a test could match on.
    mappings = list(mappings)
    if not mappings:
        raise ValueError("'mappings' must be of length >= 1")
    unknown = [m for m in mappings if m not in MAPPING_CHOICES]
    if unknown:
        raise ValueError(
            "'mappings' should be one of "
            + ", ".join(f'"{choice}"' for choice in MAPPING_CHOICES)
        )

    log = step_logger("industries", log_file)
    log.info("Industry variable script started")

    # w93_3_gen arrives with merge_basic_bhp(). Without it the mappings would
    # silently produce nothing, so the step refuses instead. Reading the schema
    # resolves the column names without collecting any data.
    if "w93_3_gen" not in frame.collect_schema().names():
        raise ValueError(
            "generate_industry_variables() needs w93_3_gen, "
            "which merge_basic_bhp() brings in"
        )

    # Each column reads w93_3_gen and neither reads the other, so the ones the
    # caller asked for are built together in one pass.
    columns: dict[str, pl.Expr] = {}

    # ==================================================================
    #  Statistisches Bundesamt (2002)
    # ==================================================================
    if "destatis" in mappings:
        log.info("Mapping w93_3_gen to the Statistisches Bundesamt 1-digit industry")
        for code, label_de, label_en in INDUSTRY1_DESTATIS_LABELS:
            log.info(f"industry1_destatis = {code} ({label_en}; {label_de})")
        columns["industry1_destatis"] = _range_mapping("w93_3_gen", DESTATIS_RANGES)

    # ==================================================================
    #  IAB establishment panel
    # ==================================================================
    if "estpanel" in mappings:
        log.info("Mapping w93_3_gen to the IAB establishment panel 1-digit industry")
        for code, label_de, label_en in INDUSTRY1_ESTPANEL_LABELS:
            log.info(f"industry1_estpanel = {code} ({label_en}; {label_de})")
        columns["industry1_estpanel"] = _range_mapping("w93_3_gen", ESTPANEL_RANGES)

    frame = frame.with_columns(**columns)

    log.info("Industry variables generated")
    return frame
