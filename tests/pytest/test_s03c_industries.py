"""
Synthetic tests for the 1-digit industry mappings.

The port of tests/testthat/test-03c_industries.R. The step maps the
time-consistent three-digit WZ93 industry `w93_3_gen` onto two one-digit
classifications, following 13_industries_1digit.do: the Statistisches
Bundesamt's 16 categories and the IAB establishment panel's 9. Both mappings
are ranges, so the tests below walk the endpoints of every range, then the gaps
between them. A code in a gap has to come out missing, which is what the
reference's chain of `replace ... if` leaves behind.
"""

import polars as pl
import pytest

from siab.steps import generate_industry_variables

# Both endpoints of every Statistisches Bundesamt range, with the code it gets
DESTATIS_ENDPOINTS: list[tuple[int, int]] = [
    (11, 1), (20, 1),
    (50, 2),
    (101, 3), (145, 3),
    (151, 4), (372, 4),
    (401, 5), (410, 5),
    (451, 6), (455, 6),
    (501, 7), (527, 7),
    (551, 8), (555, 8),
    (601, 9), (642, 9),
    (651, 10), (672, 10),
    (701, 11), (748, 11),
    (751, 12), (753, 12),
    (801, 13), (804, 13),
    (851, 14), (853, 14),
    (900, 15), (930, 15),
    (950, 16), (990, 16),
]

# Both endpoints of every IAB establishment panel range, with the code it gets
ESTPANEL_ENDPOINTS: list[tuple[int, int]] = [
    (11, 1), (50, 1),
    (101, 1), (145, 1),
    (371, 1), (410, 1),
    (900, 1),
    (151, 2), (160, 2),
    (171, 3), (193, 3),
    (221, 3), (223, 3),
    (361, 3), (366, 3),
    (201, 4), (212, 4),
    (231, 4), (287, 4),
    (291, 5), (355, 5),
    (451, 6), (455, 6),
    (551, 7), (555, 7),
    (921, 7), (930, 7),
    (501, 7), (527, 7),
    (601, 8), (634, 8),
    (641, 8), (642, 8),
    (651, 8), (672, 8),
    (701, 8), (703, 8),
    (711, 8), (714, 8),
    (721, 8), (726, 8),
    (731, 8), (744, 8),
    (745, 8), (748, 8),
    (801, 9), (804, 9),
    (851, 9), (853, 9),
    (911, 9), (913, 9),
    (751, 9), (753, 9),
]


def run_on_codes(codes, **kwargs) -> pl.DataFrame:
    """Run the step over one column of three-digit codes, in the order given."""
    frame = pl.LazyFrame(
        {
            "persnr": list(range(1, len(codes) + 1)),
            "w93_3_gen": list(codes),
        },
        schema_overrides={"persnr": pl.Int32, "w93_3_gen": pl.Int32},
    )
    return generate_industry_variables(frame, **kwargs).collect().sort("persnr")


def test_every_statistisches_bundesamt_range_maps_its_endpoints():
    codes = [code for code, _ in DESTATIS_ENDPOINTS]
    expected = [category for _, category in DESTATIS_ENDPOINTS]

    out = run_on_codes(codes)

    assert out["industry1_destatis"].to_list() == expected


def test_every_iab_establishment_panel_range_maps_its_endpoints():
    codes = [code for code, _ in ESTPANEL_ENDPOINTS]
    expected = [category for _, category in ESTPANEL_ENDPOINTS]

    out = run_on_codes(codes)

    assert out["industry1_estpanel"].to_list() == expected


def test_a_code_between_two_statistisches_bundesamt_ranges_is_missing():
    # One code out of each gap in the 16 ranges, plus one below the first range
    # and one above the last.
    gaps = [10, 21, 49, 51, 100, 146, 150, 373, 400, 411, 450,
            456, 500, 528, 550, 556, 600, 643, 650, 673, 700,
            749, 750, 754, 800, 805, 850, 854, 899, 931, 949, 991]

    out = run_on_codes(gaps)

    assert out["industry1_destatis"].null_count() == len(gaps)


def test_a_code_between_two_iab_establishment_panel_ranges_is_missing():
    # 194 to 200 and 288 to 290 sit inside manufacturing and still get nothing:
    # the establishment panel mapping covers less of the classification than the
    # Statistisches Bundesamt one does.
    gaps = [10, 51, 100, 146, 150, 161, 170, 194, 200, 213, 220,
            224, 230, 288, 290, 356, 360, 367, 370, 411, 450, 456,
            500, 528, 550, 556, 600, 635, 640, 643, 650, 673, 700,
            704, 710, 715, 720, 727, 730, 749, 750, 754, 800, 805,
            850, 854, 899, 901, 910, 914, 920, 931, 990]

    out = run_on_codes(gaps)

    assert out["industry1_estpanel"].null_count() == len(gaps)


def test_a_missing_industry_stays_missing_in_both_classifications():
    # Stata treats missing as larger than any number, so `w93_3_gen >= 11 &
    # w93_3_gen <= 20` is false for it and the reference leaves the category
    # missing. A negative special code falls in no range for the same reason.
    out = run_on_codes([None, -9, -7])

    assert out["industry1_destatis"].null_count() == 3
    assert out["industry1_estpanel"].null_count() == 3


def test_mappings_picks_which_classification_is_built():
    # 00_master_SIAB.do gates each mapping behind its own macro, `destatis` and
    # `estpanel`, and runs the step when either is switched on.
    destatis_only = run_on_codes([101, 551], mappings=("destatis",))
    estpanel_only = run_on_codes([101, 551], mappings=("estpanel",))

    assert "industry1_destatis" in destatis_only.columns
    assert "industry1_estpanel" not in destatis_only.columns

    assert "industry1_estpanel" in estpanel_only.columns
    assert "industry1_destatis" not in estpanel_only.columns


def test_an_unknown_mapping_name_is_refused():
    # The R arm validates the argument with `match.arg(several.ok = TRUE)`,
    # which refuses a name outside the two and an empty selection alike.
    with pytest.raises(ValueError, match="mappings"):
        run_on_codes([101], mappings=("blossfeld",))

    with pytest.raises(ValueError, match="mappings"):
        run_on_codes([101], mappings=())


def test_the_step_stops_when_w93_3_gen_is_not_there():
    # w93_3_gen arrives with merge_basic_bhp(). Without it the mappings would
    # silently produce nothing, so the step refuses instead.
    frame = pl.LazyFrame(
        {"persnr": [1, 2], "beruf": [11, 12]},
        schema_overrides={"persnr": pl.Int32, "beruf": pl.Int32},
    )

    with pytest.raises(ValueError, match="w93_3_gen"):
        generate_industry_variables(frame)
