"""03_SIAB_bio.do against the Python port, one test per generated column."""

import pytest

from conftest import assert_column_matches

STEP = "03_SIAB_bio"

BIO_COLUMNS = [
    "azubi",
    "ein_erw",
    "tage_erw",
    "ein_bet",
    "tage_bet",
    "ein_job",
    "tage_job",
    "anz_lst",
    "tage_lst",
]


@pytest.mark.parametrize("column", BIO_COLUMNS)
def test_bio_column_matches(reference_query, column):
    assert_column_matches(reference_query(STEP), column)


def test_establishment_and_job_blanks_agree(reference_query):
    """Where the reference leaves ein_bet empty, all four columns are empty."""
    query = reference_query(STEP)
    disagreeing = query(
        "SELECT count(*) AS n "
        "FROM stata JOIN py ON stata.persnr = py.persnr "
        "                  AND stata.spell = py.spell "
        "                  AND stata.begepi = py.begepi "
        "WHERE stata.ein_bet IS NULL AND ("
        "  py.ein_bet IS NOT NULL OR py.tage_bet IS NOT NULL OR "
        "  py.ein_job IS NOT NULL OR py.tage_job IS NOT NULL)"
    )["n"][0]
    assert disagreeing == 0
