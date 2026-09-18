"""
02.) Generate additional biographic variables from longitudinal data

Port of 03_SIAB_bio.do.

First day in employment (ein_erw):
  - Date of entry into first employment
  - Periods of vocational training are ignored (erwstat = 102, 121, 122, 141)
    --> Missing for persons continuously in vocational training in SIAB
  - Entry into first employment can be later than entry in first establishment
    or job, since the latter variables include periods of vocational training

Number of days in employment (tage_erw):
  - Total number of days a person was employed until the end of the observation
  - Periods of vocational training are ignored
    --> Value 0 for persons who are in vocational training throughout

First day in establishment (ein_bet):
  - Date of entry into establishment, including vocational training
  - Not affected by interruptions of employment

Number of days in establishment (tage_bet):
  - Number of days in establishment, gaps are subtracted

First day in job (ein_job):
  - Start date of current job. Vocational training is a separate job.
  - Re-employment in the same establishment is a new job if
    a) the reason of notification implies end of employment
       (grund = 130, 134, 140, 149) and the gap is > 92 days, or
    b) any other reason of notification and the gap is > 366 days

Number of days in job (tage_job):
  - Number of days in the current job, see ein_job

Number of benefit receipts (anz_lst):
  - Receipts according to SGB II or SGB III, sources LeH and LHG
  - Gaps are ignored if the interruption is < 10 days
  - A change of benefit type does not count as a new receipt

Number of days with benefit receipt (tage_lst):
  - Duration of benefit receipts, gaps are subtracted

Author(s): Eduard Brüll
Python/polars reimplementation of the original procedure by Johanna Eberle and
Alexandra Schmucker

Version: 1.0
Created: 2026-09-17
"""

from __future__ import annotations

import os

import polars as pl

from siab.common import assert_empty, pl_days, step_logger

__all__ = ["generate_biographic_variables"]

AZUBI_ERWSTAT = [102, 121, 122, 141]
JOB_ENDING_GRUND = [130, 134, 140, 149]
BENEFIT_QUELLE = [2, 3]


def _episode_days(frame_end: str = "endepi", frame_start: str = "begepi") -> pl.Expr:
    return pl_days(pl.col(frame_end), pl.col(frame_start)) + 1


def generate_biographic_variables(frame: pl.LazyFrame,
                                  log_file: str | os.PathLike | None = None) -> pl.LazyFrame:
    log = step_logger("siab_bio", log_file)
    log.info("Script to generate additional biographic variables started")

    # Every ordering below carries `spell` or `begepi` as a last key rather than
    # `spell` alone. 01_split_episodes.do cuts a spell that runs over a year
    # boundary into one row per year and gives the pieces the same spell number,
    # so spell stopped identifying a row at the previous step. The reference's
    # sort lines carry the same tie-breaks, added in
    # tests/fixtures/make_fixtures.do and marked `// TIE-BREAK ADDED`.

    # ---------------------------------------#
    # Add OBSERVATION COUNTER                #
    # ---------------------------------------#
    # bysort persnr begepi quelle (spell): gen byte level1 = _n-1
    # bysort persnr begepi (spell):        gen byte level2 = _n-1
    log.info("Adding level1 and level2 observation counters")

    frame = frame.with_columns(
        level1=(pl.col("spell").rank("ordinal").over(["persnr", "begepi", "quelle"]) - 1)
        .cast(pl.Int32),
        level2=(pl.col("spell").rank("ordinal").over(["persnr", "begepi"]) - 1)
        .cast(pl.Int32),
    )

    # assert !missing(level1, level2)
    assert_empty(frame,
                 pl.col("level1").is_null() | pl.col("level2").is_null(),
                 "Missings found in the observation counters")
    log.info("-> ASSERT: No missings in observation counters")

    # ---------------------------------------#
    # FIRST DAY IN EMPLOYMENT  (ein_erw)     #
    # ---------------------------------------#
    # gen byte azubi = inlist(erwstat, 102, 121, 122, 141) & grund != 154
    # gen byte emp   = 1 if azubi != 1 & quelle == 1 & grund != 154
    #
    # Both conditions have to survive a missing value the way Stata reads one.
    # inlist() on a missing erwstat is 0, not missing, and `grund != 154` is
    # true when grund is missing, so fill_null() stands in for both. Dropping
    # the second test is not an option even though 02_grund154.do has already
    # removed every grund == 154 spell: the reference keeps it, and so does this.
    #
    # emp is 1 or null, never 0. That is what makes ein_erw work: a person's
    # first employment is the smallest begorig over the emp == 1 rows, which
    # Stata reaches with begorig[1] inside the group and then carries to every
    # row of the person with egen max().
    log.info("Creating emp, azubi and ein_erw variables")

    azubi = (
        pl.col("erwstat").fill_null(-1).is_in(AZUBI_ERWSTAT)
        & (pl.col("grund").fill_null(-1) != 154)
    ).cast(pl.Int32)

    frame = frame.with_columns(azubi=azubi).with_columns(
        emp=pl.when(
            (pl.col("azubi") != 1)
            & (pl.col("quelle").fill_null(-1) == 1)
            & (pl.col("grund").fill_null(-1) != 154)
        )
        .then(pl.lit(1, dtype=pl.Int32))
        .otherwise(None)
    ).with_columns(
        ein_erw=pl.when(pl.col("emp") == 1)
        .then(pl.col("begorig"))
        .otherwise(None)
        .min()
        .over("persnr")
    )

    # -----------------------------------------#
    # NUMBER OF DAYS IN EMPLOYMENT (tage_erw)  #
    # -----------------------------------------#
    # bysort persnr emp begepi (spell): gen byte nrE = _n if emp == 1
    # gen int d = endepi - begepi + 1 if nrE == 1
    # bysort persnr (begepi nrE spell): gen int tage_erw = sum(d)
    #
    # Only the first spell of an episode contributes its length, so parallel
    # episodes are counted once. nrE is missing outside employment, which sorts
    # last in Stata and is asked for explicitly here.
    log.info("Computing number of days in employment")

    frame = frame.with_columns(
        nrE=pl.when(pl.col("emp") == 1)
        .then(pl.col("spell").rank("ordinal").over(["persnr", "emp", "begepi"]))
        .otherwise(None)
        .cast(pl.Int32)
    ).with_columns(
        d=pl.when(pl.col("nrE") == 1).then(_episode_days()).otherwise(0).cast(pl.Int32)
    )

    frame = frame.sort(
        ["persnr", "begepi", "nrE", "spell"], nulls_last=True, maintain_order=True
    ).with_columns(
        tage_erw=pl.col("d").cum_sum().over("persnr").cast(pl.Int32)
    ).drop("d", "emp", "nrE")

    # ---------------------------------------------------------------------#
    # FIRST DAY (ein_bet) and NUMBER OF DAYS IN ESTABLISHMENT (tage_bet)   #
    # ---------------------------------------------------------------------#
    # SIAB 7523 v2 carries a real establishment identifier, so the grouping is
    # by establishment as in the reference.
    #
    # ein_bet is the smallest begorig, the start of the unsplit spell, over the
    # employment rows of the person and establishment. begepi would be the split
    # episode's own start, which is later whenever the spell crossed a new year.
    log.info("Computing first day and number of days in establishment")

    frame = frame.with_columns(
        emp2=pl.when(
            (pl.col("quelle").fill_null(-1) == 1)
            & pl.col("betnr").is_not_null()
            & (pl.col("grund").fill_null(-1) != 154)
        )
        .then(pl.lit(1, dtype=pl.Int32))
        .otherwise(None)
    ).with_columns(
        ein_bet=pl.when(pl.col("emp2") == 1)
        .then(pl.col("begorig"))
        .otherwise(None)
        .min()
        .over(["persnr", "betnr"]),
        # bysort persnr betnr begepi endepi (spell): gen byte nrB = _n if ...
        # `_n` counts every row of the group; the `if` only decides which rows
        # keep the number.
        nrB=pl.when(
            pl.col("betnr").is_not_null() & (pl.col("grund").fill_null(-1) != 154)
        )
        .then(pl.col("spell").rank("ordinal").over(["persnr", "betnr", "begepi", "endepi"]))
        .otherwise(None)
        .cast(pl.Int32),
    ).with_columns(
        dauer=pl.when(pl.col("nrB") == 1).then(_episode_days()).otherwise(0).cast(pl.Int32)
    )

    frame = frame.sort(
        ["persnr", "betnr", "spell", "begepi"], nulls_last=True, maintain_order=True
    ).with_columns(
        tage_bet=pl.when(pl.col("betnr").is_null())
        .then(None)
        .otherwise(pl.col("dauer").cum_sum().over(["persnr", "betnr"]))
        .cast(pl.Int32)
    ).drop("emp2", "nrB", "dauer")

    # -----------------------------------------------------------------#
    # FIRST DAY IN JOB (ein_job) AND NUMBER OF DAYS IN JOB (tage_job)   #
    # -----------------------------------------------------------------#
    # The reference works this block out row by row over the data sorted by
    # person, apprenticeship, establishment, spell and episode start:
    #
    #   gen byte job = 1 if persnr == persnr[_n-1] & betnr == betnr[_n-1] &
    #                       azubi == azubi[_n-1] & !missing(betnr)
    #
    # In that sort order the test is just "this is not the first row of its
    # person-apprenticeship-establishment group". A job then ends where job is
    # missing, so the runs of job == 1 are the jobs, and a cumulative count of
    # the breaks names them. ein_job is the begepi of the row that opens the
    # run, and tage_job the running duration inside it; the reference reaches
    # both by copying from the previous row, which is a running total in
    # disguise.
    #
    # grund levels, following the reference: inlist(grund[1], 130, 134, 140, 149)
    #   130: Deregistration due to end of employment
    #   134: Deregistration due to interruption of more than one month
    #   140: Simultaneous registration and deregistration, end of employment
    #   149: Deregistration due to death
    log.info("Computing first day and number of days in job")

    job_group = ["persnr", "azubi", "betnr"]

    frame = frame.sort(
        ["persnr", "azubi", "betnr", "spell", "begepi"], nulls_last=True, maintain_order=True
    ).with_columns(
        # bysort persnr azubi betnr begepi (spell): gen byte end = 1 if
        #   inlist(grund[1], 130, 134, 140, 149)
        end=pl.col("grund")
        .sort_by("spell")
        .first()
        .over(["persnr", "azubi", "betnr", "begepi"])
        .is_in(JOB_ENDING_GRUND)
        .fill_null(False)
        .cast(pl.Int32),
        # bysort persnr azubi betnr begepi (spell): gen byte nrA = _n if !missing(betnr)
        nrA=pl.when(pl.col("betnr").is_not_null())
        .then(pl.col("spell").rank("ordinal").over(["persnr", "azubi", "betnr", "begepi"]))
        .otherwise(None)
        .cast(pl.Int32),
        job=pl.when(
            pl.col("betnr").is_not_null()
            & (pl.int_range(pl.len()).over(job_group) > 0)
        )
        .then(pl.lit(1, dtype=pl.Int32))
        .otherwise(None),
    ).with_columns(
        # gen int gap = begepi - endepi[_n-1] - 1 if job == 1
        gap=pl.when(pl.col("job") == 1)
        .then(pl_days(pl.col("begepi"), pl.col("endepi").shift(1).over(job_group)) - 1)
        .otherwise(None)
        .cast(pl.Int32),
    ).with_columns(
        # replace job = . if end[_n-1] == 1 & gap > 92
        # replace job = . if gap > 366
        job=pl.when(
            (
                ((pl.col("end").shift(1).over(job_group).fill_null(0) == 1)
                 & (pl.col("gap") > 92))
                | (pl.col("gap") > 366)
            ).fill_null(False)
        )
        .then(None)
        .otherwise(pl.col("job"))
    ).with_columns(
        # Runs of job == 1 are the jobs.
        job_run=pl.col("job").is_null().cast(pl.Int32).cum_sum().over(job_group),
        jobdauer=pl.when(pl.col("betnr").is_not_null())
        .then(_episode_days())
        .otherwise(None)
        .cast(pl.Int32),
    ).with_columns(
        # The row that opens a job contributes its own length; a later row
        # contributes its length only when it is the episode's first spell, so
        # parallel episodes in the same job are not counted twice. That is the
        # reference's jobdauer - jobdauer_dup, read forwards.
        jobdauer_add=pl.when(pl.col("betnr").is_null())
        .then(None)
        .when(pl.col("job").is_null())
        .then(pl.col("jobdauer"))
        .when(pl.col("nrA") == 1)
        .then(pl.col("jobdauer"))
        .otherwise(0)
        .cast(pl.Int32)
    ).with_columns(
        ein_job=pl.when(pl.col("betnr").is_null())
        .then(None)
        .otherwise(pl.col("begepi").first().over(job_group + ["job_run"])),
        tage_job=pl.when(pl.col("betnr").is_null())
        .then(None)
        .otherwise(
            pl.col("jobdauer_add").fill_null(0).cum_sum().over(job_group + ["job_run"])
        )
        .cast(pl.Int32),
    ).drop("end", "nrA", "job", "gap", "job_run", "jobdauer", "jobdauer_add")

    # --------------------------------------#
    # NUMBER OF BENEFIT RECEIPTS (anz_lst)  #
    # --------------------------------------#
    # nrL is missing outside a benefit episode, exactly as in the reference. The
    # tage_lst block below relies on that: it sorts on nrL and needs the
    # non-benefit rows to land last.
    #
    # A receipt counts as new when more than 10 days have passed since the end
    # of the last one. Where there is no last one the difference is missing, and
    # a Stata missing is larger than 10, so the first receipt of a person always
    # counts. That is the ende_vor.is_null() branch.
    log.info("Computing number of benefit receipts")

    frame = frame.with_columns(
        quelleL=pl.col("quelle").fill_null(-1).is_in(BENEFIT_QUELLE).cast(pl.Int32)
    )

    # bysort persnr begepi quelleL (quelle spell): gen byte nrL = _n if quelleL
    frame = frame.sort(
        ["persnr", "begepi", "quelleL", "quelle", "spell"], nulls_last=True, maintain_order=True
    ).with_columns(
        nrL=pl.when(pl.col("quelleL") == 1)
        .then(pl.int_range(1, pl.len() + 1).over(["persnr", "begepi", "quelleL"]))
        .otherwise(None)
        .cast(pl.Int32)
    )

    # sort persnr spell begepi
    # replace ende_vor = endepi[_n-1] if quelleL[_n-1] & persnr == persnr[_n-1]
    # replace ende_vor = ende_vor[_n-1] if missing(ende_vor) & persnr == persnr[_n-1]
    frame = frame.sort(
        ["persnr", "spell", "begepi"], nulls_last=True, maintain_order=True
    ).with_columns(
        ende_vor=pl.when(pl.col("quelleL").shift(1).over("persnr") == 1)
        .then(pl.col("endepi").shift(1).over("persnr"))
        .otherwise(None)
    ).with_columns(
        ende_vor=pl.col("ende_vor").forward_fill().over("persnr")
    ).with_columns(
        # gen byte lst = quelleL & nrL == 1 & (begepi - ende_vor > 10)
        lst=(
            (pl.col("quelleL") == 1)
            & (pl.col("nrL") == 1)
            & (
                pl.col("ende_vor").is_null()
                | (pl_days(pl.col("begepi"), pl.col("ende_vor")) > 10)
            )
        ).cast(pl.Int32)
    )

    # gsort persnr begepi -lst spell, then anz_lst accumulates lst
    frame = frame.sort(
        ["persnr", "begepi", "lst", "spell"],
        descending=[False, False, True, False],
        nulls_last=True,
        maintain_order=True,
    ).with_columns(
        anz_lst=pl.col("lst").cum_sum().over("persnr").cast(pl.Int32)
    ).drop("ende_vor", "lst")

    # --------------------------------------------------#
    # NUMBER OF DAYS WITH BENEFIT RECEIPT (tage_lst)    #
    # --------------------------------------------------#
    # The reference carries the running sum only on the main spell of each
    # episode and on non-benefit spells, then copies it to the parallel spells.
    log.info("Computing number of days with benefit receipts")

    frame = frame.with_columns(
        lstdauer=pl.when((pl.col("quelleL") == 1) & (pl.col("nrL") == 1))
        .then(_episode_days())
        .otherwise(0)
        .cast(pl.Int32)
    )

    frame = frame.sort(
        ["persnr", "spell", "begepi"], nulls_last=True, maintain_order=True
    ).with_columns(
        tage_lst=pl.col("lstdauer").cum_sum().over("persnr").cast(pl.Int32)
    ).with_columns(
        tage_lst=pl.when((pl.col("quelleL") == 0) | (pl.col("nrL") == 1))
        .then(pl.col("tage_lst"))
        .otherwise(None)
    )

    # bysort persnr begepi (nrL spell): replace tage_lst = tage_lst[1]
    # nrL is missing for non-benefit spells in Stata and therefore sorts last,
    # so benefit spells come first here.
    frame = frame.sort(
        ["persnr", "begepi", "nrL", "spell"], nulls_last=True, maintain_order=True
    ).with_columns(
        tage_lst=pl.col("tage_lst").first().over(["persnr", "begepi"])
    ).drop("lstdauer", "quelleL", "nrL")

    # -----------------------#
    # ADJUST MISSING VALUES  #
    # -----------------------#
    # The four establishment and job variables say nothing outside the
    # employment history, so the reference blanks them wherever the source is
    # not BEH. A missing quelle counts as not BEH, because `quelle != 1` is true
    # for a Stata missing.
    log.info("Blanking the establishment and job variables outside quelle 1")

    not_beh = pl.col("quelle").fill_null(-1) != 1
    frame = frame.with_columns(
        ein_bet=pl.when(not_beh).then(None).otherwise(pl.col("ein_bet")),
        tage_bet=pl.when(not_beh).then(None).otherwise(pl.col("tage_bet")),
        ein_job=pl.when(not_beh).then(None).otherwise(pl.col("ein_job")),
        tage_job=pl.when(not_beh).then(None).otherwise(pl.col("tage_job")),
    )

    log.info("Biographic variables script finished")
    return frame
