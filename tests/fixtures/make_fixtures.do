/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	Produce the Stata reference dumps the R test suite compares against.

	This runs the Stueber-Dauth-Eppelsheimer preparation over the FDZ test data
	and saves the full dataset after each step. This file replaces
	00_master_SIAB.do as the orchestration layer, because that master carries an
	IAB working directory, Windows path separators and the 7519 variable names.
	The reference steps themselves are used as they stand, with one exception,
	described below.

	What this file does that the master does not:
	  - points the four folder globals at a local work directory
	  - sets ${logfile} to 0, because every reference step opens its log with a
	    Windows backslash path that does not resolve on macOS
	  - renames persnr_siab and betnr_siab to persnr and betnr, which is what
	    SIAB 7523 v2 calls the keys and what the 7519-era reference expects
	  - saves the dataset after each step into $dump
	  - runs 09_restrictions.do for its dump but then continues from the
	    step 08 data, because that step imposes one project's sample cut
	    and the R pipeline has no counterpart for it
	  - turns on all five of step 11's switches and both of step 12's, which the
	    master leaves off because the files behind them have to be requested
	    separately; the test data carries every file step 11 wants
	  - runs steps 13 to 16 with the master's own switches on, so the chain
	    reaches the parallel episodes and the yearly panel; 17_clean_up.do is
	    not run, because its three working lines change no value
	  - fabricates the two AKM files step 12 reads, which no FDZ test product
	    supplies, from the shape the FDZ methodology report describes. See
	    tests/fixtures/make_synth_akm.do. The effects are noise, so step 12 is
	    compared on which rows receive one, never on a value.

	Everything else, including the pre-step block that restricts the sources and
	generates jahr and age, is copied from 00_master_SIAB.do unchanged.

	Two deviations from "the reference is never edited"
	---------------------------------------------------
	03_SIAB_bio.do has been changed in ten places and 15_parallel_episodes.do in
	one, each marked in the file with `// TIE-BREAK ADDED`. The reference lives
	in local_context/, which is not in this repo, so the changes are committed
	here as patches instead:

	    tests/fixtures/03_SIAB_bio_tiebreak.patch
	    tests/fixtures/15_parallel_episodes_tiebreak.patch

	The second adds `spell` as the last key of the gsort that defines the main
	episode. Without it the step keeps an arbitrary row in 1,945 of the test
	data's 479,806 person-episode groups, which are tied on quelle, tage_bet and
	wage_imp together, and neither Stata nor the R port is reproducible there.
	The step's own comment beside that line asks for exactly this. The R port
	carries the same last key, in functions/08_parallel_episodes.R.

	From a clean clone, put the published reference in
	local_context/stata_reference/origin_EastGermanWageStructure/, keep a copy as
	03_SIAB_bio.do.unmodified, and apply the patch before running this file:

	    patch local_context/stata_reference/origin_EastGermanWageStructure/03_SIAB_bio.do \
	        < tests/fixtures/03_SIAB_bio_tiebreak.patch
	    patch local_context/stata_reference/origin_EastGermanWageStructure/15_parallel_episodes.do \
	        < tests/fixtures/15_parallel_episodes_tiebreak.patch

	Without it the fixtures cannot be reproduced, because anz_lst and tage_lst
	come out differently on every run.

	Every one of the ten orders on `spell` and nothing further. After
	01_split_episodes.do a spell that crosses a year boundary is one row per
	calendar year and all its rows keep the same spell number, so those orders
	are tied, and Stata breaks a tie by shuffling. The step then takes running
	totals in that order, which means the reference's own anz_lst and tage_lst
	depend on the shuffle. On one benefit spell running from June 1975 to June
	1976 the unmodified step processed the 1976 half first and gave the 1975 row
	346 days against the 1976 row's 153, so the totals ran backwards in time.

	The fix adds begepi, or spell where begepi is already in the key, as the
	last sort key. It settles the order without changing what any statement
	computes, and it is what lets the R port be compared on these columns at all:
	before it, 65,518 of 505,050 rows disagreed on anz_lst with no port bug
	behind them.

	Run it from the project root:

	    /Applications/Stata/StataMP.app/Contents/MacOS/stata-mp -b do \
	        tests/fixtures/make_fixtures.do

	Stata returns 0 even when a do-file errors, so read
	local_context/stata_fixtures/log/make_fixtures.log afterwards.

	Then convert the dumps to the committed parquet fixtures:

	    Rscript tests/fixtures/make_fixtures.R

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

clear all
set more off
version 17

********************************************************************************
* Folders
********************************************************************************

global root     "/Users/ebr/Seafile/MeineBibliothek/git_projects/SIAB_DuckDB"
global prog     "${root}/local_context/stata_reference/origin_EastGermanWageStructure"
global testdata "${root}/local_context/testdata/siab_7523_v2"
global work     "${root}/local_context/stata_fixtures"

global orig     "${work}/orig"
global data     "${work}/data"
global log      "${work}/log"
global dump     "${work}/dump"

cap mkdir "${work}"
cap mkdir "${orig}"
cap mkdir "${data}"
cap mkdir "${log}"
cap mkdir "${dump}"

capture log close _all
log using "${log}/make_fixtures.log", replace name(fixtures)

* The reference steps open their own logs with a Windows path. Leave this at 0.
global logfile = 0
global inspect = 0

* The R pipeline applies no year window, so neither does the fixture run.
global minYear = 1975
global maxYear = 2023

dis "$S_DATE $S_TIME"

********************************************************************************
* Stage the input files under the 7519 key names the reference expects
********************************************************************************

use "${testdata}/SIAB_7523_v2_bhp_basis_v1.dta", clear
rename betnr_siab betnr
save "${orig}/SIAB_7523_v2_bhp_basis_v1.dta", replace

* The yearly establishment panel and the four worker-flow files, which step 11
* reads. They arrive under the names that step wants already; only the key has
* to be renamed. Each yearly file holds exactly one calendar year.
* The brace form of a global in a loop header makes Stata's block parser take
* the macro's own brace as the loop's, r(198). The dollar form is safe.
forvalues y = $minYear / $maxYear {
	capture confirm file "${testdata}/SIAB_7523_v2_bhp_v1_`y'.dta"
	if !_rc {
		use "${testdata}/SIAB_7523_v2_bhp_v1_`y'.dta", clear
		rename betnr_siab betnr
		save "${orig}/SIAB_7523_v2_bhp_v1_`y'.dta", replace
	}
}

foreach f in inflow outflow entry exit {
	use "${testdata}/SIAB_7523_v2_bhp_`f'_v1.dta", clear
	rename betnr_siab betnr
	save "${orig}/SIAB_7523_v2_bhp_`f'_v1.dta", replace
}

* The two AKM files do not exist as test data anywhere, so they are fabricated
* from the shape the FDZ methodology report describes. The effects are noise;
* what step 12 is compared on is which rows receive one. See the header of
* make_synth_akm.do. Generated here, with the other staging, because it loads
* data into memory and cannot run once the pipeline is under way.
do "${root}/tests/fixtures/make_synth_akm.do"

use "${testdata}/SIAB_7523_v2.dta", clear
rename persnr_siab persnr
rename betnr_siab  betnr

********************************************************************************
* Pre-step block, copied from 00_master_SIAB.do
********************************************************************************

keep if inlist(quelle,1,2,3) // keep only employment history

foreach var of varlist _all {
	capture assert missing(`var')
	if !_rc {
		drop `var'
	}
}

gen int jahr = year(begepi)
label variable jahr "year"

gen age = jahr - gebjahr
label variable age "age (in years)"

save "${dump}/00_master.dta", replace

********************************************************************************
* The steps
********************************************************************************

do "${prog}/01_split_episodes.do"
save "${dump}/01_split_episodes.dta", replace

do "${prog}/02_grund154.do"
save "${dump}/02_grund154.dta", replace

do "${prog}/03_SIAB_bio.do"
save "${dump}/03_SIAB_bio.dta", replace

* Restrict to the observation period, from 00_master_SIAB.do. With minYear 1975
* and maxYear 2023 this drops nothing; it is kept so the order of operations
* matches the master.
keep if inrange(jahr,${minYear},${maxYear})

do "${prog}/04_merge_basic_BHP.do"
save "${dump}/04_merge_basic_BHP.dta", replace

do "${prog}/05_educ_broad.do"
save "${dump}/05_educ_broad.dta", replace

do "${prog}/06_wages_assessment_ceiling.do"
save "${dump}/06_wages_assessment_ceiling.dta", replace

do "${prog}/07_wages_marginal.do"
save "${dump}/07_wages_marginal.dta", replace

do "${prog}/08_wages_deflation.do"
save "${dump}/08_wages_deflation.dta", replace

do "${prog}/09_restrictions.do"
save "${dump}/09_restrictions.dta", replace

* 09_restrictions.do cuts the data down to one project's population: BeH spells
* only, men only, full-time only, ages 20 to 60, and it drops spells with a
* missing plant, a missing east flag or a zero wage. The reference's own header
* calls the step project-specific and expects users to edit it, and the R
* pipeline has no counterpart. Its dump above is kept so the row set stays on
* record, but the comparison continues from the unrestricted step 08 data, so
* the imputation is compared on all the rows the R port carries rather than on
* that cut.
use "${dump}/08_wages_deflation.dta", clear

do "${prog}/10_wages_imputation.do"
save "${dump}/10_wages_imputation.dta", replace

* 00_master_SIAB.do leaves all five of these at 0, because the files they read
* are only released to a project that has requested them. The test data carries
* every one of them, so the fixture run turns them all on and compares the step.
global annual_BHP  = 1
global BHP_inflow  = 1
global BHP_outflow = 1
global BHP_entry   = 1
global BHP_exit    = 1

do "${prog}/11_merge_BHP.do"
save "${dump}/11_merge_BHP.dta", replace

global AKM_estab = 1
global AKM_pers  = 1

do "${prog}/12_merge_AKM.do"
save "${dump}/12_merge_AKM.dta", replace

* 13_industries_1digit.do and 14_occ_blossfeld.do sit between the AKM merge and
* the parallel episodes. Both only add columns and neither drops a row, so the
* chain to step 15 runs through them as published, with the master's own
* settings, which turn all three mappings on.
global destatis = 1
global estpanel = 1

do "${prog}/13_industries_1digit.do"
save "${dump}/13_industries_1digit.dta", replace

global blossfeld = 1

do "${prog}/14_occ_blossfeld.do"
save "${dump}/14_occ_blossfeld.dta", replace

* 15_parallel_episodes.do keeps one episode per person and episode start. The
* rule is the one the reference leaves uncommented: longest tenure first, the
* imputed wage only as a tie-break. The R port has to be called with
* handling = "tenure" to match it; its other setting sorts on the imputed wage,
* which is drawn at random on both sides and therefore picks different rows.
global parallel_vars = 1

do "${prog}/15_parallel_episodes.do"
save "${dump}/15_parallel_episodes.dta", replace

do "${prog}/16_yearly_panel.do"
save "${dump}/16_yearly_panel.dta", replace

* 17_clean_up.do has three working lines: sort, xtset and compress. None of them
* changes a value, so there is nothing for the R port to reproduce and nothing
* to compare. It is not run here.

dis "fixture dumps written to ${dump}"
dis "$S_DATE $S_TIME"

capture log close _all
