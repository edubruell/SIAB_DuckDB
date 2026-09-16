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

	Everything else, including the pre-step block that restricts the sources and
	generates jahr and age, is copied from 00_master_SIAB.do unchanged.

	One deviation from "the reference is never edited"
	--------------------------------------------------
	03_SIAB_bio.do has been changed in ten places, each marked in the file with
	`// TIE-BREAK ADDED`. The original is kept beside it as
	03_SIAB_bio.do.unmodified, so the change is a one-line diff to audit.

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

dis "fixture dumps written to ${dump}"
dis "$S_DATE $S_TIME"

capture log close _all
