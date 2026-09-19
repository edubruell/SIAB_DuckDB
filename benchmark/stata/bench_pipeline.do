/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	Time the original Stata preparation over a synthetic delivery.

	The chain is 00_master_SIAB.do's, with the master's own switches: the two
	merges that read separately requested files are off, industries, Blossfeld
	occupations, parallel episodes and the yearly panel are on. What is left
	out is every `save` between steps. The fixture generator make_fixtures.do
	keeps those because it exists to dump each step; here they would be disk
	the two other arms never touch.

	Each step writes a two-line log in the shape both other arms write, so
	benchmark/run_benchmark.py reads all three the same way.

	Four environment variables, each with a fallback:
	  SIAB_ROOT            the project root, default the working directory
	  SIAB_STATA_REFERENCE the folder holding the reference do-files
	  SIAB_TEST_DATA       the delivery to run over, synthetic or real
	  SIAB_STATA_WORK      the working folder this file writes into

	Run it with:

	    stata-mp -b do benchmark/stata/bench_pipeline.do

	The delivery needs a core spell file. A synthetic one is written by
	make_delivery.py as one file per copy and appended by append_copies.do,
	which has to run first.

	Author(s): Eduard Brüll

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

clear all
set more off

********************************************************************************
* Folders
********************************************************************************

global root : env SIAB_ROOT
if "${root}" == "" global root "`c(pwd)'"

global prog : env SIAB_STATA_REFERENCE
if "${prog}" == "" ///
    global prog "${root}/local_context/stata_reference/origin_EastGermanWageStructure"

global testdata : env SIAB_TEST_DATA
if "${testdata}" == "" global testdata "${root}/local_context/testdata/siab_7523_v2"

global work : env SIAB_STATA_WORK
if "${work}" == "" global work "${root}/local_context/stata_bench"

global orig "${work}/orig"
global data "${work}/data"
global log  "${work}/log"

cap mkdir "${work}"
cap mkdir "${orig}"
cap mkdir "${data}"
cap mkdir "${log}"

* A fresh set of step logs. benchlog appends, so leftovers from an earlier run
* in the same folder would read as one very long step.
local leftovers : dir "${log}" files "*.log"
foreach leftover of local leftovers {
	erase "${log}/`leftover'"
}

* The reference steps open their own logs with a Windows path. Leave this at 0.
global logfile = 0
global inspect = 0

* The window the two other arms apply, so all three do the same work.
global minYear = 1975
global maxYear = 2023

********************************************************************************
* Label languages, which a synthetic delivery arrives without
********************************************************************************

* A delivery carries its labels in two named languages, `de` and `en`, and the
* reference switches between them in almost every step. pyreadstat writes
* neither name, so a synthetic file arrives with one unnamed set and the
* reference stops at its first `label language de` with r(111). Both names are
* made here out of whatever labels the file carries. Nothing in the prep
* branches on the text of a label, only on values.
capture program drop ensure_languages
program define ensure_languages
	capture label language de
	if _rc label language de, rename
	capture label language en
	if _rc label language en, new copy
	label language de
end

********************************************************************************
* A step log in the shape the R and Python arms write
********************************************************************************

capture program drop benchlog
program define benchlog
	args step event
	local today = string(date(c(current_date), "DMY"), "%tdCCYY-NN-DD")
	local stamp "`today' `c(current_time)'"
	file open benchlogfile using "${log}/`step'.log", write append text
	file write benchlogfile "INFO [`stamp'] `event'" _n
	file close benchlogfile
end

********************************************************************************
* Stage the input files under the key names the reference expects
********************************************************************************

benchlog 00_staging "staging started"

use "${testdata}/SIAB_7523_v2_bhp_basis_v1.dta", clear
rename betnr_siab betnr
ensure_languages
save "${orig}/SIAB_7523_v2_bhp_basis_v1.dta", replace

* The brace form of a global in a loop header makes Stata's block parser take
* the macro's own brace as the loop's, r(198). The dollar form is safe.
forvalues y = $minYear / $maxYear {
	capture confirm file "${testdata}/SIAB_7523_v2_bhp_v1_`y'.dta"
	if !_rc {
		use "${testdata}/SIAB_7523_v2_bhp_v1_`y'.dta", clear
		rename betnr_siab betnr
		ensure_languages
		save "${orig}/SIAB_7523_v2_bhp_v1_`y'.dta", replace
	}
}

foreach f in inflow outflow entry exit {
	capture confirm file "${testdata}/SIAB_7523_v2_bhp_`f'_v1.dta"
	if !_rc {
		use "${testdata}/SIAB_7523_v2_bhp_`f'_v1.dta", clear
		rename betnr_siab betnr
		ensure_languages
		save "${orig}/SIAB_7523_v2_bhp_`f'_v1.dta", replace
	}
}

benchlog 00_staging "staging finished"

********************************************************************************
* Pre-step block, copied from 00_master_SIAB.do
********************************************************************************

benchlog 00_master "pre-step block started"

use "${testdata}/SIAB_7523_v2.dta", clear
rename persnr_siab persnr
rename betnr_siab  betnr
ensure_languages

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

benchlog 00_master "pre-step block finished"

********************************************************************************
* The steps, in the master's own order and with the master's switches
********************************************************************************

benchlog 01_split_episodes "step started"
do "${prog}/01_split_episodes.do"
benchlog 01_split_episodes "step finished"

benchlog 02_grund154 "step started"
do "${prog}/02_grund154.do"
benchlog 02_grund154 "step finished"

benchlog 03_SIAB_bio "step started"
do "${prog}/03_SIAB_bio.do"
benchlog 03_SIAB_bio "step finished"

benchlog 03b_observation_period "step started"
keep if inrange(jahr, ${minYear}, ${maxYear})
benchlog 03b_observation_period "step finished"

benchlog 04_merge_basic_BHP "step started"
do "${prog}/04_merge_basic_BHP.do"
benchlog 04_merge_basic_BHP "step finished"

benchlog 05_educ_broad "step started"
do "${prog}/05_educ_broad.do"
benchlog 05_educ_broad "step finished"

benchlog 06_wages_assessment_ceiling "step started"
do "${prog}/06_wages_assessment_ceiling.do"
benchlog 06_wages_assessment_ceiling "step finished"

benchlog 07_wages_marginal "step started"
do "${prog}/07_wages_marginal.do"
benchlog 07_wages_marginal "step finished"

benchlog 08_wages_deflation "step started"
do "${prog}/08_wages_deflation.do"
benchlog 08_wages_deflation "step finished"

* 09_restrictions.do is skipped, as it is in the fixture generator.
* In this reference it is not the empty default: it carries one project's own
* sample cut and takes the test data from 505,050 rows to 83,817. Neither arm
* ports it, so running it here would time a different chain over a fifth of
* the data.

benchlog 10_wages_imputation "step started"
do "${prog}/10_wages_imputation.do"
benchlog 10_wages_imputation "step finished"

* 11_merge_BHP.do and 12_merge_AKM.do stay off, as they are in the master and
* in python/main.py. R/run_testdata.R runs the annual BHP merge, so an R run
* does that much more work than the other two; the per-step times are what the
* difference is read off.
global annual_BHP = 0
global BHP_inflow = 0
global BHP_outflow = 0
global BHP_entry = 0
global BHP_exit = 0
global AKM_estab = 0
global AKM_pers = 0

global destatis = 1
global estpanel = 1
benchlog 13_industries_1digit "step started"
do "${prog}/13_industries_1digit.do"
benchlog 13_industries_1digit "step finished"

global blossfeld = 1
benchlog 14_occ_blossfeld "step started"
do "${prog}/14_occ_blossfeld.do"
benchlog 14_occ_blossfeld "step finished"

global parallel_vars = 1
benchlog 15_parallel_episodes "step started"
do "${prog}/15_parallel_episodes.do"
benchlog 15_parallel_episodes "step finished"

benchlog 16_yearly_panel "step started"
do "${prog}/16_yearly_panel.do"
benchlog 16_yearly_panel "step finished"

********************************************************************************
* Report, in the shape run_benchmark.py reads
********************************************************************************

count
dis "rows: " r(N)

exit, clear STATA
