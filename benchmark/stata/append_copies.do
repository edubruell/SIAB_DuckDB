/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	Append a synthetic delivery's per-copy core files into one delivery file.

	make_delivery.py writes the core spell file one copy at a time, because
	pyreadstat holds a whole frame in memory to write it and a stacked delivery
	does not fit. Stata appends them here, into the single
	SIAB_7523_v2.dta a delivery has and the reference prep expects.

	This is where the Stata arm meets its own ceiling: Stata holds the dataset
	in memory, so a delivery it cannot append is a delivery it could not have
	prepared either. The failure is the measurement.

	One environment variable:
	  SIAB_TEST_DATA  the delivery folder, holding a `core` subfolder

	Run it with:

	    stata-mp -b do benchmark/stata/append_copies.do

	Author(s): Eduard Brüll

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

clear all
set more off

global testdata : env SIAB_TEST_DATA
if "${testdata}" == "" {
	display as error "Set SIAB_TEST_DATA to the delivery folder."
	exit 198
}

local files : dir "${testdata}/core" files "SIAB_7523_v2_copy*.dta"
local files : list sort files
local count : word count `files'

if `count' == 0 {
	display as error "No per-copy core files in ${testdata}/core."
	exit 601
}

display "Appending `count' copies"

local first : word 1 of `files'
use "${testdata}/core/`first'", clear

forvalues i = 2/`count' {
	local next : word `i' of `files'
	append using "${testdata}/core/`next'"
	display "  `i'/`count', " _N " rows"
}

compress
save "${testdata}/SIAB_7523_v2.dta", replace
display "rows: " _N

exit, clear STATA
