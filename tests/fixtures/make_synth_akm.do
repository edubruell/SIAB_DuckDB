/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	Fabricate the two AKM files, because no FDZ test product carries them.

	12_merge_AKM.do merges person and establishment wage effects onto the SIAB
	from two files that are only released to a project that has requested them.
	The FDZ test data does not include them and there is no test product that
	does, so without this file step 12 cannot be run at all and the R port of it
	cannot be compared against anything.

	*** THE EFFECTS ARE NOISE. *** Normal draws with the right dispersion and
	nothing else: no wage content, no connected set, no relation to the wages in
	the test data. Any number computed from them is meaningless. What running
	step 12 on them tests is which rows get an effect and which are left missing,
	and that is a property of the merge, not of the values.

	The shape is taken from the FDZ documentation, not guessed:

	    Lochner, Benjamin; Wolter, Stefanie (2025): AKM effects for German
	    labour market data 1985-2023. FDZ-Methodenreport 03/2025 (en).
	    https://doku.iab.de/fdz/reporte/2025/MR_03-25_EN.pdf

	From it:
	  - five non-overlapping windows, 1985-1992, 1993-2000, 2001-2008,
	    2009-2016 and 2017-2023 (section 2, and the variable list in section 7)
	  - one file per side, the person file keyed on persnr_siab and the
	    establishment file on betnr_siab (section 5.1)
	  - variables peff_1985_1992 ... peff_2017_2023 and feff_1985_1992 ...
	    feff_2017_2023, numeric (sections 7.2 and 7.3)
	  - German and English variable labels, switched with `label language`
	    (end of section 5), which is why 12_merge_AKM.do runs `label language de`
	    before both merges
	  - both effects have mean zero, because the Stata reimplementation estimates
	    them with reghdfe as deviations from the mean (section 2)

	Section 7 prints the variable names twice, once in the heading with
	underscores and once in the table body with a hyphen, as in `peff_1985-1992`.
	A hyphen is not a legal Stata name, so the underscore form is the real one.

	The dispersions are Table 4-1's "sd person effects" and "sd estbl. effects"
	row, per window. The coverage rates are Table 5-1, which reports the share of
	SIAB persons and establishments that find a match in each window, on a sample
	restricted the way the AKM estimation is. Coverage is applied per window
	rather than per file, because that is how the real files behave: an
	establishment in the panel throughout carries five effects, one that existed
	only after 2010 carries the last two and missing for the rest. A row is
	written only if at least one of its five windows survived.

	Coverage is deliberately below 100 per cent and it is what makes the
	comparison worth running: `keep(master match)` leaves non-matching rows with
	missing effects, and a generator that matched everything would never exercise
	that path.

	Called from make_fixtures.do after the identifiers have been renamed. It
	reads the staged input under ${orig} and writes the two files beside it.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

if "$akm_seed" == "" global akm_seed 20260916
set seed $akm_seed

* The FDZ test data carries German and English labels; a dataset built here
* carries only "default". 12_merge_AKM.do runs `label language de` before both
* merges, so both languages have to exist on the using files too.
capture program drop akm_lang
program define akm_lang
	capture quietly label language de
	if _rc quietly label language de, new
	capture quietly label language en
	if _rc quietly label language en, new
	quietly label language en
end

********************************************************************************
* Establishment effects
********************************************************************************

* Table 4-1, sd estbl. effects, and Table 5-1, establishment match rates.
local f_sd  "0.185 0.222 0.263 0.232 0.180"
local f_cov "0.882 0.944 0.916 0.904 0.897"
local wins  "1985_1992 1993_2000 2001_2008 2009_2016 2017_2023"

use betnr using "${orig}/SIAB_7523_v2_bhp_basis_v1.dta", clear
bysort betnr: keep if _n == 1

local i = 1
foreach w of local wins {
	local sd  : word `i' of `f_sd'
	local cov : word `i' of `f_cov'
	gen double feff_`w' = rnormal(0, `sd')
	replace    feff_`w' = .  if runiform() > `cov'
	label var  feff_`w' "Establishment Effect `=subinstr("`w'","_","-",1)' - SYNTHETIC, meaningless"
	local ++i
}

egen byte n_eff = rownonmiss(feff_*)
drop if n_eff == 0
drop n_eff

compress
label data "SYNTHETIC AKM establishment effects - FAKE DATA"
akm_lang
save "${orig}/SIAB_7523_v2_akm_estab.dta", replace
dis "akm_estab written: " _N " establishments"

********************************************************************************
* Person effects
********************************************************************************

* Table 4-1, sd person effects, and Table 5-1, person match rates.
local p_sd  "0.350 0.350 0.394 0.408 0.404"
local p_cov "0.891 0.954 0.941 0.945 0.956"

use persnr_siab using "${testdata}/SIAB_7523_v2.dta", clear
rename persnr_siab persnr
bysort persnr: keep if _n == 1

local i = 1
foreach w of local wins {
	local sd  : word `i' of `p_sd'
	local cov : word `i' of `p_cov'
	gen double peff_`w' = rnormal(0, `sd')
	replace    peff_`w' = .  if runiform() > `cov'
	label var  peff_`w' "Person Effect `=subinstr("`w'","_","-",1)' - SYNTHETIC, meaningless"
	local ++i
}

egen byte n_eff = rownonmiss(peff_*)
drop if n_eff == 0
drop n_eff

compress
label data "SYNTHETIC AKM person effects - FAKE DATA"
akm_lang
save "${orig}/SIAB_7523_v2_akm_pers.dta", replace
dis "akm_pers written: " _N " persons"
