# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Build the occupation crosswalks used by R/functions/02_occupations.R
#
# Writes:
#   - kldb88_beruf.csv         beruf (KldB-88 Berufsordnung) with its German and
#                              English title, its 2-digit Berufsgruppe and
#                              1-digit Berufsbereich, and the observed frequency
#                              in the SIAB file the script is pointed at.
#   - walkover_beruf_occblo.csv  beruf -> Blossfeld occupation (occ_blo).
#
# Sources:
#   - Hierarchy and German titles: Destatis Klassifikationsserver, KldB 1988
#     structure file, downloaded at run time.
#   - English titles and the SIAB-specific codes (470, 555, 666, 888, 924, 995,
#     996, 997): the value label tables carried by the SIAB .dta itself.
#   - Blossfeld recode: 14_occ_blossfeld.do of the Stueber/Dauth/Eppelsheimer
#     preparation, after Schimpl-Neimanns (2003), ZUMA-Methodenbericht 2003/10.
#
# beruf is KldB 1988, not KldB 1992: all 334 KldB-88 Berufsordnungen occur in the
# SIAB label table, while 100 SIAB codes have no KldB-92 counterpart.
#
# Run this only when the classification or the SIAB version changes; the CSVs it
# writes are committed.
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library("pacman")
p_load(readstata13, dplyr, tidyr, readr, stringr, purrr, here)

# The SIAB file supplying value labels and observed counts.
siab_file <- Sys.getenv(
  "SIAB_FILE",
  here("local_context", "testdata", "siab_7523_v2", "SIAB_7523_v2.dta")
)

kldb_url <- paste0(
  "https://www.klassifikationsserver.de/klassService/thyme/variant/download/",
  "kldb1988?file=complete&type=csv"
)

# --- KldB 1988 hierarchy from the Klassifikationsserver -----------------------

read_kldb88_structure <- function(url) {
  zipfile <- tempfile(fileext = ".zip")
  download.file(url, zipfile, quiet = TRUE, mode = "wb")
  # The archive stores its filenames in CP850, so list them without translating.
  names  <- suppressWarnings(unzip(zipfile, list = TRUE)$Name)
  inner  <- names[grepl("Gliederung\\.csv$", names, useBytes = TRUE)][1]
  path  <- unzip(zipfile, files = inner, exdir = tempdir())
  # Nine lines of provenance header precede the column names.
  read_delim(path, delim = ";", skip = 8, col_types = cols(.default = col_character()),
             locale = locale(encoding = "UTF-8")) |>
    set_names(c("code", "level", "title_de", "unit")) |>
    select(code, level, title_de) |>
    mutate(level = as.integer(level))
}

kldb88 <- read_kldb88_structure(kldb_url)

berufsordnung <- kldb88 |>
  filter(level == 4) |>
  transmute(beruf = as.integer(code), kldb88_de = title_de)

berufsgruppe <- kldb88 |>
  filter(level == 3) |>
  transmute(kldb88_2 = as.integer(code), kldb88_2_de = title_de)

berufsbereich <- kldb88 |>
  filter(level == 1) |>
  transmute(kldb88_1 = code, kldb88_1_de = title_de)

# Level 1 is a roman numeral, so the parent of each Berufsgruppe is carried down
# the file rather than derived from the code.
hierarchy <- kldb88 |>
  filter(level %in% c(1, 3, 4)) |>
  # Level 1 codes are roman numerals, so only level-3 codes are made numeric.
  mutate(kldb88_1 = if_else(level == 1, code, NA_character_),
         kldb88_2 = if_else(level == 3, suppressWarnings(as.integer(code)), NA_integer_)) |>
  fill(kldb88_1, kldb88_2) |>
  filter(level == 4) |>
  transmute(beruf = as.integer(code), kldb88_2, kldb88_1)

# --- SIAB value labels and observed counts -----------------------------------

siab_head <- read.dta13(siab_file, convert.factors = FALSE, select.rows = c(1, 2))
label_tables <- attr(siab_head, "label.table")

label_tbl <- function(name, column) {
  tbl <- label_tables[[name]]
  tibble(beruf = as.integer(tbl), !!column := names(tbl)) |>
    filter(beruf < 2000) |>
    # The labels repeat the code as a prefix: "011 Landwirte".
    mutate(!!column := str_remove(.data[[column]], "^\\s*\\d+\\s+"))
}

siab_labels <- label_tbl("beruf_de", "label_de") |>
  left_join(label_tbl("beruf_en", "label_en"), by = "beruf")

counts <- read.dta13(siab_file, convert.factors = FALSE, select.cols = "beruf") |>
  count(beruf, name = "n_siab") |>
  filter(!is.na(beruf), beruf < 2000)

# --- The Blossfeld recode ----------------------------------------------------
# Transcribed from 14_occ_blossfeld.do. Every range is inclusive and the ranges
# are disjoint, so the order of the rules does not matter.

blossfeld_rules <- list(
  `1`  = list(c(11, 22), c(41, 51), c(53, 62)),
  `2`  = list(c(71, 133), c(135, 141), 143, c(151, 162), 164, c(176, 193),
              c(203, 213), c(222, 244), 252, 263, 301, 313, c(321, 323),
              c(332, 346), c(352, 371), 373, c(375, 377), c(402, 403), 412,
              c(423, 433), 442, c(452, 463), c(465, 472), 482, 486, 504,
              c(512, 531), c(543, 549)),
  `3`  = list(134, 142, 144, 163, c(171, 175), c(201, 202), 221, 251,
              c(261, 262), c(270, 291), 302, c(305, 312), c(314, 315), 331, 351,
              372, 374, c(378, 401), 411, c(421, 422), 441, 451, 464, 481,
              c(483, 485), c(491, 503), 511, c(541, 542)),
  `4`  = list(303, 304, c(621, 635), c(721, 722), 733, 857),
  `5`  = list(32, 52, c(601, 612), 726, 883),
  `6`  = list(c(685, 686), 688, 706, c(713, 716), c(723, 725), c(741, 744),
              c(791, 794), 805, 838, c(911, 913), c(923, 937)),
  `7`  = list(684, c(704, 705), c(711, 712), c(801, 804), 812, 814, 831,
              c(832, 836), 837, c(851, 852), c(854, 856), c(892, 902),
              c(921, 922)),
  `8`  = list(c(821, 823), 853, c(861, 864), c(873, 877)),
  `9`  = list(811, 813, c(841, 844), c(871, 872), c(881, 882), 891),
  `10` = list(682, 687, c(731, 732), 734, c(782, 784), 773),
  `11` = list(31, 681, 683, c(691, 703), c(771, 772), c(774, 781)),
  `12` = list(c(751, 763))
)

blossfeld_labels <- tribble(
  ~occ_blo, ~occ_blo_de,                                  ~occ_blo_en,
  1L,  "AGR-Agrarberufe",                                 "AGR-Agricultural occupations",
  2L,  "EMB-Einfache manuelle Berufe",                    "EMB-Unskilled manual occupations",
  3L,  "QMB-Qual. manuelle Berufe",                       "QMB-Skilled manual occupations",
  4L,  "TEC-Techniker",                                   "TEC-Technicians",
  5L,  "ING-Ingenieure",                                  "ING-Engineers",
  6L,  "EDI-Einfache Dienste",                            "EDI-Unskilled services",
  7L,  "QDI-Qual. Dienste",                               "QDI-Skilled services",
  8L,  "SEMI-Semiprofessionen",                           "SEMI-Semiprofessions",
  9L,  "PROF-Professionen",                               "PROF-Professions",
  10L, "EVB-Einfache kaufm. u. Verwaltungsberufe",        "EVB-Unskilled com. and admin. occupations",
  11L, "QVB-Qual. kaufm. u. Verwaltungsberufe",           "QVB-Skilled com. and admin. occupations",
  12L, "MAN-Manager",                                     "MAN-Managers",
  99L, "NO-Nicht zuordenbar",                             "NO-Not assignable"
)

expand_rule <- function(spec) {
  if (length(spec) == 1L) spec else seq(spec[1], spec[2])
}

blossfeld <- blossfeld_rules |>
  imap(~tibble(beruf = unlist(map(.x, expand_rule)), occ_blo = as.integer(.y))) |>
  bind_rows()

stopifnot(!any(duplicated(blossfeld$beruf)))

# --- Assemble and write ------------------------------------------------------

kldb88_beruf <- siab_labels |>
  left_join(berufsordnung, by = "beruf") |>
  left_join(hierarchy,     by = "beruf") |>
  left_join(berufsgruppe,  by = "kldb88_2") |>
  left_join(berufsbereich, by = "kldb88_1") |>
  left_join(counts,        by = "beruf") |>
  mutate(in_kldb88 = !is.na(kldb88_de),
         n_siab    = coalesce(n_siab, 0L)) |>
  select(beruf, label_de, label_en, in_kldb88, kldb88_de,
         kldb88_2, kldb88_2_de, kldb88_1, kldb88_1_de, n_siab) |>
  arrange(beruf)

walkover_beruf_occblo <- siab_labels |>
  select(beruf, label_de, label_en) |>
  left_join(blossfeld, by = "beruf") |>
  mutate(occ_blo = coalesce(occ_blo, 99L)) |>
  left_join(blossfeld_labels, by = "occ_blo") |>
  select(beruf, occ_blo, occ_blo_de, occ_blo_en, label_de, label_en) |>
  arrange(beruf)

write_csv(kldb88_beruf,          here("classifications", "kldb88_beruf.csv"))
write_csv(walkover_beruf_occblo, here("classifications", "walkover_beruf_occblo.csv"))

cat("kldb88_beruf.csv:", nrow(kldb88_beruf), "occupations,",
    sum(!kldb88_beruf$in_kldb88), "outside the KldB-88 structure\n")
cat("walkover_beruf_occblo.csv:", nrow(walkover_beruf_occblo), "rows,",
    sum(walkover_beruf_occblo$occ_blo == 99L), "not assignable to Blossfeld\n")
