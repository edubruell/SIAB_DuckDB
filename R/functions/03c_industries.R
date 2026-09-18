# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   
#   SIAB Preparation
# 
# Generate 1-digit industry variables from the 3-digit industry (w93_3_gen)
# 
# Generates the variables:
#   - industry1_destatis: Industry; 1-digit; Statistisches Bundesamt (2002);
#     based on w93_3_gen
#   - industry1_estpanel: Industry; 1-digit; IAB establishment panel;
#     based on w93_3_gen
# 
# Notes:
#   w93_3_gen arrives with merge_basic_bhp() and is already coded
# time-consistently, see Eberle et al. (2014). Both mappings are ranges over the
# WZ93 three-digit code, spelled out in 13_industries_1digit.do and copied here
# unchanged. A code that falls in no range keeps a missing industry, which is
# what the reference's `replace ... if` leaves behind as well.
# 
#   The reference gates each mapping behind a macro, `destatis` and `estpanel`.
# The `mappings` argument is that switch. The R port has no value labels, so the
# labels of both classifications sit in the tables below and are written to the
# log with the case counts.
# 
#   Port of 13_industries_1digit.do.
# 
# Author(s): Eduard Bruell based on code by Wolfgang Dauth, Johann Eppelsheimer
#            and Heiko Stueber
# 
# Version: 1.0
# Created: 2026-09-16
# 
# References:
#   Eberle, J., P. Jacobebbinghaus, J. Ludsteck, and J. Witter (2014):
#   Generation of time-consistent industry codes in the face of classification
#   changes. FDZ-Methodenreport 05/2011.
#   Statistisches Bundesamt (2002): Klassifikation der Wirtschaftszweige,
#   Ausgabe 1993.
# 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

generate_industry_variables <- function(connection,
                                        mappings = c("destatis", "estpanel"),
                                        log_file = NULL){
  
  mappings <- match.arg(mappings, c("destatis", "estpanel"), several.ok = TRUE)

  #The 16 categories of the Statistisches Bundesamt mapping
  industry1_destatis_labels <- tibble::tribble(
    ~industry1_destatis, ~label_de,                                                   ~label_en,
    1L,  "Land- u. Forstwirtschaft",                                  "Agriculture a. forestry",
    2L,  "Fischerei u. Fischzucht",                                   "Fishing a. fish farming",
    3L,  "Bergbau u. Gewinnung v. Steinen u. Erden",                  "Mining and sector",
    4L,  "Verarbeitendes Gewerbe",                                    "Manufacturing industry",
    5L,  "Energie- u. Wasserversorgung",                              "Energy and water supply, recycling",
    6L,  "Baugewerbe",                                                "Construction",
    7L,  "Handel; Instandhaltung u. Reparatur v. KFZ u. Geb.guetern", "Wholesale and retail trade; repair of motor vehicles, motorcycles and personal and household goods",
    8L,  "Gastgewerbe",                                               "Hospitality",
    9L,  "Verkehr und Nachrichtenuebermittlung",                      "Transport, storage, communication",
    10L, "Kredit- u. Vers.gewerbe",                                   "Finance and insurance",
    11L, "Grunds.- u. Wohn.wesen, Vermietung bewgl. Sachen, Dienstl. f. Untern.", "Real estate and housing, renting of movable property, business related services",
    12L, "Oeff. Verw., Verteid., Soz.vers.",                          "Public administration, defense, social security",
    13L, "Erziehung u. Unterricht",                                   "Education",
    14L, "Gesundsh.-, Vet.- u. Soz.wesen",                            "Healthcare and social services",
    15L, "Sonstige Dienstl.",                                         "Other services",
    16L, "Private Haushalte",                                         "Private households"
  )

  #The 9 categories of the IAB establishment panel mapping
  industry1_estpanel_labels <- tibble::tribble(
    ~industry1_estpanel, ~label_de,                               ~label_en,
    1L, "Land- und Forstwirtschaft, Fischerei", "Agriculture, forestry and fishing",
    2L, "Nahrungs- und Genussmittel",           "Food and beverages",
    3L, "Verbrauchsgueter",                     "Consumables",
    4L, "Produktionsgueter",                    "Production goods",
    5L, "Investitions- und Gebrauchsgueter",    "Capital and consumer goods",
    6L, "Baugewerbe",                           "Construction",
    7L, "Gastgewerbe",                          "Hospitality",
    8L, "Verkehr und Lagerei",                  "Transport and storage",
    9L, "Erziehung und Unterricht",             "Education"
  )
  
  # Remove old log file if it exists
  if (!is.null(log_file) && file.exists(log_file)) {
    file.remove(log_file)
  }
  
  # Clear existing log appenders
  log_appender(NULL, namespace = "industries")
  
  # Initialize console logger
  log_appender(appender_console, namespace = "industries")
  
  # Initialize file logger if log_file is specified
  if (!is.null(log_file)) {
    log_appender(appender_tee(file = log_file), namespace = "industries")
  }
  
  log_info("Industry variable script started", namespace = "industries")
  
  if (!"w93_3_gen" %in% colnames(tbl(connection, "data"))) {
    stop("generate_industry_variables() needs w93_3_gen, which merge_basic_bhp() brings in")
  }
  
  #A small helper that logs how many episodes each category got
  log_categories <- function(column, labels){
    counts <- tbl(connection, "data") |>
      count(.data[[column]]) |>
      collect() |>
      rename(code = 1)
    
    labels |>
      rename(code = 1) |>
      left_join(counts, by = "code") |>
      mutate(n = coalesce(n, 0L)) |>
      arrange(code) |>
      glue_data("{column} = {code} ({label_en}) for {n} episodes") |>
      walk(log_info, namespace = "industries")
    
    missing <- tbl(connection, "data") |>
      filter(is.na(.data[[column]])) |>
      count() |>
      pull(n)
    log_info("{column} is missing for {missing} episodes",
             namespace = "industries")
  }
  
  #====================================================================
  #  Statistisches Bundesamt (2002)
  #====================================================================
  if ("destatis" %in% mappings) {
    log_info("Mapping w93_3_gen to the Statistisches Bundesamt 1-digit industry",
             namespace = "industries")
    
    tbl(connection, "data") |>
      mutate(industry1_destatis = case_when(
        w93_3_gen >=  11 & w93_3_gen <=  20 ~  1L,
        w93_3_gen >=  50 & w93_3_gen <=  50 ~  2L,
        w93_3_gen >= 101 & w93_3_gen <= 145 ~  3L,
        w93_3_gen >= 151 & w93_3_gen <= 372 ~  4L,
        w93_3_gen >= 401 & w93_3_gen <= 410 ~  5L,
        w93_3_gen >= 451 & w93_3_gen <= 455 ~  6L,
        w93_3_gen >= 501 & w93_3_gen <= 527 ~  7L,
        w93_3_gen >= 551 & w93_3_gen <= 555 ~  8L,
        w93_3_gen >= 601 & w93_3_gen <= 642 ~  9L,
        w93_3_gen >= 651 & w93_3_gen <= 672 ~ 10L,
        w93_3_gen >= 701 & w93_3_gen <= 748 ~ 11L,
        w93_3_gen >= 751 & w93_3_gen <= 753 ~ 12L,
        w93_3_gen >= 801 & w93_3_gen <= 804 ~ 13L,
        w93_3_gen >= 851 & w93_3_gen <= 853 ~ 14L,
        w93_3_gen >= 900 & w93_3_gen <= 930 ~ 15L,
        w93_3_gen >= 950 & w93_3_gen <= 990 ~ 16L,
        TRUE ~ NA_integer_
      )) |>
      compute_and_overwrite()
    
    log_categories("industry1_destatis", industry1_destatis_labels)
  }
  
  #====================================================================
  #  IAB establishment panel
  #====================================================================
  # The reference writes this mapping as nine groups of `replace` lines, several
  # of them made of ranges that are far apart in the classification. The ranges
  # do not overlap, so the case_when below reproduces them in one pass.
  if ("estpanel" %in% mappings) {
    log_info("Mapping w93_3_gen to the IAB establishment panel 1-digit industry",
             namespace = "industries")
    
    tbl(connection, "data") |>
      mutate(industry1_estpanel = case_when(
        # Agriculture, hunting and forestry, fishing
        w93_3_gen >=  11 & w93_3_gen <=  50 ~ 1L,
        # Mining and quarrying, energy and water supply, recycling
        w93_3_gen >= 101 & w93_3_gen <= 145 ~ 1L,
        w93_3_gen >= 371 & w93_3_gen <= 410 ~ 1L,
        w93_3_gen == 900                    ~ 1L,
        # Manufacture of food products, beverages and tobacco
        w93_3_gen >= 151 & w93_3_gen <= 160 ~ 2L,
        # Manufacture of consumer products
        w93_3_gen >= 171 & w93_3_gen <= 193 ~ 3L,
        w93_3_gen >= 221 & w93_3_gen <= 223 ~ 3L,
        w93_3_gen >= 361 & w93_3_gen <= 366 ~ 3L,
        # Manufacture of industrial goods
        w93_3_gen >= 201 & w93_3_gen <= 212 ~ 4L,
        w93_3_gen >= 231 & w93_3_gen <= 287 ~ 4L,
        # Manufacture of capital and consumer goods
        w93_3_gen >= 291 & w93_3_gen <= 355 ~ 5L,
        # Construction
        w93_3_gen >= 451 & w93_3_gen <= 455 ~ 6L,
        # Hotels and restaurants, personal services, trade and repair
        w93_3_gen >= 551 & w93_3_gen <= 555 ~ 7L,
        w93_3_gen >= 921 & w93_3_gen <= 930 ~ 7L,
        w93_3_gen >= 501 & w93_3_gen <= 527 ~ 7L,
        # Transport and storage, information, finance, real estate, renting,
        # IT, liberal professions and other business related services
        w93_3_gen >= 601 & w93_3_gen <= 634 ~ 8L,
        w93_3_gen >= 641 & w93_3_gen <= 642 ~ 8L,
        w93_3_gen >= 651 & w93_3_gen <= 672 ~ 8L,
        w93_3_gen >= 701 & w93_3_gen <= 703 ~ 8L,
        w93_3_gen >= 711 & w93_3_gen <= 714 ~ 8L,
        w93_3_gen >= 721 & w93_3_gen <= 726 ~ 8L,
        w93_3_gen >= 731 & w93_3_gen <= 744 ~ 8L,
        w93_3_gen >= 745 & w93_3_gen <= 748 ~ 8L,
        # Education, health and social work, organizations, public administration
        w93_3_gen >= 801 & w93_3_gen <= 804 ~ 9L,
        w93_3_gen >= 851 & w93_3_gen <= 853 ~ 9L,
        w93_3_gen >= 911 & w93_3_gen <= 913 ~ 9L,
        w93_3_gen >= 751 & w93_3_gen <= 753 ~ 9L,
        TRUE ~ NA_integer_
      )) |>
      compute_and_overwrite()
    
    log_categories("industry1_estpanel", industry1_estpanel_labels)
  }
  
  log_success("Industry variables generated", namespace = "industries")
  #Return the connection so we can pipe prepare functions
  return(connection)
}
