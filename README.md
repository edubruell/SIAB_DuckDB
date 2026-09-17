# SIAB  DuckDB <img src="siab_duckdb.png" align="right" height="139" alt="" />
![License: CC BY 4.0](https://img.shields.io/badge/License-CC%20BY%204.0-blue.svg)

## Project Overview

This project is a reimplementation of the data preparation process for the Sample of Integrated Labour Market Biographies (SIAB) based on the original `STATA` reference implementation by [Wolfgang Dauth and Johann Eppelsheimer (2023)](https://labourmarketresearch.springeropen.com/articles/10.1186/s12651-023-00335-w). This reimplementation uses R and DuckDB, which handles large datasets efficiently, including those that exceed available memory.

### Which SIAB version this targets

The target is the **SIAB 7523 v2**, the weakly anonymous extract covering 1975 to 2023.

### Advantages of Using DuckDB

- **High Performance**: DuckDB's in-memory database engine optimizes query performance, significantly speeding up the preparation compared to STATA. In addition processes like wage imputation based on observables are also much faster in R, leading to run times of less than 40 minutes for the entire workflow on a very limited virtual machine with just 8GB of RAM. 
  
- **Big Data Capability**: This implementation can handle datasets that exceed memory limits, making it possible to extend this code to prepare the entire universe of German social security data with only minor modifications. This larger-than-memory capability was tested by running the reimplementation on a virtual machine with artificially limited memory to a size smaller than the SIAB 2% sample data.

- **Portability**: The use of `dbplyr` and automatic SQL translation to DuckDB ensures that most of the code is a pure tidyverse implementation. Therefore, it can be easily adapted to other database systems or different big-data solutions like [tidypolars](https://github.com/etiennebacher/tidypolars). In fact only the episode splitting part uses SQL code directly.

## Repository layout

```
SIAB_DuckDB/
  R/                  the R arm: siab_main.R, run_testdata.R,
                      stata_to_db_batch_read.R and functions/
  python/             the Python arm: the siab package and main.py
  classifications/    shared, language-neutral lookup tables (CSV)
  tests/              testthat/ for R, pytest/ for Python,
                      and the shared Stata fixtures
  log/                per-step run logs, written by either arm
```

Both arms answer to the same Stata reference and to the same committed fixtures.
Neither answers to the other.

## Installation and Setup

### Prerequisites

- **R**: Ensure that R is installed on your machine.
- **DuckDB**: Install the DuckDB package for R.
- **tidyverse**: The code relies on `dplyr`, `dbplyr`, `readr`, `tidyr`, `purrr`, `stringr`, `glue` and `here`.
- **logger**: Logger writes the log files of the preparation workflow.
- **survival**: Contains the fast censored normal regression used for imputing wages on observables.
- **readstata13**: Reads the Basic Establishment File for the BHP merge.
- **scales** and **data.table**: `scales` labels the censoring overviews in the imputation, `data.table` is used by the read-in step.

### Getting started

1. **Clone the repository**:
   ```bash
   git clone https://github.com/edubruell/SIAB_DuckDB.git
   ```

2. **Read a SIAB into DuckDB**
The file `R/stata_to_db_batch_read.R` contains all code needed to install the prerequisites and read a STATA SIAB 7523 v2 file into a duckdb database. Run this to get started.

3. **Data preparation workflow**
`R/siab_main.R` launches the preparation workflow. The code installs and loads the necessary packages with the `pacman` package manager and its `p_load()` function. It then connects to a duckdb database, keeps the employment history, generates the year and age variables, and runs the following steps from the `R/functions` folder.

- `drop_empty_columns()`: Drops every column that is missing on all rows, as the reference master does once the sources are restricted. Pass `drop = FALSE` to keep them.
- `split_episodes()`: Splits the episodes in the SIAB data.
- `reallocate_one_time_payments()`: Moves one-time payments onto the episodes they belong to.
- `generate_biographic_variables()`: Generates biographical variables.
- `restrict_observation_period()`: Keeps the episodes whose year lies in the observation period, 1975 to 2023 by default.
- `generate_occupation_variables()`: Generates occupation-related variables.
- `generate_educ_variable()`: Generates the education variable.
- `merge_basic_bhp()`: Merges the Basic Establishment File.
- `generate_industry_variables()`: Maps the three-digit industry to the two one-digit classifications.
- `generate_limit_assess()`: Generates the wage assessment ceiling variable.
- `generate_limit_marginal()`: Generates the marginal wages related variables.
- `deflate_wages()`: Deflates the wages using the CPI.
- `impute_wages()`: Imputes missing wages.
- `handle_parallel_episodes()`: Handles parallel episodes.
- `build_yearly_panel()`: Builds the yearly panel.

`build_monthly_panel()` is an alternative to the last step rather than a step after it: it cuts every episode into one row per calendar month and keeps the month's 15th, where the yearly panel keeps one episode per year. Both start from the output of `handle_parallel_episodes()`, so a pipeline calls one or the other. The call sits commented out in `R/siab_main.R`, next to the yearly one.

Two further merges, `merge_annual_bhp()` and `merge_akm()`, sit commented out in `R/siab_main.R`. Both read files that have to be requested from the FDZ on top of the SIAB itself. Uncomment either call once the files are in place.

Each of these steps logs its progress to the specified log files.

### Acknowledgments

This project builds on the methodology outlined by Wolfgang Dauth and Johann Eppelsheimer. The original STATA scripts provided in their supplementary materials were essential references for this reimplementation.

### Licence
This project is licensed under the Creative Commons Attribution 4.0 International License (CC BY 4.0). See the LICENSE file for details.
