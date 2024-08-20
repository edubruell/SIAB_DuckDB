# Reimplementation of SIAB Preparation using R and DuckDB

## Project Overview

This project is a reimplementation of the data preparation process for the Sample of Integrated Labour Market Biographies (SIAB) based on original `STATA` reference implementation  by Wolfgang Dauth and Johann Eppelsheimer. This reimplementation uses R and DuckDB, leveraging the high-performance capabilities of DuckDB to handle large datasets efficiently, including those that exceed available memory.

### Key Differences from the Original Implementation

One of the most significant differences between this reimplementation and the original work by Dauth and Eppelsheimer is the use of the **SIAB-R7514 Scientific Use File (SUF)**. While the original implementation used the less anonymized SIAB 7517 data, this project is tailored for the factually anonymized version available to researchers under stricter data protection constraints.

- **SIAB-R7514 SUF**: This dataset is more anonymized, with certain variables aggregated or suppressed to protect individual identities. For example, only person-specific establishment identifiers are not available, and industry and occupation codes are less detailed.
  
- **Implementation Adjustments**: The code includes necessary modifications to accommodate these differences, ensuring that the preparation process is fully compatible with the SIAB-R7514 SUF. This includes adjustments to how variables are generated, and how certain analyses (e.g., wage imputation) are conducted.

### Advantages of Using DuckDB

- **High Performance**: DuckDB's in-memory database engine optimizes query performance, significantly speeding up the preparation comparad to STATA. In addition. processes like wage imputation based on observables are also much faster in R.
  
- **Big Data Capability**: This implementation can handle datasets that exceed memory limits, making it possible to extend this code to prepare the entire universe of German social security data with only minor modifications. This larger-then-memory capability was tested by running the reimplementation on a virtual machine with artificially limited memory to a size smaller than the SIAB 2% sample data.

- **Portability**: Through the use of `dbplyr` and automatic SQL translation to DuckDB ensures that most of the code is pure tidyverse. Therefore, it can be easily adapted to other database systems or different big-data solutions like [tidypolars](https://github.com/etiennebacher/tidypolars). In fact only the episode splitting part uses SQL code directly.

## Installation and Setup

### Prerequisites

- **R**: Ensure that R is installed on your machine.
- **DuckDB**: Install the DuckDB package for R.
- **tidyverse**: The code heavily relies on tidyverse packages, namely `dplyr`, `dbplyr`, `readr`, `tidyr`, `purrr`,  `stringr`, `glue` and  `here`,
- **logger**: Logger is neede to create log-files for the data preparation workflows
- **survival**: Contains a very fast implementation of censored normal regression used for imputing wages on observables

### Getting started

1. **Clone the repository**: 
   ```bash
   git clone https://github.com/edubruell/SIAB_DuckDB.git
   ```
2. **Convert a SUF to DuckDB**
The file `stata_db_batch_read.R` in the project main folder contains all code needed to install the prerequisites and read a STATA SIAB-R7514 SUF into a duckdb database. Run this to get started.

3. **Data preparation workflow**
The `siab_main.R` launches a data prepare workflow. The code starts by automatically installing and loading the necessary packages using the `pacman` package manager and the `p_load()` function. The code then connects to a duckdb database and executes a series of functions to prepare the SIAB data as a yearly panel, which are cotained in the functions folder.

- `split_episodes()`: Splits the episodes in the SIAB data.
- `generate_biographic_variables()`: Generates biographical variables.
- `generate_occupation_variables()`: Generates occupation-related variables.
- `generate_educ_variable()`: Generates the education variable.
- `generate_limit_assess()`: Generates the wage assessment ceiling variable.
- `generate_limit_marginal()`: Generates the marginal wages related variables.
- `deflate_wages()`: Deflates the wages using the CPI.
- `impute_wages()`: Imputes missing wages.
- `handle_parallel_episodes()`: Handles parallel episodes.
- `build_yearly_panel()`: Builds the yearly panel.

Each of these steps logs the progress to the specified log files.
 
### Acknowledgments

This project builds on the methodology outlined by Wolfgang Dauth and Johann Eppelsheimer. The original STATA scripts provided in their supplementary materials were essential references for this reimplementation.
License

### Licence
This project is licensed under the Creative Commons Attribution 4.0 International License (CC BY 4.0). See the LICENSE file for details.
