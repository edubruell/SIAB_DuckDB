"""One module per pipeline step, mirroring the R arm's R/functions/ folder."""

from siab.steps.s00b_drop_empty_columns import drop_empty_columns
from siab.steps.s01_siab_bio import generate_biographic_variables
from siab.steps.s01_split_episodes import split_episodes
from siab.steps.s01b_grund154 import reallocate_one_time_payments
from siab.steps.s02_occupations import generate_occupation_variables
from siab.steps.s03_education import generate_educ_variable
from siab.steps.s03b_merge_basic_bhp import merge_basic_bhp
from siab.steps.s03c_industries import generate_industry_variables
from siab.steps.s03d_observation_period import restrict_observation_period
from siab.steps.s04_wage_assessment_ceiling import generate_limit_assess
from siab.steps.s05_wages_marginal import generate_limit_marginal
from siab.steps.s06_wages_deflation import deflate_wages
from siab.steps.s07_wages_imputation import impute_wages
from siab.steps.s07b_merge_annual_bhp import merge_annual_bhp
from siab.steps.s07c_merge_akm import merge_akm
from siab.steps.s08_parallel_episodes import handle_parallel_episodes
from siab.steps.s09_yearly_panel import build_yearly_panel
from siab.steps.s09b_monthly_panel import build_monthly_panel

# In the reference's order, which is the order main.py calls them in.
__all__ = [
    "drop_empty_columns",
    "split_episodes",
    "reallocate_one_time_payments",
    "generate_biographic_variables",
    "restrict_observation_period",
    "generate_occupation_variables",
    "merge_basic_bhp",
    "generate_industry_variables",
    "generate_educ_variable",
    "generate_limit_assess",
    "generate_limit_marginal",
    "deflate_wages",
    "impute_wages",
    "merge_annual_bhp",
    "merge_akm",
    "handle_parallel_episodes",
    "build_yearly_panel",
    "build_monthly_panel",
]
