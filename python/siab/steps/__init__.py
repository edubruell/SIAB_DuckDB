"""One module per pipeline step, mirroring the R arm's functions/ folder."""

from siab.steps.drop_empty_columns import drop_empty_columns
from siab.steps.education import generate_educ_variable
from siab.steps.grund154 import reallocate_one_time_payments
from siab.steps.industries import generate_industry_variables
from siab.steps.merge_akm import merge_akm
from siab.steps.merge_annual_bhp import merge_annual_bhp
from siab.steps.merge_basic_bhp import merge_basic_bhp
from siab.steps.monthly_panel import build_monthly_panel
from siab.steps.observation_period import restrict_observation_period
from siab.steps.occupations import generate_occupation_variables
from siab.steps.parallel_episodes import handle_parallel_episodes
from siab.steps.siab_bio import generate_biographic_variables
from siab.steps.split_episodes import split_episodes
from siab.steps.wage_assessment_ceiling import generate_limit_assess
from siab.steps.wages_deflation import deflate_wages
from siab.steps.wages_imputation import impute_wages
from siab.steps.wages_marginal import generate_limit_marginal
from siab.steps.yearly_panel import build_yearly_panel

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
