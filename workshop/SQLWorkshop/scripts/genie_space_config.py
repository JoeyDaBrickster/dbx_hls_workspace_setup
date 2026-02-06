"""
Genie Space configuration for the SQL Workshop.
Genie Spaces may require manual creation in the UI; this module holds
configuration values and behavior rules for documentation and any future API use.
Do not hardcode secrets; use workspace identity or documented manual steps.
"""
from dataclasses import dataclass
from typing import List

WORKSHOP_GENIE_SPACE_NAME = "SQL Workshop - Patient Claims"


@dataclass
class GenieBehaviorRule:
    """Single behavior rule for the Genie Space."""
    name: str
    description: str


GENIE_BEHAVIOR_RULES: List[GenieBehaviorRule] = [
    GenieBehaviorRule(
        name="local_provider",
        description="When the user says 'local' or 'local provider', interpret as New York (NY). Use state code NY in filters.",
    ),
    GenieBehaviorRule(
        name="state_abbreviations",
        description="Use two-letter US state abbreviations (e.g. NY, CA, TX) in filters and generated SQL.",
    ),
    GenieBehaviorRule(
        name="flag_fields",
        description="Treat columns ending in _flag or containing 'ind' as boolean. Use in WHERE as appropriate.",
    ),
    GenieBehaviorRule(
        name="claim_amount_date_defaults",
        description="Default claim amount to the primary payment column; default date to claim_start_date for filtering.",
    ),
    GenieBehaviorRule(
        name="diagnosis_descriptions",
        description="When referring to diagnoses by name, join to dim_diagnosis and use description/code; prefer descriptions in answers.",
    ),
]


def get_tables_for_genie(catalog: str, schema: str) -> List[str]:
    """Return fully qualified table names to attach to the Genie Space."""
    return [
        f"{catalog}.{schema}.fact_patient_claims",
        f"{catalog}.{schema}.rprt_patient_claims",
        f"{catalog}.{schema}.dim_beneficiary",
        f"{catalog}.{schema}.dim_provider",
        f"{catalog}.{schema}.dim_date",
        f"{catalog}.{schema}.dim_diagnosis",
    ]


SAMPLE_QUESTIONS = [
    "Claims for patients who saw a local provider.",
    "Total claim amount by state.",
    "How many claims in New York in 2020?",
    "Claim amounts by year and gender.",
    "Show me diagnosis descriptions for the top 10 claim amounts.",
]
