# Part 6 – Genie

## Objective

Use the workshop Genie Space to answer natural-language questions and verify behavior rules (e.g. “local” = NY).

## Prerequisites

- A Genie Space has been created and configured per **docs/GENIE_SPACE_CONFIG.md**.
- The Space has the workshop warehouse and gold/cms tables attached.
- Behavior rules are set (see GENIE_SPACE_CONFIG).

## Instructions

1. Open **Genie** and select the workshop Space (name as provided by the instructor).
2. Ask: **“Claims for patients who saw a local provider.”**
3. Inspect the generated SQL and results; confirm that “local” is interpreted as **New York (NY)**.
4. Ask follow-up questions, for example:
   - Total claim amount by state.
   - How many claims in New York in 2020?
   - Claim amounts by year and gender.
5. Compare answers with your earlier SQL and dashboard results.

## Success criteria

- The question “Claims for patients who saw a local provider” returns results filtered by NY (or equivalent).
- You can view the underlying SQL and any auto-visualizations.
