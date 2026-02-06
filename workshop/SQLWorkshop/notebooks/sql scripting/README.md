# SQL Scripting – Instructor Notebooks

Databricks SQL Scripting topics with examples using the **gold** schema `healthcare_dev.gold` and tables: `fact_cms_outpatient_claims`, `fact_cms_inpatient_claims`, `dim_cms_beneficiary`, `dim_cms_provider`, `dim_date`.

**Prerequisites:** BuildTheWorkspace.ipynb has been run so that `healthcare_dev.gold` is populated (including CMS gold tables from `run_cms_builds_in_order`).

| # | Notebook | Topic |
|---|----------|--------|
| 00 | 00_sql_scripting_overview.ipynb | Overview and simple BEGIN/END with workshop data |
| 01 | 01_begin_end_compound_statement.ipynb | BEGIN END, DECLARE, EXIT HANDLER |
| 02 | 02_case_statement.ipynb | Simple and searched CASE |
| 03 | 03_if_then_else_statement.ipynb | IF / ELSEIF / ELSE |
| 04 | 04_for_statement.ipynb | FOR over query results, LEAVE |
| 05 | 05_while_statement.ipynb | WHILE, ITERATE |
| 06 | 06_loop_statement.ipynb | LOOP with LEAVE/ITERATE |
| 07 | 07_repeat_statement.ipynb | REPEAT UNTIL |
| 08 | 08_leave_and_iterate_statements.ipynb | LEAVE and ITERATE with workshop examples |
| 09 | 09_create_procedure.ipynb | CREATE PROCEDURE, CALL, IN/OUT parameters |
| 10 | 10_get_diagnostics_statement.ipynb | GET DIAGNOSTICS in condition handlers |
| 11 | 11_signal_statement.ipynb | SIGNAL for custom conditions |
| 12 | 12_resignal_statement.ipynb | RESIGNAL in handlers |

**Reference:** [SQL scripting - Azure Databricks - Databricks SQL](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-scripting) and linked control-flow docs.
