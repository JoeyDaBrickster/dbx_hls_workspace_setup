# Part 3 – Query Parameter

## Objective

Add a **Year** parameter to a SQL query and refresh results by changing the parameter value.

## Prerequisites

- SQL Editor and a query that filters by year (or use the notebook **03_query_parameter.ipynb**).

## Instructions

1. Create a **new** query (or open the one from Part 2).
2. Add a **query parameter** named **Year** (type: integer). In Databricks SQL: Query settings → Parameters → Add parameter.
3. In the WHERE clause, filter by `YEAR(claim_start_date) = ${Year}` (or the parameter syntax your workspace uses).
4. Run the query and enter a year (e.g. 2020).
5. Change the parameter to another year and run again; confirm the result set updates.

## Success criteria

- The query accepts a Year parameter and filters results by that year.
- Changing the parameter changes the results.
