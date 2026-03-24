# Data Analyst Assignment

## Business goal

Serve analysts and business with observations about engine failures while ensuring data consistency. You should end up with 3 tables in line with the medallion architecture, where in our case gold layer is the semantic one.

Extra: create a dashboard showcasing stats about the data or it's issues

Data source: `source_data.csv`

## Data model

Bronze object requirements:
- all records and columns ingested

Silver object requirements:
- contains only full records
- contains only valid records
- column names expanded where necessary

Gold objects requirements:
- keeps columns that contain information
- keeps records that passed input **and** business tests

## Quality requirements

Input tests:
- records with mismatching types (see description)
- records with invalid values (see description)
- records with missing values

Business tests:
- OPH <= 120000

## Tools and turn-in

Register for a free account at [Databricks](https://www.databricks.com/learn/free-edition).

You should structre your solution in a single workbook and work with the language of your choice (preferrably python/pySpark/SQL). You should use databricks dashboards if you chose to perform the extra task. Make a separate `README.md` file explaining how to execute your solution. You can use any tools, but have to understand your end product (don't be sloppy!). Details of the implementation are up to you.

You should upload your solution to a **public** repo of your choice (e.g. [GitHub](https://github.com)) and share only the link pointing to it.


_Have fun!_