# Engine Failures Data Pipeline

This is my solution for the Data Analyst assignment. The goal is to clean up engine failure data and get it ready for business analysis.

I used PySpark and Databricks to process the data. The code follows the Medallion Architecture (Bronze, Silver, Gold).

## Files in this Repository
- `solution.py`: My PySpark code for cleaning the data and the SQL dashboard queries.
- `source_data.csv`: The original data file.
- `business_description.txt` & `task.md`: The assignment instructions.

## How to Run It
1. Upload `source_data.csv` to your Databricks Workspace (the code expects it to be a table named `workspace.default.source_data`).
2. Import `solution.py` into Databricks.
3. Attach your cluster and run all the cells.

## How the Code Works

### Bronze Layer
This step simply imports the raw data exactly as it is. I loaded everything as string columns so we don't accidentally lose any rows with bad data. I also added a timestamp column to show when the data was ingested.

### Silver Layer
This is where I cleaned the data:
- **Changing Types**: I had to turn off Databricks' ANSI SQL mode because some rows had bad text (like "kkkkk" instead of numbers). Turning it off let the `.cast()` function safely change those bad strings into `null` values instead of throwing an error.
- **Filtering**: I removed rows that were missing important info. I also dropped rows if their `issue_type` wasn't one of the four types allowed in the instructions.
- **Cleaning Columns**: I changed the `resting_analysis_results` numbers into normal words (normal, abnormal, critical) so it's easier to read. I also deleted the `op_set_2` and `op_set_3` columns because they were completely empty.

### Gold Layer
This layer is the final table for analysts:
- I applied the business rule to keep only records where `oph <= 120000`. This successfully removed a broken row that had a billion operating hours.
- Finally, I kept only the useful columns that analysts actually need for dashboards.

## Dashboards
At the end of the script, there are two SQL queries wrapped in PySpark `spark.sql()` commands. Because they are enclosed in the `display()` function, they will automatically run alongside the rest of the Python code and output clean, formatted tables.
