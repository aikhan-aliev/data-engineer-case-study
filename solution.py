from pyspark.sql.functions import col, when, current_timestamp
from pyspark.sql.types import IntegerType, BooleanType

table_name = "workspace.default.source_data"

# Bronze Layer
bronze_df = (
    spark.table(table_name)
    .withColumn("ingestion_timestamp", current_timestamp())
)

bronze_df.createOrReplaceTempView("bronze_engine_data")
display(bronze_df)



# Silver Layer
spark.conf.set("spark.sql.ansi.enabled", "false")

silver_df = spark.table("bronze_engine_data")

valid_issue_types = ['typical', 'atypical', 'non-related', 'non-symptomatic']

silver_df = (
    silver_df
    .withColumn("oph", col("oph").cast(IntegerType()))
    .withColumn("pist_m", col("pist_m").cast(IntegerType()))
    .withColumn("bmep", col("bmep").cast(IntegerType()))
    .withColumn("ng_imp", col("ng_imp").cast(IntegerType()))
    .withColumn("past_dmg", col("past_dmg").cast(BooleanType()))
    .withColumn("rpm_max", col("rpm_max").cast(IntegerType()))
    .withColumn("full_load_issues", col("full_load_issues").cast(BooleanType()))
    .withColumn("number_up", col("number_up").cast(IntegerType()))
    .withColumn("number_tc", col("number_tc").cast(IntegerType()))
    .withColumn("high_breakdown_risk", col("high_breakdown_risk").cast(BooleanType()))
    
    .withColumn("resting_analysis_results", col("resting_analysis_results").cast(IntegerType()))
    .withColumn("resting_analysis_results_desc", 
                when(col("resting_analysis_results") == 0, "normal")
               .when(col("resting_analysis_results") == 1, "abnormal")
               .when(col("resting_analysis_results") == 2, "critical")
               .otherwise("unknown"))
)

silver_df = (
    silver_df
    .filter(col("issue_type").isin(valid_issue_types))
    .filter(col("oph").isNotNull() & col("issue_type").isNotNull())
)

silver_df = silver_df.drop("op_set_2", "op_set_3")

silver_df.createOrReplaceTempView("silver_engine_data")
display(silver_df)



# Gold Layer
gold_df = spark.table("silver_engine_data")

gold_df = gold_df.filter(col("oph") <= 120000)

gold_df = gold_df.select(
    "oph", "pist_m", "issue_type", "bmep", "ng_imp", 
    "past_dmg", "resting_analysis_results_desc", "rpm_max", 
    "full_load_issues", "number_up", "number_tc", "op_set_1", "high_breakdown_risk"
)

gold_df.createOrReplaceTempView("gold_engine_data")
display(gold_df)


# dashboard showcasing stats about the data or it's issues


# 1. average pressure and operating hours per engine issue

display(spark.sql("""
select 
  issue_type,
  avg(bmep) as avg_bmep,
  avg(oph) as avg_oph,
  count(*) as total_occurrences
from gold_engine_data
group by issue_type
order by total_occurrences desc
"""))
# 2. Engine Risk Correlation against Resting Results

display(spark.sql("""
select 
  resting_analysis_results_desc,
  high_breakdown_risk,
  count(*) as total_engines
from gold_engine_data
group by resting_analysis_results_desc, high_breakdown_risk
order by resting_analysis_results_desc, high_breakdown_risk
"""))