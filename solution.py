from pyspark.sql.functions import col, when, current_timestamp
from pyspark.sql.types import IntegerType, BooleanType

table_name = "workspace.default.source_data"


bronze_df = (
    spark.table(table_name)
    .withColumn("ingestion_timestamp", current_timestamp())
)

bronze_df.createOrReplaceTempView("bronze_engine_data")
display(bronze_df)



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
