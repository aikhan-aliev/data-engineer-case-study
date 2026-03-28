from pyspark.sql.functions import col, when, current_timestamp
from pyspark.sql.types import IntegerType, BooleanType

table_name = "workspace.default.source_data"


bronze_df = (
    spark.table(table_name)
    .withColumn("ingestion_timestamp", current_timestamp())
)

bronze_df.createOrReplaceTempView("bronze_engine_data")
display(bronze_df)
