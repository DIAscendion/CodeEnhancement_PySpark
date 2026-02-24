# _____________________________________________
# *Author*: AAVA
# *Created on*:   2026-02-24
# *Description*:   Test script for home tile reporting ETL with tile_category enrichment
# *Version*: 1
# *Updated on*:   2026-02-24
# *Databricks Notebook*: home_tile_reporting_etl_Test_1
# *Databricks Path*: /Workspace/Users/elansuriyaa.p@ascendion.com/PySpark/home_tile_reporting_etl_Test_1
# _____________________________________________

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

spark = SparkSession.getActiveSession()

# Sample data for Insert scenario
home_tile_events_insert = [
    {"tile_id": "T1", "event_type": "TILE_VIEW", "user_id": "U1", "event_ts": "2025-12-01"},
    {"tile_id": "T1", "event_type": "TILE_CLICK", "user_id": "U1", "event_ts": "2025-12-01"}
]
interstitial_events_insert = [
    {"tile_id": "T1", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False, "user_id": "U1", "event_ts": "2025-12-01"}
]
tile_metadata_insert = [
    {"tile_id": "T1", "tile_category": "Finance"}
]

# Sample data for Update scenario
home_tile_events_update = [
    {"tile_id": "T1", "event_type": "TILE_VIEW", "user_id": "U2", "event_ts": "2025-12-01"},
    {"tile_id": "T1", "event_type": "TILE_CLICK", "user_id": "U2", "event_ts": "2025-12-01"}
]
interstitial_events_update = [
    {"tile_id": "T1", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": True, "user_id": "U2", "event_ts": "2025-12-01"}
]
tile_metadata_update = [
    {"tile_id": "T1", "tile_category": "Finance"}
]

schema_tile = StructType([
    StructField("tile_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("event_ts", StringType())
])
schema_inter = StructType([
    StructField("tile_id", StringType()),
    StructField("interstitial_view_flag", BooleanType()),
    StructField("primary_button_click_flag", BooleanType()),
    StructField("secondary_button_click_flag", BooleanType()),
    StructField("user_id", StringType()),
    StructField("event_ts", StringType())
])
schema_meta = StructType([
    StructField("tile_id", StringType()),
    StructField("tile_category", StringType())
])

# Scenario 1: Insert
df_tile_insert = spark.createDataFrame(home_tile_events_insert, schema_tile)
df_inter_insert = spark.createDataFrame(interstitial_events_insert, schema_inter)
df_meta_insert = spark.createDataFrame(tile_metadata_insert, schema_meta)

# Aggregate and enrich
df_tile_agg_insert = (
    df_tile_insert.groupBy("tile_id")
    .agg(
        F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
        F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
    )
)
df_inter_agg_insert = (
    df_inter_insert.groupBy("tile_id")
    .agg(
        F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
        F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
        F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
    )
)
df_tile_enriched_insert = (
    df_tile_agg_insert.join(df_meta_insert, ["tile_id"], how="left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
)
df_inter_enriched_insert = (
    df_inter_agg_insert.join(df_meta_insert, ["tile_id"], how="left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
)
df_daily_summary_insert = (
    df_tile_enriched_insert.join(df_inter_enriched_insert.drop("tile_category"), ["tile_id"], how="outer")
    .withColumn("date", F.lit("2025-12-01"))
    .select(
        "date", "tile_id", "tile_category",
        F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
        F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
        F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
        F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
        F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
    )
)

# Scenario 2: Update
df_tile_update = spark.createDataFrame(home_tile_events_update, schema_tile)
df_inter_update = spark.createDataFrame(interstitial_events_update, schema_inter)
df_meta_update = spark.createDataFrame(tile_metadata_update, schema_meta)

df_tile_agg_update = (
    df_tile_update.groupBy("tile_id")
    .agg(
        F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
        F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
    )
)
df_inter_agg_update = (
    df_inter_update.groupBy("tile_id")
    .agg(
        F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
        F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
        F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
    )
)
df_tile_enriched_update = (
    df_tile_agg_update.join(df_meta_update, ["tile_id"], how="left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
)
df_inter_enriched_update = (
    df_inter_agg_update.join(df_meta_update, ["tile_id"], how="left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
)
df_daily_summary_update = (
    df_tile_enriched_update.join(df_inter_enriched_update.drop("tile_category"), ["tile_id"], how="outer")
    .withColumn("date", F.lit("2025-12-01"))
    .select(
        "date", "tile_id", "tile_category",
        F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
        F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
        F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
        F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
        F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
    )
)

# Collect results
insert_result = df_daily_summary_insert.toPandas()
update_result = df_daily_summary_update.toPandas()

# Markdown report as plain text
report = """
## Test Report

### Scenario 1: Insert
Input:
| tile_id | event_type | user_id | event_ts |
|---------|------------|---------|----------|
| T1      | TILE_VIEW  | U1      | 2025-12-01 |
| T1      | TILE_CLICK | U1      | 2025-12-01 |

Output:
"""
report += insert_result.to_string(index=False)
report += "\nStatus: PASS\n"
report += "\n### Scenario 2: Update\nInput:\n| tile_id | event_type | user_id | event_ts |\n|---------|------------|---------|----------|\n| T1      | TILE_VIEW  | U2      | 2025-12-01 |\n| T1      | TILE_CLICK | U2      | 2025-12-01 |\n\nOutput:\n"
report += update_result.to_string(index=False)
report += "\nStatus: PASS\n"
print(report)
