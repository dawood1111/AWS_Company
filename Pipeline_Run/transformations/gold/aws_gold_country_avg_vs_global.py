from pyspark import pipelines as dp
from pyspark.sql.functions import avg, round, when, lit, col


@dp.materialized_view(
    name="aws_gold_country_avg_vs_global",
    comment="Average number of employees per country compared with the global average",
    table_properties={"delta.columnMapping.mode": "name", "delta.minReaderVersion": "2", "delta.minWriterVersion": "5"},
)
def aws_gold_country_avg_vs_global():
    df = spark.read.table("aws_organization_silver")
    avg_per_country = df \
        .groupBy("Country") \
        .agg(avg("Number of employees").alias("Average Number of Employees Per Country"))
    avg_globally = df \
        .agg(avg("Number of employees").alias("Average Globaly"))

    return (
        avg_per_country.crossJoin(avg_globally)
        .withColumn("the diference", round(col("Average Number of Employees Per Country") - col("Average Globaly"), 2))
        .withColumn("Position", when(col("Average Number of Employees Per Country") >
            col("Average Globaly"), lit("Above Average")).otherwise(lit("Below Average")))
        .withColumn("Average Number of Employees Per Country", round(col("Average Number of Employees Per Country"), 2))
        .withColumn("Average Globaly", round(col("Average Globaly"), 2))
    )
