from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum, dense_rank, desc
from pyspark.sql.window import Window


@dp.materialized_view(
    name="aws_gold_top_industries_per_country",
    comment="Top 3 industries per country by total number of employees",
    table_properties={"delta.columnMapping.mode": "name", "delta.minReaderVersion": "2", "delta.minWriterVersion": "5"},
)
def aws_gold_top_industries_per_country():
    df = spark.read.table("aws_organization_silver")
    return (
        df.groupBy("Main_industry", "Country")
        .agg(sum("Number of employees").alias("Total Number of Employees"))
        .withColumn("Industry Rank", dense_rank().over(Window.partitionBy("Country").orderBy(desc("Total Number of Employees"))))
        .filter(col("industry rank") <= 3)
    )
