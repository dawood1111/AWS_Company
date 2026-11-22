from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum, round, concat, lit
from pyspark.sql.window import Window


@dp.materialized_view(
    name="aws_gold_country_employee_percentage",
    comment="Percentage of all employees working in each country",
    table_properties={"delta.columnMapping.mode": "name", "delta.minReaderVersion": "2", "delta.minWriterVersion": "5"},
)
def aws_gold_country_employee_percentage():
    df = spark.read.table("aws_organization_silver")
    df = df \
        .groupBy("Country") \
        .agg(sum("Number of employees").alias("Total Number of Employees"))

    return (
        df \
        .withColumn("Total employees in all countries", sum(col("Total Number of Employees")).over(Window.partitionBy()).alias("Total employes")) \
        .withColumn("Percentage", concat(round(col("Total Number of Employees") / col("Total employees in all countries") * 100, 2).cast("string"), lit("%")))
    )
