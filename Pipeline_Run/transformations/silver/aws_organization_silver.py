from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    when,
    year,
    current_date,
    regexp_extract,
    split,
    size,
    trim,
    concat_ws,
    slice,
)


@dp.materialized_view(
    name="aws_organization_silver",
    comment="Silver AWS organization data: deduplicated bronze records with company size, company age, website domain, and split industries",
    table_properties={"delta.columnMapping.mode": "name", "delta.minReaderVersion": "2", "delta.minWriterVersion": "5"},
)
def aws_organization_silver():
    industry_parts = split(col("Industry"), r"\s*/\s*")
    df = spark.read.table("dawood.aws_organization_schema.aws_organization_table")
    return (
        df.withColumn("Company Size",
            when(col("Number of employees") <= 1000, "Small")
            .when((col("Number of employees") > 1000) & (col("Number of employees") <= 3000), "Medium")
            .when(col("Number of employees") > 3000, "Enterprise").otherwise("Unknown"))
        .withColumn("Company Age", year(current_date()) - col("Founded"))
        .withColumn("Website", regexp_extract(col("Website"), r"^https?://(?:www\.)?([^/]+)", 1))
        .withColumn("Main_industry", trim(industry_parts[0]))
        .withColumn("Sub_industry", when(size(industry_parts) == 0, "Empty").otherwise(
            concat_ws(", ", slice(industry_parts, 2, 100))))
        .dropDuplicates(subset=["Index"])
        .drop("_rescued_data", "Industry")
    )
