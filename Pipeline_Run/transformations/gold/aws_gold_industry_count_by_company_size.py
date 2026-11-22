from pyspark import pipelines as dp


@dp.materialized_view(
    name="aws_gold_industry_count_by_company_size",
    comment="Number of companies for each industry, pivoted by company size",
    table_properties={"delta.columnMapping.mode": "name", "delta.minReaderVersion": "2", "delta.minWriterVersion": "5"},
)
def aws_gold_industry_count_by_company_size():
    return (
        spark.read.table("aws_organization_silver")
        .groupBy("Main_industry")
        .pivot("Company Size")
        .count()
    )
