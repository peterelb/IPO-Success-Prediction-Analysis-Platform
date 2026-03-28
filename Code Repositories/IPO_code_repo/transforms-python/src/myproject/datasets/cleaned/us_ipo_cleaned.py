from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("ri.foundry.main.dataset.19edae55-3530-46dc-9ab8-8cd24257f929"),
    source_df=Input("ri.foundry.main.dataset.def8e9bc-5850-4d45-bd96-a617f864cc2a"),
)
def compute(source_df):
    source_df = source_df.withColumn(
    "sector_name",
    F.when(F.lower(F.col("industry_name")) == "it", "Technology")
     .when(F.lower(F.col("industry_name")) == "health", "Healthcare")
     .when(F.lower(F.col("industry_name")) == "finance", "Financial Services")
     .when(F.lower(F.col("industry_name")).isin("energy", "oil"), "Energy")
     .when(F.lower(F.col("industry_name")).isin("machinery", "engineering"), "Industrials")
     .when(F.lower(F.col("industry_name")).isin("retail", "restaurant"), "Consumer Goods")
     .when(F.lower(F.col("industry_name")).isin("chemical", "metal", "mining"), "Materials")
     .when(F.lower(F.col("industry_name")) == "agriculture", "Agriculture")
     .when(F.lower(F.col("industry_name")) == "communications", "Communication & Media")
     .when(F.lower(F.col("industry_name")).isin("consulting", "services"), "Professional Services")
     .otherwise("Unknown")
    ).drop("industry_name")

    return source_df
