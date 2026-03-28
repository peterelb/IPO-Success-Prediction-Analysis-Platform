from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/datasets/preprocessed/us_ipo_preprocessed"),
    source_df=Input("ri.foundry.main.dataset.d86ddcad-8b98-48b1-9703-4a7e54d12739"),
)
def compute(source_df):
    #Rename columns
    source_df = (
        source_df
        .withColumnRenamed("company", "company_name")
        .withColumnRenamed("state", "state_name")
        .withColumnRenamed("exchange", "exchange_name")
        .withColumnRenamed("industry", "industry_name")
        .withColumnRenamed("ticker", "ticker_name")
        .withColumnRenamed("profit_MUSD", "profit_millions")
        .withColumnRenamed("debt_MUSD", "debt_millions")
        .withColumnRenamed("assets_MUSD", "assets_millions")
        .withColumnRenamed("ipo_fees_MUSD", "ipo_fees_millions")
        .withColumnRenamed("ipo_price_USDshare", "ipo_price_usd_per_share") 
        .withColumnRenamed("ipo_amount_MUSD", "ipo_amount_millions")
        )

    #Remove duplicates/nulls
    source_df = source_df.dropDuplicates(["company_name","ticker_name"]).dropna(subset=["ticker_name"])

    #Negative values in profit to 0
    source_df = source_df.withColumn("profit_millions", F.when(F.col("profit_millions")<0,0).otherwise(F.col("profit_millions")))

    
    #Round columns
    cols_to_round = ["debt_millions", "assets_millions", "ipo_fees_millions", 
                 "ipo_price_usd_per_share", "ipo_amount_millions"]
    
    for c in cols_to_round:
        source_df = source_df.withColumn(c, F.round(F.col(c), 1))

    source_df=source_df.drop("is_synthetic").dropna(subset=["ipo_date"])

    return source_df
