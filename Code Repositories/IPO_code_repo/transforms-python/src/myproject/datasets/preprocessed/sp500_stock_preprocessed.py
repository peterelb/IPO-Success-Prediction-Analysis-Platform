from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/datasets/preprocessed/sp500_stock_preprocessed"),
    source_df=Input("ri.foundry.main.dataset.edcf7ca9-2de9-4f68-b661-02b7abe8dfdc"),
)
def compute(source_df):
    source_df = (
        source_df
        .withColumnRenamed("Adj_Close", "adj_close")
        .withColumnRenamed("Close", "close")
        .withColumnRenamed("High", "high")
        .withColumnRenamed("Low", "low")
        .withColumnRenamed("Open", "open")
        .withColumnRenamed("Volume", "volume")
        .withColumnRenamed("Symbol", "ticker_name")
        .withColumnRenamed("Date", "date")
        .withColumn("date", F.to_date(
            F.regexp_replace(F.col("date"), "^\\w+,\\s*", ""),
            "MMMM d, yyyy"
        ))
        .withColumn("adj_close", F.round("adj_close", 1))
        .withColumn("close", F.round("close", 1))
        .withColumn("high", F.round("high", 1))
        .withColumn("low", F.round("low", 1))
        .withColumn("open", F.round("open", 1))
    ).dropna(subset=["volume"])

    return source_df