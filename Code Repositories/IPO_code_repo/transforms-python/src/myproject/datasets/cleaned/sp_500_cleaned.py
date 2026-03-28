from pyspark.sql import functions as F
from pyspark.sql import Window
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/datasets/cleaned/sp_500_cleaned"),
    source_df=Input("ri.foundry.main.dataset.ea44bdab-5faa-47fe-ad39-f63ec028099b"),
)
def compute(source_df):
    window = Window.partitionBy("ticker_name").orderBy("date")

    source_df = (
        source_df
        .withColumn("prev_close", F.lag("close", 1).over(window))
        .withColumn("daily_return", F.round(
            (F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100, 2
        ))
        .drop("prev_close")
        .withColumn("year",    F.year("date"))
        .withColumn("month",   F.month("date"))
        .withColumn("quarter", F.quarter("date"))
    )
    source_df = source_df.withColumn("stock_date_id", F.concat_ws("_", F.col("ticker_name"), F.col("date").cast("string")))

    return source_df