from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/datasets/transformed/sp500_prices_with_index"),
    stock_df=Input("ri.foundry.main.dataset.43e6b1dd-3cae-400c-96d6-898fa6b4f1b5"),
    index_df=Input("ri.foundry.main.dataset.7e413ab2-790b-4b3d-9c28-64dabc4b8065"),
)

def compute(stock_df, index_df):
    index_slim = index_df.select(
        "date",
        F.col("sp_500").alias("index_value")
    )
    
    final_df = stock_df.join(index_slim, on="date", how="left")
    final_df = final_df.dropna(subset=["index_value"])

    return final_df