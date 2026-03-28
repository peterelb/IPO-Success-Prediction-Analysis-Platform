from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/datasets/transformed/sp500_stock_companies_transformed"),
    stock_df=Input("ri.foundry.main.dataset.5365c0bf-257f-477b-b0a0-09517ce37c22"),
    companies_df=Input("ri.foundry.main.dataset.0b7054a4-7ec3-4c9d-8049-00e4939c7602"),
    
)
def compute(stock_df,companies_df):
    companies_slim = companies_df.select(
        "ticker_name", "sector_name", "industry", "market_cap", "weight"
    )
    joined_df =stock_df.join(companies_slim, on="ticker_name", how="left")

    joined_df = joined_df.dropna(subset=["sector_name"])
    
    return joined_df

