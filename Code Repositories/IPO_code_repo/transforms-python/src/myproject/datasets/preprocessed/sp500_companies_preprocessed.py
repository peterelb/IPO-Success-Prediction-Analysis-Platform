from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/datasets/preprocessed/sp500_companies_preprocessed"),
    source_df=Input("ri.foundry.main.dataset.bce5baf9-e390-48ac-8b8e-c1f34106a312"),
)
def compute(source_df):
    source_df = source_df.dropna(subset=["Revenuegrowth","State","Fulltimeemployees"]).drop("Shortname")
    source_df = (
        source_df
        .withColumnRenamed("Weight", "weight")
        .withColumnRenamed("Exchange", "exchange")
        .withColumnRenamed("Symbol", "ticker_name")
        .withColumnRenamed("Longname", "company_name")
        .withColumnRenamed("Sector", "sector_name")
        .withColumnRenamed("Industry", "industry")
        .withColumnRenamed("Currentprice", "current_price")
        .withColumnRenamed("Marketcap", "market_cap")
        .withColumnRenamed("Revenuegrowth", "revenue_growth")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("Fulltimeemployees", "full_time_employees")
        .withColumnRenamed("Longbusinesssummary", "long_business_summary")
    )
    

    return source_df
