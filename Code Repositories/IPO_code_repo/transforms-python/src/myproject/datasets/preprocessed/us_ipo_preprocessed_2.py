from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/datasets/preprocessed/us_ipo_preprocessed_2"),
    source_df=Input("ri.foundry.main.dataset.7629dc9f-1a03-49b1-87cb-919e29caa4c5"),
)
def compute(source_df):
    source_df = source_df.drop_duplicates(["Company_Name"])
    source_df = source_df.dropna(subset= ["US_state"]).drop("year","industry","is_synthetic")

    price_cols = [
    "firstday_adjclose", "firstday_open",
    "inmonth_adjclose", "inmonth_open",
    "inweek_adjclose", "inweek_open",
    "inyear_adjclose", "inyear_open",
    "CEO_pay","Offer_Amount",
]

    for col_name in price_cols:
      source_df= source_df.withColumn(col_name, F.round(F.col(col_name), 2))

    source_df = source_df.withColumnRenamed("Company_Name", "company_name") \
       .withColumnRenamed("Symbol", "ticker_name") \
       .withColumnRenamed("Market", "exchange_name") \
       .withColumnRenamed("Price", "ipo_price_usd_per_share") \
       .withColumnRenamed("Shares", "shares") \
       .withColumnRenamed("Offer_Amount", "ipo_amount_millions") \
       .withColumnRenamed("Date_Priced", "ipo_date") \
       .withColumnRenamed("US_state", "state_name") \
       .withColumnRenamed("employees2019", "employees_in_2019") \
       .withColumnRenamed("CEO_pay", "ceo_pay") \
       .withColumnRenamed("CEO_born", "ceo_born")\
       .withColumnRenamed("sector","industry_name")

    source_df = source_df.dropna(subset=["ceo_born","ceo_pay","employees_in_2019","industry_name","address","employees","ticker_name"])
    return source_df
