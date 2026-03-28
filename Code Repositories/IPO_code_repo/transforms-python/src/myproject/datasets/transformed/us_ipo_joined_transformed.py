from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("ri.foundry.main.dataset.def8e9bc-5850-4d45-bd96-a617f864cc2a"),
    ipo_two=Input("ri.foundry.main.dataset.8b2797e9-2c07-4e2b-bb88-56cacb0dca56"),
    ipo_one=Input("ri.foundry.main.dataset.beb1ee79-b505-48bd-865a-1a0fb7fa1047"),
)
def compute(ipo_two,ipo_one):

    shared_cols = [c for c in ipo_one.columns if c in ipo_two.columns and c != "ticker_name"]

    ipo_joined = ipo_one.join(
        ipo_two.drop(*shared_cols),
        on="ticker_name",
        how="inner"
    )

    ipo_joined = ipo_joined.drop_duplicates(subset=["ticker_name"])

    return ipo_joined
