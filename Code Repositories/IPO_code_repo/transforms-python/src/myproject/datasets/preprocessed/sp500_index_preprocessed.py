# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/datasets/preprocessed/sp500_index_preprocessed"),
    source_df=Input("ri.foundry.main.dataset.abb5f937-a228-40bb-a19b-597935af05ab"),
)
def compute(source_df):
    source_df = source_df.withColumnRenamed("Date","date")\
    .withColumnRenamed("SP500","sp_500")
    return source_df
