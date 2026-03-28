from pyspark.sql import functions as F
from transforms.api import transform, Input, Output


# ── All feature columns to keep for modeling ─────────────────────
PRE_IPO_FEATURES = [
    "LastSale", "MarketCap", "logMarketCap",
    "Revenue_M", "netIncome_M", "profitMargin",
    "lastFiscalYearGrowth",
    "employees", "revenuePerEmployee", "marketCapToRevenue",
    "CEOAge", "PresidentAge",
    "companyAgeAtIPO", "yearDifference", "YearFounded",
    "MarketMonthTrend", "Market3MonthTrend",
    "Market6MonthTrend", "MarketYearTrend",
    "Year", "Month", "Day", "dayOfWeek",
    "Sector", "Industry",
    "CEOGender", "PresidentGender",
    "USACompany",
    "CEOInChargeDuringIPO", "presidentInChargeDuringIPO",
    "FiscalMonth",
    "employeesGrouped", "FoundingDateGrouped",
]

POST_IPO_FEATURES = [
    "ipoDay0Return", "firstWeekReturn", "firstMonthReturn",
    "ipoDay0Volatility", "avgFirstWeekVolume",
    "closeDay0", "openDay0", "highDay0", "lowDay0", "volumeDay0",
    "closeDay1", "closeDay2", "closeDay3", "closeDay4",
    "volumeDay1", "volumeDay2", "volumeDay3", "volumeDay4",
]

TARGET = "Profitable"


@transform(
    cleaned_input=Input("ri.foundry.main.dataset.9082b381-dc74-45ae-b822-53f6a69a1898"),
    training_output=Output("ri.foundry.main.dataset.6d034bd9-e32f-42bf-9b48-41d131d3bfbb"),
    testing_output=Output("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/datasets/machinelearning/ml_testing"),
)
def compute(cleaned_input, training_output, testing_output):
    df = cleaned_input.dataframe()

    all_cols = PRE_IPO_FEATURES + POST_IPO_FEATURES + [TARGET]
    existing_cols = [c for c in all_cols if c in df.columns]
    df = df.select(*existing_cols)

    df = df.filter(F.col(TARGET).isNotNull())

    training_data, testing_data = df.randomSplit([0.8, 0.2], seed=42)

    training_output.write_dataframe(training_data)
    testing_output.write_dataframe(testing_data)
