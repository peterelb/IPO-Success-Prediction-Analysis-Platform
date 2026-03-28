from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window
from transforms.api import transform_df, Input, Output
 
 
@transform_df(
    Output("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/datasets/preprocessed/ml_preprocessed"),
    df=Input("ri.foundry.main.dataset.f528e8bd-c06b-4dd6-ac03-a71581178d72"),
)
def compute(df):
 
    # ── 1. Fix column names: strip whitespace ────────────────
    df = df.toDF(*[c.strip() for c in df.columns])
 
    # ── 2. Remove exact duplicate rows ───────────────────────
    df = df.dropDuplicates()
 
    # ── 3. Remove duplicate symbols (keep first by ipoDate) ─
    w = Window.partitionBy("Symbol").orderBy("ipoDate")
    df = df.withColumn("_rn", F.row_number().over(w)) \
           .filter(F.col("_rn") == 1) \
           .drop("_rn")
 
    # ── 4. Decode HTML entities in Name ──────────────────────
    df = df.withColumn("Name", F.regexp_replace("Name", "&#39;", "'"))
    df = df.withColumn("Name", F.regexp_replace("Name", "&amp;", "&"))
    df = df.withColumn("Name", F.regexp_replace("Name", "&lt;", "<"))
    df = df.withColumn("Name", F.regexp_replace("Name", "&gt;", ">"))
 
    # ── 5. Strip whitespace from all string columns ──────────
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col_name in string_cols:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))
 
    # ── 6. Replace dash placeholders with null, cast numeric ─
    df = df.withColumn("lastFiscalYearGrowth",
        F.when(F.col("lastFiscalYearGrowth") == "-", None)
         .otherwise(F.col("lastFiscalYearGrowth"))
         .cast(DoubleType())
    )
    df = df.withColumn("employees",
        F.when(F.col("employees") == "-", None)
         .otherwise(F.col("employees"))
         .cast(DoubleType())
    )
 
    # ── 7. Fix YearFounded outlier (value 12 → null) ────────
    df = df.withColumn("YearFounded",
        F.when(F.col("YearFounded") < 1600, None)
         .otherwise(F.col("YearFounded"))
    )
 
    # ── 8. Set MarketCap == 0 to null ────────────────────────
    df = df.withColumn("MarketCap",
        F.when(F.col("MarketCap") == 0, None)
         .otherwise(F.col("MarketCap"))
    )
 
    # ── 9. Replace 'Blank' / 'Unknown' sentinels with null ──
    blank_cols = [
        "employeesGrouped", "usableCEOAge", "usableCEOGender",
        "usablePresidentAge", "usablePresidentGender"
    ]
    for col_name in blank_cols:
        df = df.withColumn(col_name,
            F.when(F.col(col_name).isin("Blank", "Unknown"), None)
             .otherwise(F.col(col_name))
        )
 
    df = df.withColumn("FiscalMonth",
        F.when(F.col("FiscalMonth") == "Unknown", None)
         .otherwise(F.col("FiscalMonth"))
    )
    df = df.withColumn("USACompany",
        F.when(F.col("USACompany") == "Unknown", None)
         .otherwise(F.col("USACompany"))
    )
 
    # ── 10. Convert FiscalDateEnd to proper timestamp ────────
    df = df.withColumn("FiscalDateEnd", F.to_timestamp("FiscalDateEnd"))
 
    # ── 11. Drop uninformative columns ───────────────────────
    drop_cols = [
        "ADR TSO",                                        # 97% missing
        "Fiscal_year_ends_in_December_USDYearBeforeIPO",  # 87% missing
        "yearDifferenceGrouped",                          # 100% "Unknown"
        "Summary Quote",                                  # URLs, not useful
        "HomeRunDay"
    ]
    existing_drops = [c for c in drop_cols if c in df.columns]
    df = df.drop(*existing_drops)
 
    return df