from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from transforms.api import transform_df, Input, Output
 
 
@transform_df(
    Output("ri.foundry.main.dataset.9082b381-dc74-45ae-b822-53f6a69a1898"),
    cleaned_df=Input("ri.foundry.main.dataset.51e3e7ed-d0e4-44cf-a3d5-3041194f2335"),
)
def compute(cleaned_df):
    df = cleaned_df
 
    # ── 1. Parse Revenue string → numeric (millions) ────────
    df = df.withColumn("Revenue_clean", F.regexp_replace("Revenue", r"[\$,]", ""))
    df = df.withColumn("Revenue_M",
        F.when(F.col("Revenue_clean").endswith("B"),
            F.regexp_replace("Revenue_clean", "B$", "").cast(DoubleType()) * 1000
        ).when(F.col("Revenue_clean").endswith("M"),
            F.regexp_replace("Revenue_clean", "M$", "").cast(DoubleType())
        ).when(F.col("Revenue_clean").endswith("K"),
            F.regexp_replace("Revenue_clean", "K$", "").cast(DoubleType()) / 1000
        ).otherwise(
            F.col("Revenue_clean").cast(DoubleType())
        )
    ).drop("Revenue_clean")
 
    # ── 2. Parse netIncome string → numeric (millions) ───────
    df = df.withColumn("netIncome_clean", F.regexp_replace("netIncome", r"[\$,]", ""))
    df = df.withColumn("netIncome_M",
        F.when(F.col("netIncome_clean").endswith("B"),
            F.regexp_replace("netIncome_clean", "B$", "").cast(DoubleType()) * 1000
        ).when(F.col("netIncome_clean").endswith("M"),
            F.regexp_replace("netIncome_clean", "M$", "").cast(DoubleType())
        ).when(F.col("netIncome_clean").endswith("K"),
            F.regexp_replace("netIncome_clean", "K$", "").cast(DoubleType()) / 1000
        ).otherwise(
            F.col("netIncome_clean").cast(DoubleType())
        )
    ).drop("netIncome_clean")
 
    # Drop original string columns
    df = df.drop("Revenue", "netIncome")
 
    # ── 3. Recalculate yearDifference ────────────────────────
    df = df.withColumn("yearDifference",
        F.year(F.col("ipoDate")) - F.col("YearFounded")
    )
 
    # ── 4. Consolidate gender categories ─────────────────────
    for col_name in ["CEOGender", "PresidentGender", "usableCEOGender", "usablePresidentGender"]:
        df = df.withColumn("_gc", F.lower(F.trim(F.col(col_name)))) \
               .withColumn(col_name,
                   F.when(F.col("_gc").isNull(), None)
                    .when(F.col("_gc") == "male", "male")
                    .when(F.col("_gc") == "female", "female")
                    .when(F.col("_gc") == "mostly_male", "male")
                    .when(F.col("_gc") == "mostly_female", "female")
                    .when(F.col("_gc") == "andy", "unknown")
                    .otherwise("unknown")
               ).drop("_gc")
 
    # ── 5. Profit margin ────────────────────────────────────
    df = df.withColumn("profitMargin",
        F.when(
            (F.col("Revenue_M").isNotNull()) & (F.col("Revenue_M") != 0),
            F.round(F.col("netIncome_M") / F.col("Revenue_M"), 4)
        )
    )
 
    # ── 6. IPO day return ───────────────────────────────────
    df = df.withColumn("ipoDay0Return",
        F.when(
            (F.col("openDay0").isNotNull()) & (F.col("openDay0") != 0),
            F.round((F.col("closeDay0") - F.col("openDay0")) / F.col("openDay0"), 4)
        )
    )
 
    # ── 7. First-week return (Day0 close → Day4 close) ──────
    df = df.withColumn("firstWeekReturn",
        F.when(
            (F.col("closeDay0").isNotNull()) & (F.col("closeDay0") != 0),
            F.round((F.col("closeDay4") - F.col("closeDay0")) / F.col("closeDay0"), 4)
        )
    )
 
    # ── 8. First-month return (Day0 close → Day20 close) ────
    df = df.withColumn("firstMonthReturn",
        F.when(
            (F.col("closeDay0").isNotNull()) & (F.col("closeDay0") != 0),
            F.round((F.col("closeDay20") - F.col("closeDay0")) / F.col("closeDay0"), 4)
        )
    )
 
    # ── 9. IPO day volatility (high-low range / open) ───────
    df = df.withColumn("ipoDay0Volatility",
        F.when(
            (F.col("openDay0").isNotNull()) & (F.col("openDay0") != 0),
            F.round((F.col("highDay0") - F.col("lowDay0")) / F.col("openDay0"), 4)
        )
    )
 
    # ── 10. Average first-week volume ────────────────────────
    vol_cols = [F.col(f"volumeDay{i}") for i in range(5)]
    df = df.withColumn("avgFirstWeekVolume",
        F.round(
            sum(F.coalesce(c, F.lit(0)) for c in vol_cols) / F.lit(5), 2
        )
    )
 
    # ── 11. Company age at IPO ───────────────────────────────
    df = df.withColumn("companyAgeAtIPO",
        F.when(F.col("yearDifference") >= 0, F.col("yearDifference"))
    )
 
    # ── 12. Log of market cap ────────────────────────────────
    df = df.withColumn("logMarketCap",
        F.when(
            F.col("MarketCap").isNotNull() & (F.col("MarketCap") > 0),
            F.round(F.log(F.col("MarketCap")), 4)
        )
    )
 
    # ── 13. Revenue per employee ─────────────────────────────
    df = df.withColumn("revenuePerEmployee",
        F.when(
            (F.col("employees").isNotNull()) & (F.col("employees") > 0) & F.col("Revenue_M").isNotNull(),
            F.round((F.col("Revenue_M") * 1e6) / F.col("employees"), 2)
        )
    )
 
    # ── 14. Market cap to revenue ratio ──────────────────────
    df = df.withColumn("marketCapToRevenue",
        F.when(
            (F.col("Revenue_M").isNotNull()) & (F.col("Revenue_M") > 0) & F.col("MarketCap").isNotNull(),
            F.round(F.col("MarketCap") / (F.col("Revenue_M") * 1e6), 2)
        )
    )
 
    return df
 
