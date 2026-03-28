from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transforms.api import transform, Input, Output

@transform(
    feature_importance=Input("ri.foundry.main.dataset.70ae3a3a-02e7-4076-be66-9bedf050d205"),
    ml_training=Input("ri.foundry.main.dataset.6d034bd9-e32f-42bf-9b48-41d131d3bfbb"),
    future_ipo_raw=Input("ri.foundry.main.dataset.21e9dcd3-c00e-4f90-a6df-109b02849b70"),
    output=Output("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/datasets/machinelearning/evaluation/metrics/future_ipo_feature_importance"),
)
def compute(feature_importance, ml_training, future_ipo_raw, output):

    fi_df = feature_importance.dataframe()
    train_df = ml_training.dataframe()
    future_df = future_ipo_raw.dataframe()

    # --- Step 1: Identify usable numeric features (exist in both training and future) ---
    future_columns = set(future_df.columns)

    # Features to exclude (not business-meaningful for factor analysis)
    exclude_features = {"Year", "Month", "Day", "dayOfWeek", "YearFounded", "FoundingDateGrouped", "FiscalMonth", "employeesGrouped"}

    # Get feature names from importance dataset, filtered to those in future_ipo_raw
    fi_rows = fi_df.filter(F.col("model_name") == "post_ipo_model") \
                    .select("feature_name", "importance", "rank") \
                    .collect()

    # Only keep numeric features that exist in future_ipo_raw and are not excluded
    numeric_feature_types = dict(future_df.dtypes)
    usable_features = []
    for row in fi_rows:
        fname = row["feature_name"]
        if fname in future_columns \
                and fname not in exclude_features \
                and numeric_feature_types.get(fname) in ("double", "float", "int", "long", "bigint"):
            usable_features.append({
                "feature_name": fname,
                "importance": float(row["importance"]),
                "rank": int(row["rank"]),
            })

    if not usable_features:
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
        schema = StructType([
            StructField("CompanyName", StringType()),
            StructField("factor_name", StringType()),
            StructField("factor_type", StringType()),
            StructField("actual_value", DoubleType()),
            StructField("baseline_value", DoubleType()),
            StructField("deviation", DoubleType()),
            StructField("weighted_impact", DoubleType()),
            StructField("importance_rank", IntegerType()),
            StructField("factor_rank", IntegerType()),
        ])
        return output.write_dataframe(train_df.sparkSession.createDataFrame([], schema))

    feature_names = [f["feature_name"] for f in usable_features]

    # --- Step 2: Compute successful IPO baselines (mean and stddev) ---
    successful = train_df.filter(F.col("Profitable") == 1)

    agg_exprs = []
    for fname in feature_names:
        agg_exprs.append(F.mean(F.col(fname).cast("double")).alias(f"{fname}__mean"))
        agg_exprs.append(F.stddev(F.col(fname).cast("double")).alias(f"{fname}__std"))

    baselines = successful.agg(*agg_exprs).collect()[0]

    baseline_mean = {}
    baseline_std = {}
    importance_map = {}
    rank_map = {}
    for f in usable_features:
        fname = f["feature_name"]
        baseline_mean[fname] = float(baselines[f"{fname}__mean"]) if baselines[f"{fname}__mean"] is not None else 0.0
        std_val = float(baselines[f"{fname}__std"]) if baselines[f"{fname}__std"] is not None else 1.0
        baseline_std[fname] = std_val if std_val > 0 else 1.0
        importance_map[fname] = f["importance"]
        rank_map[fname] = f["rank"]

    # --- Step 3: For each future company, compute weighted deviations ---
    n_features = len(feature_names)
    stack_expr_parts = []
    for fname in feature_names:
        stack_expr_parts.append(f"'{fname}', cast(`{fname}` as double)")
    stack_str = ", ".join(stack_expr_parts)
    stack_expr = f"stack({n_features}, {stack_str}) as (factor_name, actual_value)"

    unpivoted = future_df.select("CompanyName", F.expr(stack_expr))

    baseline_mean_expr = F.create_map([val for fname in feature_names for val in (F.lit(fname), F.lit(baseline_mean[fname]))])
    baseline_std_expr = F.create_map([val for fname in feature_names for val in (F.lit(fname), F.lit(baseline_std[fname]))])
    importance_expr = F.create_map([val for fname in feature_names for val in (F.lit(fname), F.lit(importance_map[fname]))])
    rank_expr = F.create_map([val for fname in feature_names for val in (F.lit(fname), F.lit(float(rank_map[fname])))])

    unpivoted = unpivoted \
        .withColumn("baseline_value", baseline_mean_expr[F.col("factor_name")]) \
        .withColumn("std_value", baseline_std_expr[F.col("factor_name")]) \
        .withColumn("importance", importance_expr[F.col("factor_name")]) \
        .withColumn("importance_rank", rank_expr[F.col("factor_name")].cast("int"))

    unpivoted = unpivoted \
        .withColumn("deviation", (F.col("actual_value") - F.col("baseline_value")) / F.col("std_value")) \
        .withColumn("weighted_impact", F.col("deviation") * F.col("importance"))

    unpivoted = unpivoted \
        .withColumn("factor_type", F.when(F.col("weighted_impact") >= 0, "strength").otherwise("risk"))

    # --- Step 4: Always take top 3 and bottom 3 per company ---
    top_window = Window.partitionBy("CompanyName").orderBy(F.col("weighted_impact").desc())
    top_3 = unpivoted.withColumn("_rank", F.row_number().over(top_window)) \
        .filter(F.col("_rank") <= 3) \
        .withColumn("factor_type", F.lit("strength")) \
        .withColumn("factor_rank", F.col("_rank")) \
        .drop("_rank")

    bottom_window = Window.partitionBy("CompanyName").orderBy(F.col("weighted_impact").asc())
    bottom_3 = unpivoted.withColumn("_rank", F.row_number().over(bottom_window)) \
        .filter(F.col("_rank") <= 3) \
        .withColumn("factor_type", F.lit("risk")) \
        .withColumn("factor_rank", F.col("_rank")) \
        .drop("_rank")

    result = top_3.unionByName(bottom_3)

    result = result.select(
        "CompanyName",
        "factor_name",
        "factor_type",
        F.round("actual_value", 4).alias("actual_value"),
        F.round("baseline_value", 4).alias("baseline_value"),
        F.round("deviation", 4).alias("deviation"),
        F.round("weighted_impact", 6).alias("weighted_impact"),
        "importance_rank",
        "factor_rank",
    )
    result = result.withColumn(
        "pk",
        F.concat_ws("__", "CompanyName", "factor_name", "factor_type")
    )

    output.write_dataframe(result)