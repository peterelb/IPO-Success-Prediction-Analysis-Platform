from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql import Row


NUMERIC_FEATURES = [
    "LastSale", "MarketCap", "logMarketCap",
    "Revenue_M", "netIncome_M", "profitMargin",
    "lastFiscalYearGrowth",
    "employees", "revenuePerEmployee", "marketCapToRevenue",
    "CEOAge", "PresidentAge",
    "companyAgeAtIPO", "yearDifference", "YearFounded",
    "MarketMonthTrend", "Market3MonthTrend",
    "Market6MonthTrend", "MarketYearTrend",
    "Year", "Month", "Day", "dayOfWeek",
]

CATEGORICAL_FEATURES = [
    "Sector", "Industry",
    "CEOGender", "PresidentGender",
    "USACompany",
    "CEOInChargeDuringIPO", "presidentInChargeDuringIPO",
    "FiscalMonth",
    "employeesGrouped", "FoundingDateGrouped",
]

POST_IPO_NUMERIC = [
    "ipoDay0Return", "firstWeekReturn", "firstMonthReturn",
    "ipoDay0Volatility", "avgFirstWeekVolume",
    "closeDay0", "openDay0", "highDay0", "lowDay0", "volumeDay0",
    "closeDay1", "closeDay2", "closeDay3", "closeDay4",
    "volumeDay1", "volumeDay2", "volumeDay3", "volumeDay4",
]

TARGET = "Profitable"


def train_and_extract(training_df, numeric_features, categorical_features):
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    numeric_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])

    categorical_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="constant", fill_value="Unknown")),
        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])

    preprocessor = ColumnTransformer(transformers=[
        ("num", numeric_transformer, numeric_features),
        ("cat", categorical_transformer, categorical_features),
    ])

    model = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("classifier", RandomForestClassifier(
            n_estimators=200,
            max_depth=15,
            min_samples_split=10,
            min_samples_leaf=5,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1,
        )),
    ])

    feature_cols = numeric_features + categorical_features
    X_train = training_df[feature_cols]
    y_train = training_df[TARGET]
    model.fit(X_train, y_train)

    classifier = model.named_steps["classifier"]
    transformed_names = preprocessor.get_feature_names_out()
    raw_importances = classifier.feature_importances_

    importance_map = {}
    for name, imp in zip(transformed_names, raw_importances):
        if name.startswith("num__"):
            original = name[5:]
        elif name.startswith("cat__"):
            stripped = name[5:]
            original = stripped
            for cat_feat in categorical_features:
                if stripped.startswith(cat_feat):
                    original = cat_feat
                    break
        else:
            original = name
        importance_map[original] = importance_map.get(original, 0.0) + imp

    return importance_map


@transform(
    pre_ipo_predictions=Input("ri.foundry.main.dataset.87b21df2-4ac4-4680-ae0d-aaff0714545a"),
    post_ipo_predictions=Input("ri.foundry.main.dataset.8c373bd8-3b21-47bf-8ba8-edfd80272efd"),
    training_data=Input("ri.foundry.main.dataset.6d034bd9-e32f-42bf-9b48-41d131d3bfbb"),
    metrics_output=Output("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/datasets/machinelearning/evaluation/metrics/model_performance_metrics"),
    importance_output=Output("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/datasets/machinelearning/evaluation/metrics/feature_importance"),
)
def compute(pre_ipo_predictions, post_ipo_predictions, training_data, metrics_output, importance_output, ctx):
    from sklearn.metrics import (
        accuracy_score, precision_score, recall_score, f1_score,
        roc_auc_score, matthews_corrcoef, confusion_matrix,
    )

    # ── Metrics ──
    results = []

    for model_name, pred_input in [
        ("pre_ipo_model", pre_ipo_predictions),
        ("post_ipo_model", post_ipo_predictions),
    ]:
        df = pred_input.dataframe().select("Profitable", "prediction", "probability_0", "probability_1").toPandas()

        y_true = df["Profitable"].values
        y_pred = df["prediction"].values
        y_prob = df["probability_1"].values

        tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

        results.append(Row(
            model_name=model_name,
            accuracy=float(round(accuracy_score(y_true, y_pred), 4)),
            precision=float(round(precision_score(y_true, y_pred), 4)),
            recall=float(round(recall_score(y_true, y_pred), 4)),
            f1_score=float(round(f1_score(y_true, y_pred), 4)),
            roc_auc=float(round(roc_auc_score(y_true, y_prob), 4)),
            mcc=float(round(matthews_corrcoef(y_true, y_pred), 4)),
            true_positives=int(tp),
            false_positives=int(fp),
            true_negatives=int(tn),
            false_negatives=int(fn),
            total_samples=int(len(y_true)),
            positive_rate=float(round(y_true.mean(), 4)),
        ))

    metrics_df = ctx.spark_session.createDataFrame(results)
    metrics_output.write_dataframe(metrics_df)

    # ── Feature Importance ──
    training_df = training_data.dataframe().toPandas()

    pre_importances = train_and_extract(
        training_df, NUMERIC_FEATURES, CATEGORICAL_FEATURES
    )
    post_importances = train_and_extract(
        training_df, NUMERIC_FEATURES + POST_IPO_NUMERIC, CATEGORICAL_FEATURES
    )

    importance_rows = []

    total_pre = sum(pre_importances.values())
    for rank, (feat, imp) in enumerate(
        sorted(pre_importances.items(), key=lambda x: x[1], reverse=True), 1
    ):
        importance_rows.append(Row(
            pk=f"pre_ipo_model__{feat}",
            model_name="pre_ipo_model",
            feature_name=feat,
            importance=float(round(imp, 6)),
            importance_pct=float(round(imp / total_pre * 100, 2)),
            rank=rank,
        ))

    total_post = sum(post_importances.values())
    for rank, (feat, imp) in enumerate(
        sorted(post_importances.items(), key=lambda x: x[1], reverse=True), 1
    ):
        importance_rows.append(Row(
            pk=f"post_ipo_model__{feat}",
            model_name="post_ipo_model",
            feature_name=feat,
            importance=float(round(imp, 6)),
            importance_pct=float(round(imp / total_post * 100, 2)),
            rank=rank,
        ))

    importance_df = ctx.spark_session.createDataFrame(importance_rows)
    importance_output.write_dataframe(importance_df)