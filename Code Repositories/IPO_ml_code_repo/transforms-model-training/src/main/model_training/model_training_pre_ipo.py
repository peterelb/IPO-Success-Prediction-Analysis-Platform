from transforms.api import transform, Input
from palantir_models.transforms import ModelOutput
from main.model_adapters.adapter import PreIPOModelAdapter


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

TARGET = "Profitable"


def train_model(training_df):
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
        ("num", numeric_transformer, NUMERIC_FEATURES),
        ("cat", categorical_transformer, CATEGORICAL_FEATURES),
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

    feature_cols = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    X_train = training_df[feature_cols]
    y_train = training_df[TARGET]
    model.fit(X_train, y_train)

    return model, feature_cols


@transform(
    training_data_input=Input("ri.foundry.main.dataset.6d034bd9-e32f-42bf-9b48-41d131d3bfbb"),
    model_output=ModelOutput("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/models/ipo_pre_ipo_model"),
)
def compute(training_data_input, model_output):
    training_df = training_data_input.pandas()
    model, feature_cols = train_model(training_df)

    foundry_model = PreIPOModelAdapter(model, feature_cols)
    model_output.publish(model_adapter=foundry_model)
