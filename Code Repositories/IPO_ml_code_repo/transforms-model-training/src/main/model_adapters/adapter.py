import palantir_models as pm


# ── Pre-IPO features (known before listing day) ─────────────────
PRE_IPO_NUMERIC = [
    ("LastSale", float), ("MarketCap", float), ("logMarketCap", float),
    ("Revenue_M", float), ("netIncome_M", float), ("profitMargin", float),
    ("lastFiscalYearGrowth", float),
    ("employees", float), ("revenuePerEmployee", float), ("marketCapToRevenue", float),
    ("CEOAge", float), ("PresidentAge", float),
    ("companyAgeAtIPO", float), ("yearDifference", float), ("YearFounded", float),
    ("MarketMonthTrend", float), ("Market3MonthTrend", float),
    ("Market6MonthTrend", float), ("MarketYearTrend", float),
    ("Year", float), ("Month", float), ("Day", float), ("dayOfWeek", float),
]

PRE_IPO_CATEGORICAL = [
    ("Sector", str), ("Industry", str),
    ("CEOGender", str), ("PresidentGender", str),
    ("USACompany", str),
    ("CEOInChargeDuringIPO", str), ("presidentInChargeDuringIPO", str),
    ("FiscalMonth", str),
    ("employeesGrouped", str), ("FoundingDateGrouped", str),
]

PRE_IPO_COLUMNS = PRE_IPO_NUMERIC + PRE_IPO_CATEGORICAL

# ── Post-IPO early signals (available after ~1 week) ────────────
POST_IPO_COLUMNS = [
    ("ipoDay0Return", float), ("firstWeekReturn", float), ("firstMonthReturn", float),
    ("ipoDay0Volatility", float), ("avgFirstWeekVolume", float),
    ("closeDay0", float), ("openDay0", float), ("highDay0", float),
    ("lowDay0", float), ("volumeDay0", float),
    ("closeDay1", float), ("closeDay2", float), ("closeDay3", float), ("closeDay4", float),
    ("volumeDay1", float), ("volumeDay2", float), ("volumeDay3", float), ("volumeDay4", float),
]

# ── Output columns ───────────────────────────────────────────────
OUTPUT_EXTRA = [
    ("prediction", int),
    ("probability_0", float),
    ("probability_1", float),
]


class PreIPOModelAdapter(pm.ModelAdapter):
    """Predicts IPO success using only pre-IPO information."""

    @pm.auto_serialize
    def __init__(self, model, feature_columns):
        self.model = model
        self.feature_columns = feature_columns

    @classmethod
    def api(cls):
        inputs = {"df_in": pm.Pandas(columns=PRE_IPO_COLUMNS)}
        outputs = {"df_out": pm.Pandas(columns=PRE_IPO_COLUMNS + OUTPUT_EXTRA)}
        return inputs, outputs

    def predict(self, df_in):
        X = df_in[self.feature_columns]
        df_in["prediction"] = self.model.predict(X)
        probas = self.model.predict_proba(X)
        df_in["probability_0"] = probas[:, 0]
        df_in["probability_1"] = probas[:, 1]
        return df_in


class PostIPOModelAdapter(pm.ModelAdapter):
    """Predicts IPO success using pre-IPO + early trading data."""

    @pm.auto_serialize
    def __init__(self, model, feature_columns):
        self.model = model
        self.feature_columns = feature_columns

    @classmethod
    def api(cls):
        all_input_cols = PRE_IPO_COLUMNS + POST_IPO_COLUMNS
        inputs = {"df_in": pm.Pandas(columns=all_input_cols)}
        outputs = {"df_out": pm.Pandas(columns=all_input_cols + OUTPUT_EXTRA)}
        return inputs, outputs

    def predict(self, df_in):
        X = df_in[self.feature_columns]
        df_in["prediction"] = self.model.predict(X)
        probas = self.model.predict_proba(X)
        df_in["probability_0"] = probas[:, 0]
        df_in["probability_1"] = probas[:, 1]
        return df_in
