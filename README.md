# IPO Analysis and Prediction Center
 
An end-to-end data engineering and machine learning project built on **Palantir Foundry** that analyzes U.S. IPO performance from 1986 to 2021, correlates it with S&P 500 market conditions, and predicts whether upcoming IPOs will be profitable using Random Forest classification models.
 
The project spans the full data lifecycle — from raw data ingestion through cleaning, transformation, and feature engineering, to ML model training, inference, and interactive dashboards for decision-making.
 
---
 
## Table of Contents
 
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Data Pipeline](#data-pipeline)
  - [Raw Data Sources](#raw-data-sources)
  - [Preprocessing](#preprocessing)
  - [Cleaning](#cleaning)
  - [Transformation](#transformation)
- [Machine Learning](#machine-learning)
  - [Feature Engineering](#feature-engineering)
  - [Pre-IPO Model](#pre-ipo-model)
  - [Post-IPO Model](#post-ipo-model)
  - [Model Performance](#model-performance)
- [Dashboards](#dashboards)
- [TypeScript Functions](#typescript-functions)
- [Custom Visualizations](#custom-visualizations)
- [Technologies](#technologies)
- [Folder Reference](#folder-reference)
 
---
 
## Project Overview
 
The **IPO Analysis and Prediction Center** addresses a core question in financial analytics: *Can we predict whether an IPO will be profitable based on company fundamentals, market conditions, and early trading data?*
 
The project tackles this through three integrated workstreams:
 
1. **Data Engineering** — Seven raw datasets are ingested, preprocessed, cleaned, and transformed through Foundry pipelines into analytics-ready outputs. This includes joining IPO records with S&P 500 market data, standardizing financial fields, and building derived features like profit margins and trading volatility metrics.
 
2. **Machine Learning** — Two Random Forest classifiers are trained to predict IPO profitability. The *Pre-IPO model* uses only information available before trading begins (company fundamentals, CEO demographics, market trends). The *Post-IPO model* adds early trading indicators (day-0 returns, first-week volatility, opening volumes) for improved accuracy after the stock starts trading.
 
3. **Interactive Dashboards** — A five-tab Foundry dashboard provides data health monitoring, historical IPO exploration, S&P 500 market analysis, and a Prediction Center where users can input company details and receive real-time profitability predictions with strength/risk explanations.
 
---
 
## Architecture
 
The system follows a layered data architecture on Palantir Foundry:
 
```
┌─────────────────────────────────────────────────────────────────────┐
│                        FOUNDRY DASHBOARDS                          │
│  Market Analysis │ IPO Explorer │ Prediction Center │ Data Health  │
├─────────────────────────────────────────────────────────────────────┤
│                      TYPESCRIPT FUNCTIONS                          │
│         Authorization │ Aggregations │ Search Queries              │
├─────────────────────────────────────────────────────────────────────┤
│                        ML MODELS                                   │
│          Pre-IPO Classifier  │  Post-IPO Classifier               │
│          (33 features)       │  (50 features)                     │
├─────────────────────────────────────────────────────────────────────┤
│                    ONTOLOGY / DATASETS                             │
│         Cleaned IPO Data │ S&P 500 │ ML Features │ Predictions    │
├─────────────────────────────────────────────────────────────────────┤
│                   TRANSFORM PIPELINES                              │
│          Raw → Preprocessed → Cleaned → Transformed               │
├─────────────────────────────────────────────────────────────────────┤
│                      RAW DATA SOURCES                              │
│   US IPO (1986-2021) │ S&P 500 Stocks │ S&P Index │ Future IPOs  │
└─────────────────────────────────────────────────────────────────────┘
```
 
Three code repositories power the pipeline:
 
- **IPO_code_repo** — Python transforms for data preprocessing, cleaning, and transformation
- **IPO_ml_code_repo** — Python model training, feature engineering, and inference
- **Functions** — TypeScript functions for dashboard interactivity and Ontology queries
 
---
 
## Repository Structure
 
```
├── Code Repositories/
│   ├── Functions/                          # TypeScript Functions (Foundry OSDK)
│   │   └── typescript-functions/
│   │       └── src/functions/
│   │           ├── helloWorld.ts            # Greeting function
│   │           ├── findSumOfArray.ts        # Array sum computation
│   │           ├── isAuthorizedUser.ts      # User authorization check
│   │           ├── searchAircraft.ts        # Ontology object search
│   │           └── __tests__/               # Vitest unit tests
│   │
│   ├── IPO_code_repo/                      # Python Data Transforms
│   │   └── transforms-python/
│   │       └── src/myproject/datasets/
│   │           ├── preprocessed/            # Column renaming, dedup, type casting
│   │           ├── cleaned/                 # Feature calculation, sector mapping
│   │           └── transformed/             # Dataset joins and enrichment
│   │
│   └── IPO_ml_code_repo/                   # ML Model Training & Inference
│       └── transforms-model-training/
│           └── src/
│               ├── main/
│               │   ├── model_adapters/      # Pre/Post IPO model adapter classes
│               │   └── model_training/      # Training scripts & feature engineering
│               └── myproject/datasets/
│                   └── machinelearning/      # Inference & metrics computation
│
├── datasets/
│   ├── raw/                                # 7 original source CSVs
│   ├── preprocessed/                       # 6 standardized datasets
│   ├── cleaned/                            # 3 final cleaned datasets
│   ├── transformed/                        # 3 joined/enriched datasets
│   └── machinelearning/
│       ├── ml_training.csv                 # Training split
│       ├── ml_testing.csv                  # Testing split
│       └── predictions/                    # Pre-IPO and Post-IPO predictions
│
├── Dashboards/                             # Dashboard tab screenshots (JPG/PNG)
│
├── Extra/
│   ├── Data Lineage/                       # 5 pipeline flow diagrams
│   ├── Notepad Report/                     # Generated IPO report PDFs
│   └── Slate/                              # Custom heatmap visualization (HTML/CSS/JS)
│
└── presentation/                           # Final project presentations (PPTX, PDF)
```
 
---
 
## Data Pipeline
 
The data pipeline processes seven raw datasets through four stages: **Raw → Preprocessed → Cleaned → Transformed**. Each stage is implemented as a Foundry Python transform with full data lineage tracking.
 
### Raw Data Sources
 
| Dataset | Description | Key Fields |
|---------|-------------|------------|
| `US_IPO_Data_1986_2021_enriched.csv` | 35 years of U.S. IPO records | Company, ticker, state, financials, IPO price, date |
| `ipo_stock_2010_2018_v2_enriched.csv` | Post-IPO stock performance | Day-by-day open/close/high/low/volume (Day 0–100) |
| `IPO_full_ml_worthy.csv` | ML-enriched IPO dataset | CEO/President info, founding date, market trends, fiscal data |
| `sp500_companies.csv` | S&P 500 constituent companies | Symbol, sector, industry, market cap, weight, revenue growth |
| `sp500_stock.csv` | S&P 500 daily stock prices | Date, ticker, OHLCV (open/high/low/close/volume) |
| `sp500_index.csv` | S&P 500 index historical values | Date, index value |
| `future_ipo_raw.csv` | Upcoming IPO candidates | Company details for future prediction |
 
### Preprocessing
 
The preprocessing stage standardizes column names, removes duplicates, handles nulls, and casts types:
 
- **us_ipo_preprocessed** — Renames columns (company → company_name, state → state_name), drops duplicates on ticker, converts negative profits to 0, rounds financials to 1 decimal place
- **us_ipo_preprocessed_2** — Processes the second IPO dataset: deduplicates on Company_Name, rounds CEO pay/offer amounts, standardizes column naming
- **sp500_companies_preprocessed** — Drops rows missing revenue growth, state, or employee count; renames Symbol → ticker_name, Sector → sector_name
- **sp500_stock_preprocessed** — Parses date format ("MMMM d, yyyy"), renames price columns, rounds to 1 decimal, drops null volumes
- **sp500_index_preprocessed** — Renames Date → date, SP500 → sp_500
- **ml_preprocessed** — Prepares the ML-enriched dataset for feature calculation
 
### Cleaning
 
The cleaning stage applies business logic, calculates derived features, and maps categories:
 
- **us_ipo_cleaned** — Maps raw industry names to 10 standardized sectors (Technology, Healthcare, Financial Services, Energy, Industrials, Consumer Goods, Materials, Agriculture, Communication & Media, Professional Services)
 
- **sp_500_cleaned** — Calculates daily return as `(close - prev_close) / prev_close × 100`, extracts year/month/quarter from date, creates a composite `stock_date_id` identifier
 
- **ml_cleaned** — The most complex transform, which:
  - Parses revenue and net income strings ($B, $M, $K notation) into numeric millions
  - Calculates 14 derived features including profit margin, IPO day-0 return, first-week return, first-month return, IPO day-0 volatility, average first-week volume, company age at IPO, log market cap, revenue per employee, and market cap to revenue ratio
  - Consolidates CEO/President gender categories into male/female/unknown
 
### Transformation
 
The transformation stage joins datasets to produce analytics-ready outputs:
 
- **us_ipo_joined_transformed** — Inner joins the two preprocessed IPO datasets on ticker_name, producing a unified IPO record with financials, performance data, and company details
- **sp500_stock_companies_transformed** — Left joins stock prices with company metadata (sector, industry, market cap, weight), filtering for complete records
- **sp500_with_index_transformed** — Joins stock prices with S&P 500 index values on date, adding market-level context to individual stock records
 
---
 
## Machine Learning
 
### Feature Engineering
 
The ML pipeline uses two feature sets, both centered on predicting the binary target: **Profitable** (1 = profitable, 0 = not profitable).
 
**Pre-IPO Features (33 total)** — Available before trading begins:
 
| Category | Features |
|----------|----------|
| Financial | LastSale, MarketCap, logMarketCap, Revenue_M, netIncome_M, profitMargin, lastFiscalYearGrowth |
| Company | employees, revenuePerEmployee, marketCapToRevenue, companyAgeAtIPO, yearDifference, YearFounded |
| Leadership | CEOAge, PresidentAge, CEOGender, PresidentGender, CEOInChargeDuringIPO, presidentInChargeDuringIPO |
| Market Trends | MarketMonthTrend, Market3MonthTrend, Market6MonthTrend, MarketYearTrend |
| Temporal | Year, Month, Day, dayOfWeek, FiscalMonth |
| Categorical | Sector, Industry, USACompany, employeesGrouped, FoundingDateGrouped |
 
**Post-IPO Features (17 additional)** — Available after trading begins:
 
| Category | Features |
|----------|----------|
| Returns | ipoDay0Return, firstWeekReturn, firstMonthReturn |
| Volatility | ipoDay0Volatility |
| Volume | avgFirstWeekVolume, volumeDay0 through volumeDay4 |
| Prices | closeDay0–closeDay4, openDay0, highDay0, lowDay0 |
 
### Pre-IPO Model
 
The Pre-IPO model answers: *"Before the stock starts trading, will this IPO be profitable?"*
 
- **Algorithm:** RandomForestClassifier (200 estimators, max_depth=15, class_weight=balanced)
- **Features:** 23 numeric + 10 categorical (33 total)
- **Pipeline:** Numeric features go through median imputation → standard scaling; categorical features go through most-frequent imputation → one-hot encoding
- **Use case:** Evaluating upcoming IPOs (e.g., the Future IPO Prediction tab) where no trading data exists yet
 
### Post-IPO Model
 
The Post-IPO model answers: *"Now that trading has started, does early performance confirm or change the prediction?"*
 
- **Algorithm:** Same RandomForestClassifier configuration
- **Features:** 40 numeric + 10 categorical (50 total) — adds 17 early trading indicators
- **Use case:** Refining predictions after the first week of trading with real market data
 
### Model Performance
 
Results on the held-out test set (704 samples):
 
| Metric | Pre-IPO Model | Post-IPO Model |
|--------|---------------|----------------|
| Accuracy | 70% | 78.7% |
| Precision | — | 79.2% |
| Recall | — | 84.0% |
| F1-Score | — | 81.5% |
| ROC-AUC | — | 0.86 |
| MCC | — | 0.57 |
 
The Post-IPO model confusion matrix shows 331 true positives, 223 true negatives, 87 false positives, and 63 false negatives — indicating stronger performance on identifying profitable IPOs (high recall) with reasonable precision.
 
Feature importance analysis reveals that market cap to revenue ratio, market capitalization, log market cap, and market trend indicators are among the most influential predictors.
 
---
 
## Dashboards
 
The Foundry dashboard application — **IPO Analysis and Prediction Center** — has five main tabs:
 
### General Market Analysis
Displays the current S&P 500 index value, total market cap, and all-time high. Includes a historical index price chart (2015–2024), a donut chart of average revenue growth by sector, and a table of top holdings ranked by market cap (AAPL, NVDA, MSFT, AMZN, etc.).
 
### Historical IPO Explorer
An interactive browser for all historical IPO records. Users can search by state, sector, ticker, or exchange. Selecting an IPO shows company details (address, website, employees), financial metrics (offer price, IPO amount, fees), and a **Performance Scorecard** comparing first-day, first-week, first-month, and first-year open prices against sector averages.
 
### Prediction Center
Three sub-tabs for ML-powered analysis:
 
- **Future IPO Prediction** — Lists upcoming IPO candidates (e.g., Quantinuum, OpenAI, Kraken, Motive, Canva, TerraGrid Energy) with their predicted probability of success, invest decision (Buy/Moderate/Sell), and a breakdown of **Strengths** and **Risks** showing which features deviate from baseline values.
 
- **Interactive Predictor** — A form where users input company fundamentals (last sale price, market cap, revenue, employees, net income, sector, industry, CEO gender/age, year founded, USA company status) and receive a real-time prediction with probability of success and an invest recommendation.
 
- **Model Performance** — Displays accuracy, precision, ROC-AUC, and sample size for both models, along with side-by-side confusion matrices visualizing true/false positives and negatives.
 
### Data Health
Monitors data quality across all pipeline datasets using Foundry's health check framework. Tracks schema validation, time-since-last-updated, null percentage, and primary key checks. Each dataset is linked to its parent project and schedule for automated monitoring.
 
---
 
## TypeScript Functions
 
The Functions repository provides server-side TypeScript logic for dashboard interactivity, built on the Foundry OSDK (Ontology SDK):
 
- **findSumOfArray** — Computes the sum of an integer array, used for aggregation widgets
- **isAuthorizedUser** — Checks if a user ID matches an authorized UUID for access-controlled dashboard features
- **searchAircraft** — Queries the Ontology for aircraft objects by manufacturer, demonstrating OSDK Client usage with object sets
- **helloWorld** — Basic greeting function used for testing the Functions framework
 
Functions are tested with Vitest and deployed via Gradle with semantic versioning to the Foundry Function Registry.
 
---
 
## Custom Visualizations
 
### Sector Annual Return Heatmap (Slate)
 
A custom Foundry Slate application located in `Extra/Slate/` that renders a **sector-by-year heatmap** of annual stock returns. Built with vanilla HTML, CSS, and JavaScript, it features:
 
- A dark-themed table with sectors as rows and years (2015–2024) as columns
- A seven-tier color scale from dark green (>=40% return) through yellow (0–10%) to dark red (<-20%)
- Hover tooltips showing exact return values per sector/year
- Click handlers for drilling into specific cells
- Sector coverage: Technology, Communication Services, Financial Services, Healthcare, Industrials, Energy, Consumer Cyclical, Consumer Defensive, Basic Materials, Real Estate, Utilities
 
---
 
## Technologies
 
- **Palantir Foundry** — Data pipelines, Ontology, object types, dashboards, Slate, health checks
- **Python** — Data transforms (PySpark DataFrames), ML training (scikit-learn RandomForest), feature engineering
- **TypeScript** — Foundry Functions using OSDK for Ontology queries and dashboard logic
- **scikit-learn** — RandomForestClassifier, Pipeline, ColumnTransformer, StandardScaler, OneHotEncoder
- **Gradle** — Build system for all three repositories (Java 17, Node 22)
- **Vitest** — Unit testing framework for TypeScript Functions
- **Conda** — Python dependency management with Hawk solver and UV
 
---
 
## Folder Reference
 
| Folder | Contents |
|--------|----------|
| `Code Repositories/Functions/` | TypeScript Functions source, tests, Gradle build config |
| `Code Repositories/IPO_code_repo/` | Python transforms for data preprocessing, cleaning, joining |
| `Code Repositories/IPO_ml_code_repo/` | ML model adapters, training scripts, inference, metrics |
| `datasets/raw/` | 7 original CSV source files |
| `datasets/preprocessed/` | 6 standardized intermediate datasets |
| `datasets/cleaned/` | 3 final cleaned datasets (IPO, S&P 500, ML) |
| `datasets/transformed/` | 3 joined/enriched analytics-ready datasets |
| `datasets/machinelearning/` | Train/test splits and model prediction outputs |
| `Dashboards/` | Screenshots of all 5 dashboard tabs |
| `Extra/Data Lineage/` | 5 pipeline diagrams (full system, IPO ingestion, ML, S&P 500, simplified) |
| `Extra/Notepad Report/` | Generated IPO analysis report and template (PDF) |
| `Extra/Slate/` | Custom heatmap visualization source (HTML/CSS/JS) and screenshot |
| `presentation/` | Final project presentations (PPTX) and project description (PDF) |
