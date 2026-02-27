# ─── PIPELINE CONFIGURATION ──────────────────────────────────
# Centralized settings for all pipelines
# Change paths and settings here — not inside pipeline files

import os

# ─── BASE PATHS ───────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR     = os.path.join(BASE_DIR, "data")
RAW_DIR      = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR= os.path.join(DATA_DIR, "processed")
LOGS_DIR     = os.path.join(BASE_DIR, "logs")
DOCS_DIR     = os.path.join(BASE_DIR, "docs")

# ─── INPUT FILES ──────────────────────────────────────────────
SUPERSTORE_INPUT  = os.path.join(RAW_DIR, "superstore.csv")
AIRTRAVEL_INPUT   = os.path.join(RAW_DIR, "airtravel.csv")
COVID_INPUT       = os.path.join(RAW_DIR, "covid_data.csv")

# ─── OUTPUT PATHS ─────────────────────────────────────────────
SUPERSTORE_OUTPUT = os.path.join(PROCESSED_DIR, "superstore_clean_parquet")
AIRTRAVEL_OUTPUT  = os.path.join(PROCESSED_DIR, "airtravel_clean_parquet")
COVID_OUTPUT      = os.path.join(PROCESSED_DIR, "covid_clean_parquet")
SQL_OUTPUT        = os.path.join(PROCESSED_DIR, "sql_analysis_parquet")

# ─── SPARK CONFIGURATION ──────────────────────────────────────
SPARK_CONFIG = {
    "app_name"              : "BatchETL_Pipeline",
    "master"                : "local[8]",
    "driver_memory"         : "4g",
    "executor_memory"       : "2g",
    "shuffle_partitions"    : "8",
    "max_result_size"       : "1g",
    "log_level"             : "ERROR"
}

# ─── PIPELINE SETTINGS ────────────────────────────────────────
PIPELINE_CONFIG = {
    "superstore" : {
        "input"     : SUPERSTORE_INPUT,
        "output"    : SUPERSTORE_OUTPUT,
        "group_by"  : ["Region", "Category"],
        "agg_col"   : "Sales"
    },
    "airtravel"  : {
        "input"     : AIRTRAVEL_INPUT,
        "output"    : AIRTRAVEL_OUTPUT,
        "year_cols" : ["1958", "1959", "1960"]
    },
    "covid"      : {
        "input"     : COVID_INPUT,
        "output"    : COVID_OUTPUT,
        "key_cols"  : [
            "iso_code", "continent", "location",
            "total_cases", "total_deaths",
            "total_vaccinations",
            "people_fully_vaccinated",
            "population", "gdp_per_capita",
            "life_expectancy", "median_age"
        ]
    }
}

# ─── DATA QUALITY THRESHOLDS ──────────────────────────────────
QUALITY_CONFIG = {
    "min_sales"          : 0,
    "max_sales"          : 100000,
    "min_passengers"     : 0,
    "max_death_rate"     : 100,
    "min_vaccination"    : 0,
    "max_vaccination"    : 100
}
