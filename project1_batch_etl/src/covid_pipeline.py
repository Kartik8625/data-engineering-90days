from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, round as spark_round,
    sum as spark_sum, avg,
    when, desc
)
from pyspark.sql.types import FloatType, LongType
import logging
import os
from datetime import datetime

# ─── LOGGING SETUP ───────────────────────────────────────────
def setup_logger():
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/covid_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

# ─── SPARK SESSION ───────────────────────────────────────────
def create_spark_session(logger):
    logger.info("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("COVID_Batch_ETL") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark session created.")
    return spark

# ─── EXTRACT ─────────────────────────────────────────────────
def extract(spark, input_path, logger):
    """Load raw COVID CSV — select only relevant columns."""
    logger.info(f"Extracting data from: {input_path}")

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Load full CSV
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Select only meaningful columns for analysis
    df_selected = df.select(
        "iso_code",
        "continent",
        "location",
        "total_cases",
        "total_deaths",
        "total_vaccinations",
        "people_fully_vaccinated",
        "population",
        "gdp_per_capita",
        "life_expectancy",
        "median_age"
    )

    logger.info(f"Rows loaded: {df_selected.count()}")
    logger.info(f"Columns selected: {len(df_selected.columns)}")
    return df_selected

# ─── TRANSFORM ───────────────────────────────────────────────
def transform(df, logger):
    """
    Clean and enrich COVID data.
    - Remove rows without continent (aggregated rows)
    - Cast numeric columns properly
    - Calculate death rate and vaccination rate
    - Classify countries by vaccination coverage
    """
    logger.info("Starting transformation...")

    # Remove aggregated rows (continent/world level)
    # These have no continent value
    df_clean = df.filter(
        col("continent").isNotNull()
    )
    logger.info(f"After removing aggregated rows: {df_clean.count()}")

    # Cast all numeric columns to Float
    numeric_cols = [
        "total_cases", "total_deaths",
        "total_vaccinations", "people_fully_vaccinated",
        "population", "gdp_per_capita",
        "life_expectancy", "median_age"
    ]

    for c in numeric_cols:
        df_clean = df_clean.withColumn(c, col(c).cast(FloatType()))

    # Remove rows where population or total_cases is null
    df_clean = df_clean.dropna(
        subset=["population", "total_cases"]
    )
    logger.info(f"After cleaning nulls: {df_clean.count()}")

    # Calculate death rate percentage
    df_clean = df_clean.withColumn(
        "Death_Rate_Pct",
        spark_round(
            (col("total_deaths") / col("total_cases")) * 100, 2
        )
    )

    # Calculate vaccination rate percentage
    df_clean = df_clean.withColumn(
        "Vaccination_Rate_Pct",
        spark_round(
            (col("people_fully_vaccinated") / col("population")) * 100, 2
        )
    )

    # Classify countries by vaccination coverage
    df_clean = df_clean.withColumn(
        "Vaccination_Status",
        when(col("Vaccination_Rate_Pct") >= 70, "Highly Vaccinated")
        .when(col("Vaccination_Rate_Pct") >= 40, "Moderately Vaccinated")
        .when(col("Vaccination_Rate_Pct") >= 10, "Low Vaccination")
        .otherwise("Minimal/No Data")
    )

    # Classify countries by GDP
    df_clean = df_clean.withColumn(
        "Economic_Status",
        when(col("gdp_per_capita") >= 30000, "High Income")
        .when(col("gdp_per_capita") >= 10000, "Upper Middle")
        .when(col("gdp_per_capita") >= 3000,  "Lower Middle")
        .otherwise("Low Income")
    )

    logger.info("Transformation complete.")
    return df_clean

# ─── ANALYZE ─────────────────────────────────────────────────
def analyze(df, spark, logger):
    """Run analytical queries using Spark SQL."""
    logger.info("Running analysis...")

    # Register as SQL view
    df.createOrReplaceTempView("covid")

    print("\n" + "="*55)
    print(">>> ANALYSIS 1: Total Cases by Continent")
    print("="*55)
    spark.sql("""
        SELECT continent,
               COUNT(location) AS Countries,
               ROUND(SUM(total_cases), 0) AS Total_Cases,
               ROUND(SUM(total_deaths), 0) AS Total_Deaths
        FROM covid
        GROUP BY continent
        ORDER BY Total_Cases DESC
    """).show()

    print("="*55)
    print(">>> ANALYSIS 2: Top 10 Most Affected Countries")
    print("="*55)
    spark.sql("""
        SELECT location,
               continent,
               ROUND(total_cases, 0) AS Total_Cases,
               ROUND(Death_Rate_Pct, 2) AS Death_Rate_Pct
        FROM covid
        ORDER BY total_cases DESC
        LIMIT 10
    """).show()

    print("="*55)
    print(">>> ANALYSIS 3: Vaccination Status Distribution")
    print("="*55)
    spark.sql("""
        SELECT Vaccination_Status,
               COUNT(*) AS Country_Count,
               ROUND(AVG(Vaccination_Rate_Pct), 2) AS Avg_Vaccination_Pct
        FROM covid
        GROUP BY Vaccination_Status
        ORDER BY Avg_Vaccination_Pct DESC
    """).show()

    print("="*55)
    print(">>> ANALYSIS 4: Avg Death Rate by Economic Status")
    print("="*55)
    spark.sql("""
        SELECT Economic_Status,
               COUNT(*) AS Countries,
               ROUND(AVG(Death_Rate_Pct), 3) AS Avg_Death_Rate,
               ROUND(AVG(Vaccination_Rate_Pct), 2) AS Avg_Vaccination_Pct
        FROM covid
        GROUP BY Economic_Status
        ORDER BY Avg_Death_Rate DESC
    """).show()

    print("="*55)
    print(">>> ANALYSIS 5: Top 5 Highest Death Rate Countries")
    print("="*55)
    spark.sql("""
        SELECT location,
               continent,
               ROUND(total_cases, 0) AS Total_Cases,
               ROUND(Death_Rate_Pct, 2) AS Death_Rate_Pct,
               Economic_Status
        FROM covid
        WHERE total_cases > 10000
        ORDER BY Death_Rate_Pct DESC
        LIMIT 5
    """).show()

# ─── LOAD ─────────────────────────────────────────────────────
def load(df, output_path, logger):
    """Save cleaned enriched data as Parquet."""
    logger.info(f"Saving to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Data saved successfully.")

# ─── MAIN ────────────────────────────────────────────────────
if __name__ == "__main__":
    logger = setup_logger()
    logger.info("="*55)
    logger.info("COVID ETL PIPELINE STARTED")
    logger.info("="*55)

    INPUT_PATH  = "data/raw/covid_data.csv"
    OUTPUT_PATH = "data/processed/covid_clean_parquet"

    try:
        spark    = create_spark_session(logger)
        df_raw   = extract(spark, INPUT_PATH, logger)
        df_clean = transform(df_raw, logger)
        analyze(df_clean, spark, logger)
        load(df_clean, OUTPUT_PATH, logger)

        logger.info("="*55)
        logger.info("COVID PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*55)

    except FileNotFoundError as e:
        logger.error(f"FILE ERROR: {e}")
    except Exception as e:
        logger.error(f"PIPELINE FAILED: {e}")
    finally:
        spark.stop()
        logger.info("Spark session stopped.")
