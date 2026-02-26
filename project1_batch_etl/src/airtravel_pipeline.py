from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg
from pyspark.sql.types import IntegerType
import logging
import os
from datetime import datetime

# ─── LOGGING SETUP ───────────────────────────────────────────
def setup_logger():
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/airtravel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

# ─── Initialize Spark ────────────────────────────────────────
def create_spark_session(logger):
    logger.info("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("Airtravel_Batch_ETL") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark session created.")
    return spark

# ─── EXTRACT ─────────────────────────────────────────────────
def extract(spark, input_path, logger):
    logger.info(f"Extracting data from: {input_path}")

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Clean column names
    for old_col in df.columns:
        df = df.withColumnRenamed(
            old_col, old_col.strip().replace('"','')
        )

    logger.info(f"Rows loaded: {df.count()}")
    return df

# ─── TRANSFORM ───────────────────────────────────────────────
def transform(df, logger):
    logger.info("Starting transformation...")

    df_clean = df \
        .withColumn("1958", col("1958").cast(IntegerType())) \
        .withColumn("1959", col("1959").cast(IntegerType())) \
        .withColumn("1960", col("1960").cast(IntegerType()))

    df_clean = df_clean.dropna()

    df_clean = df_clean \
        .withColumn("Total_3Years",
            col("1958") + col("1959") + col("1960")) \
        .withColumn("Avg_Passengers",
            ((col("1958")+col("1959")+col("1960"))/3
            ).cast(IntegerType()))

    logger.info(f"Transformation complete. Rows: {df_clean.count()}")
    return df_clean

# ─── LOAD ─────────────────────────────────────────────────────
def load(df, output_path, logger):
    logger.info(f"Saving to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Data saved successfully.")

# ─── MAIN ────────────────────────────────────────────────────
if __name__ == "__main__":
    logger = setup_logger()
    logger.info("="*50)
    logger.info("AIRTRAVEL ETL PIPELINE STARTED")
    logger.info("="*50)

    INPUT_PATH  = "data/raw/airtravel.csv"
    OUTPUT_PATH = "data/processed/airtravel_clean_parquet"

    try:
        spark    = create_spark_session(logger)
        df_raw   = extract(spark, INPUT_PATH, logger)
        df_clean = transform(df_raw, logger)
        load(df_clean, OUTPUT_PATH, logger)

        logger.info("="*50)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*50)

        df_clean.show()

    except FileNotFoundError as e:
        logger.error(f"FILE ERROR: {e}")
    except Exception as e:
        logger.error(f"PIPELINE FAILED: {e}")
    finally:
        spark.stop()
        logger.info("Spark session stopped.")
