from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.types import FloatType
import logging
import os
from datetime import datetime

# ─── LOGGING SETUP ───────────────────────────────────────────
def setup_logger():
    """Configure professional logging for pipeline."""
    # Create logs directory if not exists
    os.makedirs("logs", exist_ok=True)

    # Log filename with timestamp
    log_file = f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),    # saves to file
            logging.StreamHandler()           # prints to terminal
        ]
    )
    return logging.getLogger(__name__)

# ─── Initialize Spark ────────────────────────────────────────
def create_spark_session(logger):
    """Create and return a Spark session."""
    logger.info("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("Superstore_Batch_ETL") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark session created successfully.")
    return spark

# ─── EXTRACT ─────────────────────────────────────────────────
def extract(spark, input_path, logger):
    """Load raw CSV data into Spark DataFrame."""
    logger.info(f"Extracting data from: {input_path}")

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    row_count = df.count()
    logger.info(f"Extraction complete. Rows loaded: {row_count}")
    return df

# ─── TRANSFORM ───────────────────────────────────────────────
def transform(df, logger):
    """
    Clean and transform raw Superstore data.
    - Drop rows with null Sales
    - Cast Sales to Float
    - Aggregate Sales by Region and Category
    """
    logger.info("Starting transformation...")

    # Drop nulls
    initial_count = df.count()
    df_clean = df.dropna(subset=["Sales"])
    after_null = df_clean.count()
    logger.info(f"Null rows removed: {initial_count - after_null}")

    # Cast Sales to float
    df_clean = df_clean.withColumn(
        "Sales", col("Sales").cast(FloatType())
    )

    # Remove negative sales
    df_clean = df_clean.filter(col("Sales") > 0)
    final_count = df_clean.count()
    logger.info(f"Negative sales removed: {after_null - final_count}")

    # Aggregate
    df_aggregated = df_clean.groupBy("Region", "Category") \
        .agg(spark_sum("Sales").alias("Total_Sales")) \
        .orderBy("Region", "Category")

    logger.info(f"Transformation complete. Output rows: {df_aggregated.count()}")
    return df_aggregated

# ─── LOAD ─────────────────────────────────────────────────────
def load(df, output_path, logger):
    """Save final cleaned data as Parquet format."""
    logger.info(f"Loading data to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Data successfully saved as Parquet.")

# ─── MAIN PIPELINE ────────────────────────────────────────────
if __name__ == "__main__":
    logger = setup_logger()
    logger.info("="*50)
    logger.info("SUPERSTORE ETL PIPELINE STARTED")
    logger.info("="*50)

    INPUT_PATH  = "data/raw/superstore.csv"
    OUTPUT_PATH = "data/processed/superstore_clean_parquet"

    try:
        spark          = create_spark_session(logger)
        df_raw         = extract(spark, INPUT_PATH, logger)
        df_transformed = transform(df_raw, logger)
        load(df_transformed, OUTPUT_PATH, logger)

        logger.info("="*50)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*50)

        df_transformed.show()

    except FileNotFoundError as e:
        logger.error(f"FILE ERROR: {e}")
    except Exception as e:
        logger.error(f"PIPELINE FAILED: {e}")
    finally:
        spark.stop()
        logger.info("Spark session stopped.")
