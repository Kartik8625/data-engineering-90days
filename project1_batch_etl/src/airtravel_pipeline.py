from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, max as spark_max
from pyspark.sql.types import IntegerType

# ─── Initialize Spark ────────────────────────────────────────
def create_spark_session():
    """Create and return a Spark session."""
    spark = SparkSession.builder \
        .appName("Airtravel_Batch_ETL") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# ─── EXTRACT ─────────────────────────────────────────────────
def extract(spark, input_path):
    """Load raw airtravel CSV into Spark DataFrame."""
    print(">>> Extracting raw airtravel data...")
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True
    )
    # Clean column names (remove spaces and quotes)
    for old_col in df.columns:
        df = df.withColumnRenamed(old_col, old_col.strip().replace('"',''))
    print(f"    Rows loaded: {df.count()}")
    print(f"    Columns: {df.columns}")
    return df

# ─── TRANSFORM ───────────────────────────────────────────────
def transform(df):
    """
    Transform airtravel data.
    - Cast columns to Integer
    - Calculate total passengers per year
    - Find monthly averages across all years
    - Identify peak travel month
    """
    print(">>> Transforming airtravel data...")

    # Cast year columns to integer
    df_clean = df \
        .withColumn("1958", col("1958").cast(IntegerType())) \
        .withColumn("1959", col("1959").cast(IntegerType())) \
        .withColumn("1960", col("1960").cast(IntegerType()))

    # Drop any null rows
    df_clean = df_clean.dropna()

    # Add total column across all 3 years
    df_clean = df_clean.withColumn(
        "Total_3Years",
        col("1958") + col("1959") + col("1960")
    )

    # Add average column across all 3 years
    df_clean = df_clean.withColumn(
        "Avg_Passengers",
        ((col("1958") + col("1959") + col("1960")) / 3).cast(IntegerType())
    )

    print("    Transformation complete.")
    return df_clean

# ─── LOAD ─────────────────────────────────────────────────────
def load(df, output_path):
    """Save transformed data as Parquet."""
    print(">>> Loading data to Parquet...")
    df.write.mode("overwrite").parquet(output_path)
    print(f"    Data saved to: {output_path}")

# ─── MAIN PIPELINE ────────────────────────────────────────────
if __name__ == "__main__":
    INPUT_PATH  = "data/raw/airtravel.csv"
    OUTPUT_PATH = "data/processed/airtravel_clean_parquet"

    spark       = create_spark_session()
    df_raw      = extract(spark, INPUT_PATH)
    df_clean    = transform(df_raw)
    load(df_clean, OUTPUT_PATH)

    # Preview full output
    print("\n>>> Final Output — Monthly Airtravel Analysis:")
    df_clean.show()

    # Summary stats
    print(">>> Year-wise Total Passengers:")
    df_clean.agg(
        spark_sum("1958").alias("Total_1958"),
        spark_sum("1959").alias("Total_1959"),
        spark_sum("1960").alias("Total_1960")
    ).show()

    print(">>> Peak Travel Month (highest 3-year total):")
    df_clean.orderBy(col("Total_3Years").desc()).show(1)

    print("\n Pipeline completed successfully!")
    spark.stop()

