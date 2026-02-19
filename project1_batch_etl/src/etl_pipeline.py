from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.types import FloatType

# ─── Initialize Spark ───────────────────────────────────────────
def create_spark_session():
    """Create and return a Spark session."""
    spark = SparkSession.builder \
        .appName("Superstore_Batch_ETL") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# ─── EXTRACT ────────────────────────────────────────────────────
def extract(spark, input_path):
    """Load raw CSV data into a Spark DataFrame."""
    print(">>> Extracting raw data...")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    print(f"    Rows loaded: {df.count()}")
    return df

# ─── TRANSFORM ──────────────────────────────────────────────────
def transform(df):
    """
    Clean and transform raw Superstore data.
    - Drop rows with null Sales
    - Cast Sales to Float
    - Aggregate Sales by Region and Category
    """
    print(">>> Transforming data...")

    # Drop nulls in Sales column
    df_clean = df.dropna(subset=["Sales"])

    # Cast Sales to float
    df_clean = df_clean.withColumn("Sales", col("Sales").cast(FloatType()))

    # Aggregate: Total Sales by Region and Category
    df_aggregated = df_clean.groupBy("Region", "Category") \
        .agg(spark_sum("Sales").alias("Total_Sales")) \
        .orderBy("Region", "Category")

    print("    Transformation complete.")
    return df_aggregated

# ─── LOAD ───────────────────────────────────────────────────────
def load(df, output_path):
    """Save final cleaned data as Parquet format."""
    print(">>> Loading data to Parquet...")
    df.write.mode("overwrite").parquet(output_path)
    print(f"    Data saved to: {output_path}")

# ─── MAIN PIPELINE ──────────────────────────────────────────────
if __name__ == "__main__":
    # File paths
    INPUT_PATH  = "data/raw/superstore.csv"
    OUTPUT_PATH = "data/processed/superstore_clean_parquet"

    # Run pipeline
    spark = create_spark_session()
    df_raw        = extract(spark, INPUT_PATH)
    df_transformed = transform(df_raw)
    load(df_transformed, OUTPUT_PATH)

    # Preview output
    print("\n>>> Final Output Preview:")
    df_transformed.show()

    print("\n Pipeline completed successfully!")
    spark.stop()
