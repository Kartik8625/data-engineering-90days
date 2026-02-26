from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType

# ─── Initialize Spark ────────────────────────────────────────
def create_spark_session():
    """Create Spark session for SQL operations."""
    spark = SparkSession.builder \
        .appName("SparkSQL_Demo") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# ─── EXTRACT & REGISTER ──────────────────────────────────────
def load_and_register(spark, path):
    """
    Load CSV and register as SQL temporary view.
    This allows running real SQL queries on the data.
    """
    print(">>> Loading data and registering SQL view...")
    df = spark.read.csv(path, header=True, inferSchema=True)

    # Clean Sales column
    df = df.withColumn("Sales", col("Sales").cast(FloatType()))
    df = df.dropna(subset=["Sales", "Region", "Category"])

    # THIS IS THE KEY STEP
    # Register DataFrame as a temporary SQL table
    df.createOrReplaceTempView("orders")
    print(f"    Rows loaded: {df.count()}")
    print("    SQL view 'orders' registered successfully.")
    return df

# ─── SQL QUERIES ─────────────────────────────────────────────
def run_sql_queries(spark):
    """
    Run real SQL queries on registered view.
    This demonstrates Spark SQL capability.
    """

    print("\n" + "="*50)
    print(">>> QUERY 1: Total Sales by Region")
    print("="*50)
    spark.sql("""
        SELECT Region,
               ROUND(SUM(Sales), 2) AS Total_Sales
        FROM orders
        GROUP BY Region
        ORDER BY Total_Sales DESC
    """).show()

    print("="*50)
    print(">>> QUERY 2: Average Sales by Category")
    print("="*50)
    spark.sql("""
        SELECT Category,
               ROUND(AVG(Sales), 2) AS Avg_Sales,
               COUNT(*) AS Order_Count
        FROM orders
        GROUP BY Category
        ORDER BY Avg_Sales DESC
    """).show()

    print("="*50)
    print(">>> QUERY 3: Top 5 Highest Sales Orders")
    print("="*50)
    spark.sql("""
        SELECT Region,
               Category,
               ROUND(Sales, 2) AS Sales
        FROM orders
        ORDER BY Sales DESC
        LIMIT 5
    """).show()

    print("="*50)
    print(">>> QUERY 4: Regions with Sales above Average")
    print("="*50)
    spark.sql("""
        SELECT Region,
               ROUND(SUM(Sales), 2) AS Total_Sales
        FROM orders
        GROUP BY Region
        HAVING SUM(Sales) > (
            SELECT AVG(Sales) FROM orders
        )
        ORDER BY Total_Sales DESC
    """).show()

    print("="*50)
    print(">>> QUERY 5: Sales Performance Category")
    print("="*50)
    spark.sql("""
        SELECT Region,
               Category,
               ROUND(SUM(Sales), 2) AS Total_Sales,
               CASE
                   WHEN SUM(Sales) > 3000 THEN 'High Performer'
                   WHEN SUM(Sales) > 1000 THEN 'Mid Performer'
                   ELSE 'Low Performer'
               END AS Performance
        FROM orders
        GROUP BY Region, Category
        ORDER BY Total_Sales DESC
    """).show()

# ─── SAVE RESULTS ────────────────────────────────────────────
def save_results(spark, output_path):
    """Save final SQL query result as Parquet."""
    print(">>> Saving final results...")
    result = spark.sql("""
        SELECT Region,
               Category,
               ROUND(SUM(Sales), 2) AS Total_Sales,
               COUNT(*) AS Order_Count,
               ROUND(AVG(Sales), 2) AS Avg_Sales
        FROM orders
        GROUP BY Region, Category
        ORDER BY Total_Sales DESC
    """)
    result.write.mode("overwrite").parquet(output_path)
    print(f"    Results saved to: {output_path}")

# ─── MAIN ────────────────────────────────────────────────────
if __name__ == "__main__":
    INPUT_PATH  = "data/raw/superstore.csv"
    OUTPUT_PATH = "data/processed/sql_analysis_parquet"

    spark = create_spark_session()
    load_and_register(spark, INPUT_PATH)
    run_sql_queries(spark)
    save_results(spark, OUTPUT_PATH)

    print("\n✅ Spark SQL Demo completed successfully!")
    spark.stop()
