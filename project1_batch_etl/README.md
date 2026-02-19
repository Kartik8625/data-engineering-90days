# Project 1 — Batch ETL Pipeline (PySpark)

## Overview
A batch ETL pipeline that processes raw Superstore 
sales data using PySpark, performs data cleaning and 
aggregation, and stores results in Parquet format.

## Tech Stack
- Python 3.12
- PySpark 3.5.0
- Ubuntu / WSL2
- Parquet Storage
- Git & GitHub

## Dataset
- Sample Superstore Sales Data
- Contains: Orders, Region, Category, Sales, Profit

## Pipeline Flow
```
Raw CSV → Extract → Clean Nulls → Cast Types → 
Aggregate by Region & Category → Save as Parquet
```

## Project Structure
```
project1_batch_etl/
  data/
    raw/          ← Input CSV data
    processed/    ← Output Parquet files
  src/
    etl_pipeline.py  ← Main ETL script
  README.md
```

## How to Run
```bash
# Activate environment
source de_env/bin/activate

# Run pipeline
python3 src/etl_pipeline.py
```

## Output
Sales summary table aggregated by Region and Category
stored as optimized Parquet format.

## Skills Demonstrated
- PySpark DataFrame operations
- Data cleaning and null handling
- Aggregation and transformation logic
- Modular ETL architecture (Extract, Transform, Load)
- Linux-based development workflow
