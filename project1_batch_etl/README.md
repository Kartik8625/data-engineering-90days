# Project 1 — Batch ETL Pipeline 🚀

## Overview
A production-grade Batch ETL Pipeline processing
real-world datasets using PySpark, Python, and Linux.
Built as part of a 90-day Data Engineering journey.

## Architecture
![Pipeline Summary](docs/pipeline_summary.png)

## Tech Stack
| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.12 | Core language |
| PySpark | 3.5.0 | Distributed processing |
| Java | 17 | Spark runtime |
| Ubuntu/WSL2 | 24.04 | Development environment |
| Git/GitHub | Latest | Version control |

## Datasets
| Dataset | Rows | Source |
|---------|------|--------|
| Superstore Sales | 40 | Kaggle |
| Airtravel Passengers | 12 | FSU Open Data |
| COVID Global Data | 247 countries | Our World in Data |

## Pipeline Flow
```
Raw CSV → Extract → Transform → Load → Parquet
              ↓
         Spark SQL Analytics
              ↓
         Visualizations
```

## Project Structure
```
project1_batch_etl/
  src/
    etl_pipeline.py        ← Superstore ETL
    airtravel_pipeline.py  ← Airtravel ETL
    covid_pipeline.py      ← COVID ETL
    spark_sql_demo.py      ← SQL analytics
    visualize_results.py   ← Charts generator
  config/
    pipeline_config.py     ← Centralized settings
  data/
    raw/                   ← Input datasets
    processed/             ← Parquet outputs
  docs/                    ← Architecture diagrams
  logs/                    ← Pipeline run logs
  requirements.txt
  README.md
```

## Key Results

### Superstore Analysis
![Superstore Sales](docs/superstore_sales.png)
- Central region leads with $10,206 total sales
- South region lowest at $2,452

### Airtravel Trend
![Airtravel Trend](docs/airtravel_trend.png)
- Consistent growth from 1958 to 1960
- August peak travel month every year

### COVID Insights
![COVID Analysis](docs/covid_death_rate.png)
- High income countries: 0.53% death rate
- Upper middle income: 1.47% death rate
- Economic status directly correlates with outcomes

## Spark SQL Queries
- Total sales aggregation by region
- Category performance analysis
- Top 5 orders identification
- HAVING clause filtering
- CASE WHEN performance classification

## Professional Practices
- ✅ Modular architecture (extract/transform/load)
- ✅ Professional logging with timestamps
- ✅ Error handling (try/except/finally)
- ✅ Centralized configuration
- ✅ Data visualization
- ✅ Version control (Git/GitHub)

## How to Run
```bash
# Clone repository
git clone https://github.com/Kartik8625/data-engineering-90days.git

# Navigate to project
cd project1_batch_etl

# Create virtual environment
python3 -m venv de_env
source de_env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run pipelines
python3 src/etl_pipeline.py
python3 src/airtravel_pipeline.py
python3 src/covid_pipeline.py
python3 src/spark_sql_demo.py
python3 src/visualize_results.py
```

## Skills Demonstrated
- PySpark DataFrame operations
- ETL pipeline design
- Data cleaning and transformation
- Spark SQL analytical queries
- Data visualization (matplotlib)
- Professional logging
- Error handling
- Linux development workflow
