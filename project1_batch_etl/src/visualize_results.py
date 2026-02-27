import matplotlib
matplotlib.use('Agg')  # non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.pipeline_config import DOCS_DIR
import logging

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# Create docs directory
os.makedirs(DOCS_DIR, exist_ok=True)

# ─── CHART 1 — Superstore Sales by Region ────────────────────
def chart_superstore():
    logger.info("Generating Superstore chart...")
    data = {
        'Region'     : ['Central', 'West', 'East', 'South'],
        'Total_Sales': [10206.86, 9542.90, 3401.27, 2452.39]
    }
    df = pd.DataFrame(data)

    colors = ['#2196F3', '#4CAF50', '#FF9800', '#E91E63']
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(df['Region'], df['Total_Sales'],
                  color=colors, edgecolor='white',
                  linewidth=1.5)

    # Add value labels on bars
    for bar, val in zip(bars, df['Total_Sales']):
        ax.text(bar.get_x() + bar.get_width()/2,
                bar.get_height() + 100,
                f'${val:,.2f}',
                ha='center', va='bottom',
                fontsize=11, fontweight='bold')

    ax.set_title('Superstore — Total Sales by Region',
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Region', fontsize=13)
    ax.set_ylabel('Total Sales ($)', fontsize=13)
    ax.set_facecolor('#F8F9FA')
    fig.patch.set_facecolor('#FFFFFF')
    ax.grid(axis='y', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    path = os.path.join(DOCS_DIR, 'superstore_sales.png')
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    logger.info(f"Chart saved: {path}")

# ─── CHART 2 — Airtravel Trend ───────────────────────────────
def chart_airtravel():
    logger.info("Generating Airtravel chart...")
    months = ['JAN','FEB','MAR','APR','MAY','JUN',
              'JUL','AUG','SEP','OCT','NOV','DEC']
    y1958  = [340,318,362,348,363,435,491,505,404,359,310,337]
    y1959  = [360,342,406,396,420,472,548,559,463,407,362,405]
    y1960  = [417,391,419,461,472,535,622,606,508,461,390,432]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(months, y1958, marker='o', linewidth=2.5,
            color='#2196F3', label='1958', markersize=6)
    ax.plot(months, y1959, marker='s', linewidth=2.5,
            color='#4CAF50', label='1959', markersize=6)
    ax.plot(months, y1960, marker='^', linewidth=2.5,
            color='#FF9800', label='1960', markersize=6)

    ax.fill_between(months, y1958, alpha=0.1, color='#2196F3')
    ax.fill_between(months, y1960, alpha=0.1, color='#FF9800')

    ax.set_title('Airtravel — Monthly Passengers 1958-1960',
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Month', fontsize=13)
    ax.set_ylabel('Passengers', fontsize=13)
    ax.legend(fontsize=12)
    ax.set_facecolor('#F8F9FA')
    fig.patch.set_facecolor('#FFFFFF')
    ax.grid(alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    path = os.path.join(DOCS_DIR, 'airtravel_trend.png')
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    logger.info(f"Chart saved: {path}")

# ─── CHART 3 — COVID Death Rate by Economy ───────────────────
def chart_covid():
    logger.info("Generating COVID chart...")
    data = {
        'Economic_Status': ['High Income', 'Low Income',
                            'Lower Middle', 'Upper Middle'],
        'Avg_Death_Rate' : [0.529, 1.307, 1.415, 1.468],
        'Countries'      : [42, 78, 50, 64]
    }
    df = pd.DataFrame(data).sort_values('Avg_Death_Rate')

    colors = ['#4CAF50','#FF9800','#F44336','#9C27B0']
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df['Economic_Status'],
                   df['Avg_Death_Rate'],
                   color=colors, edgecolor='white',
                   linewidth=1.5)

    for bar, val, countries in zip(bars,
                                   df['Avg_Death_Rate'],
                                   df['Countries']):
        ax.text(bar.get_width() + 0.02,
                bar.get_y() + bar.get_height()/2,
                f'{val}% ({countries} countries)',
                va='center', fontsize=11,
                fontweight='bold')

    ax.set_title('COVID — Death Rate by Economic Status',
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Average Death Rate (%)', fontsize=13)
    ax.set_ylabel('Economic Status', fontsize=13)
    ax.set_facecolor('#F8F9FA')
    fig.patch.set_facecolor('#FFFFFF')
    ax.grid(axis='x', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    path = os.path.join(DOCS_DIR, 'covid_death_rate.png')
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    logger.info(f"Chart saved: {path}")

# ─── CHART 4 — Pipeline Summary Dashboard ────────────────────
def chart_summary():
    logger.info("Generating summary dashboard...")
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle('Project 1 — Batch ETL Pipeline Summary',
                 fontsize=18, fontweight='bold', y=1.02)

    # Chart A: Datasets processed
    datasets = ['Superstore', 'Airtravel', 'COVID']
    rows     = [40, 12, 247]
    colors_a = ['#2196F3', '#4CAF50', '#FF9800']
    axes[0].bar(datasets, rows, color=colors_a,
                edgecolor='white')
    axes[0].set_title('Rows Processed per Dataset',
                      fontweight='bold')
    axes[0].set_ylabel('Row Count')
    for i, (bar, val) in enumerate(
            zip(axes[0].patches, rows)):
        axes[0].text(bar.get_x() + bar.get_width()/2,
                     bar.get_height() + 2,
                     str(val), ha='center',
                     fontweight='bold')

    # Chart B: Pipeline components
    components = ['Extract', 'Transform', 'Load', 'SQL']
    values     = [3, 3, 3, 5]
    colors_b   = ['#9C27B0','#E91E63','#FF5722','#607D8B']
    axes[1].pie(values, labels=components,
                colors=colors_b, autopct='%1.0f%%',
                startangle=90)
    axes[1].set_title('Pipeline Components',
                      fontweight='bold')

    # Chart C: Tech stack
    tech   = ['PySpark', 'Python', 'SQL', 'Linux', 'Git']
    levels = [8, 7, 6, 6, 7]
    colors_c = ['#F44336','#2196F3','#4CAF50',
                '#FF9800','#9C27B0']
    axes[2].barh(tech, levels, color=colors_c,
                 edgecolor='white')
    axes[2].set_title('Skills Developed (1-10)',
                      fontweight='bold')
    axes[2].set_xlim(0, 10)
    for bar, val in zip(axes[2].patches, levels):
        axes[2].text(bar.get_width() + 0.1,
                     bar.get_y() + bar.get_height()/2,
                     str(val), va='center',
                     fontweight='bold')

    for ax in axes:
        ax.set_facecolor('#F8F9FA')
    fig.patch.set_facecolor('#FFFFFF')

    path = os.path.join(DOCS_DIR, 'pipeline_summary.png')
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    logger.info(f"Dashboard saved: {path}")

# ─── MAIN ─────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("="*50)
    logger.info("GENERATING ALL VISUALIZATIONS")
    logger.info("="*50)

    chart_superstore()
    chart_airtravel()
    chart_covid()
    chart_summary()

    logger.info("="*50)
    logger.info("ALL CHARTS SAVED TO docs/ FOLDER")
    logger.info("="*50)
    print("\n✅ All visualizations generated successfully!")
    print(f"📁 Check docs/ folder for PNG files")
