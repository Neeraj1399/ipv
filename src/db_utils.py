"""
Optional SQL-based analytics using DuckDB.
You can load pandas DataFrames into DuckDB and run SQL queries for YoY/QoQ if desired.
"""

import duckdb
from pathlib import Path
import pandas as pd
from src.processing import aggregate  # Use your existing aggregation function

DB_PATH = Path("data/analytics.duckdb")

def load_df_to_duckdb(df: pd.DataFrame, table_name: str, freq: str = "Y", db_path: str = None):
    """
    Load a pandas DataFrame into DuckDB as:
      1. Raw table (`vahan.<table_name>_raw`)
      2. Aggregated table (`vahan.<table_name>`)

    Args:
        df (pd.DataFrame): Raw normalized DataFrame with columns:
                           ['period', 'year', 'vehicle_category', 'manufacturer', 'registrations']
        table_name (str): Name for aggregated table (e.g., "monthly_agg", "quarterly_agg", "yearly_agg")
        freq (str): Aggregation frequency - 'M' (monthly), 'Q' (quarterly), 'Y' (yearly)
        db_path (str, optional): Path to DuckDB file
    """
    if db_path is None:
        db_path = str(DB_PATH)

    con = duckdb.connect(database=db_path, read_only=False)
    con.execute("CREATE SCHEMA IF NOT EXISTS vahan")

    # Store raw DataFrame
    con.execute(f"DROP TABLE IF EXISTS vahan.{table_name}_raw")
    con.register("tmp_raw", df)
    con.execute(f"CREATE TABLE vahan.{table_name}_raw AS SELECT * FROM tmp_raw")
    con.unregister("tmp_raw")

    # Aggregate
    agg_df = aggregate(df, freq=freq)

    # Store aggregated DataFrame
    con.execute(f"DROP TABLE IF EXISTS vahan.{table_name}")
    con.register("tmp_agg", agg_df)
    con.execute(f"CREATE TABLE vahan.{table_name} AS SELECT * FROM tmp_agg")
    con.unregister("tmp_agg")

    con.close()
    print(f"✅ Loaded raw table vahan.{table_name}_raw and aggregated table vahan.{table_name} ({db_path})")


def sql_yoy_query(db_path: str = None, table_name: str = "monthly_agg"):
    """
    SQL query for YoY % change using aggregated table with:
    period (DATE), vehicle_category, manufacturer, total.

    Returns DataFrame with:
      period, vehicle_category, manufacturer, total, total_prev_year, yoy_pct
    """
    if db_path is None:
        db_path = str(DB_PATH)

    con = duckdb.connect(database=db_path, read_only=True)

    sql = f"""
    WITH data AS (
      SELECT
        CAST(period AS DATE) AS period,
        vehicle_category,
        manufacturer,
        total
      FROM vahan.{table_name}
    ),
    lagged AS (
      SELECT
        period,
        vehicle_category,
        manufacturer,
        total,
        LAG(total, 12) OVER (
          PARTITION BY vehicle_category, manufacturer
          ORDER BY period
        ) AS total_prev_year
      FROM data
    )
    SELECT
      period,
      vehicle_category,
      manufacturer,
      total,
      total_prev_year,
      CASE 
        WHEN total_prev_year IS NULL OR total_prev_year = 0 THEN NULL
        ELSE (total / total_prev_year - 1) * 100
      END AS yoy_pct
    FROM lagged
    ORDER BY vehicle_category, manufacturer, period
    """

    df = con.execute(sql).df()
    con.close()
    return df
