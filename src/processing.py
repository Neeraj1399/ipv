"""
Processing and metric calculations:
- Aggregations by period / quarter / year
- YoY and QoQ (or PoP for yearly)
"""

import pandas as pd
import numpy as np
from typing import Tuple


def add_time_period_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add year, month, and quarter helper columns from 'period'."""
    if df.empty or "period" not in df.columns:
        return df

    df = df.copy()
    df["period"] = pd.to_datetime(df["period"], errors="coerce")
    df["year"] = df["period"].dt.year
    df["month"] = df["period"].dt.month
    df["quarter"] = df["period"].dt.to_period("Q").dt.to_timestamp()
    return df


def aggregate(df: pd.DataFrame, freq: str = "Y") -> pd.DataFrame:
    """
    Aggregate registrations to a frequency:
    - freq: 'M' (monthly), 'Q' (quarterly), 'Y' (yearly)
    Returns DataFrame with: period, vehicle_category, manufacturer, total
    """
    if df.empty or "registrations" not in df.columns:
        return pd.DataFrame(columns=["period", "vehicle_category", "manufacturer", "total"])

    df = df.copy()
    df["period"] = pd.to_datetime(df["period"], errors="coerce")

    if freq == "M":
        df["period_floor"] = df["period"].dt.to_period("M").dt.to_timestamp()
    elif freq == "Q":
        df["period_floor"] = df["period"].dt.to_period("Q").dt.to_timestamp()
    elif freq == "Y":
        df["period_floor"] = df["period"].dt.to_period("Y").dt.to_timestamp()
    else:
        raise ValueError("freq must be 'M', 'Q', or 'Y'")

    agg = (
        df.groupby(["period_floor", "vehicle_category", "manufacturer"], as_index=False)
          .agg(total=("registrations", "sum"))
    )
    agg = agg.rename(columns={"period_floor": "period"})
    return agg.sort_values(["vehicle_category", "manufacturer", "period"])


def compute_growth(agg: pd.DataFrame, freq: str = "Y") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute YoY / QoQ / PoP based on aggregation frequency.
    Returns:
        main_df: with YoY/QoQ/PoP
        q_df: quarterly aggregated DF for monthly granularity (else None)
    """
    if agg.empty or "total" not in agg.columns:
        return agg, None

    df = agg.copy()
    df = df.sort_values(["vehicle_category", "manufacturer", "period"])

    df["yoy_pct"] = np.nan
    df["qoq_pct"] = np.nan
    df["pop_pct"] = np.nan

    if freq == "M":  # Monthly → YoY vs same month last year, QoQ from quarter totals
        df["yoy_pct"] = (
            df.groupby(["vehicle_category", "manufacturer"])["total"]
              .transform(lambda s: s.pct_change(12) * 100)
        )

        # Quarterly aggregation for QoQ
        q = (
            df.assign(quarter=df["period"].dt.to_period("Q").dt.to_timestamp())
              .groupby(["quarter", "vehicle_category", "manufacturer"], as_index=False)
              .agg(total=("total", "sum"))
              .sort_values(["vehicle_category", "manufacturer", "quarter"])
        )
        q["qoq_pct"] = (
            q.groupby(["vehicle_category", "manufacturer"])["total"]
              .transform(lambda s: s.pct_change(1) * 100)
        )
        return df, q.rename(columns={"quarter": "period"})

    elif freq == "Q":  # Quarterly → YoY vs same quarter last year, QoQ vs previous quarter
        df["yoy_pct"] = (
            df.groupby(["vehicle_category", "manufacturer"])["total"]
              .transform(lambda s: s.pct_change(4) * 100)
        )
        df["qoq_pct"] = (
            df.groupby(["vehicle_category", "manufacturer"])["total"]
              .transform(lambda s: s.pct_change(1) * 100)
        )
        return df, None

    elif freq == "Y":  # Yearly → YoY vs previous year, PoP = same as YoY
        df["yoy_pct"] = (
            df.groupby(["vehicle_category", "manufacturer"])["total"]
              .transform(lambda s: s.pct_change(1) * 100)
        )
        df["pop_pct"] = df["yoy_pct"]
        return df, None

    else:
        raise ValueError("freq must be 'M', 'Q', or 'Y'")


def top_n_manufacturers(df_agg: pd.DataFrame, period, n: int = 10) -> pd.DataFrame:
    """
    Return top N manufacturers for the given period.
    Works across categories unless filtered before calling.
    """
    if df_agg.empty or "manufacturer" not in df_agg.columns:
        return pd.DataFrame(columns=["manufacturer", "total"])

    mask = df_agg["period"] == pd.to_datetime(period)
    if not mask.any():
        return pd.DataFrame(columns=["manufacturer", "total"])

    top = (
        df_agg[mask]
        .groupby("manufacturer", as_index=False)
        .agg(total=("total", "sum"))
        .sort_values("total", ascending=False)
        .head(n)
    )
    return top
