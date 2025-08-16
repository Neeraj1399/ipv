import plotly.express as px
import pandas as pd

def _validate_df(df: pd.DataFrame, required_cols: list) -> bool:
    """Check if DataFrame is not empty and contains required columns."""
    return not df.empty and all(col in df.columns for col in required_cols)


def plot_total_trend(
    agg_df: pd.DataFrame,
    x: str = "period",
    y: str = "total",
    category_col: str = "vehicle_category",
    title: str = "Total registrations over time"
):
    """Line chart of total registrations over time, split by vehicle category."""
    if not _validate_df(agg_df, [x, y]):
        return px.line(title="No data available")

    agg_df_sorted = agg_df.sort_values(x)

    fig = px.line(
        agg_df_sorted,
        x=x,
        y=y,
        color=category_col if category_col in agg_df.columns else None,
        markers=True,
        title=title
    )
    fig.update_layout(hovermode="x unified", yaxis_title="Registrations")
    return fig


def plot_yoy(
    agg_df: pd.DataFrame,
    x: str = "period",
    y: str = "yoy_pct",
    category_col: str = "vehicle_category",
    title: str = "YoY % change"
):
    """Bar chart showing YoY percentage change."""
    if not _validate_df(agg_df, [x, y]):
        return px.bar(title="No data available")

    fig = px.bar(
        agg_df.sort_values(x),
        x=x,
        y=y,
        color=category_col if category_col in agg_df.columns else None,
        barmode="group",
        text=y,
        title=title
    )
    fig.update_layout(hovermode="x unified", yaxis_title="YoY (%)")
    return fig


def plot_top_manufacturers(
    df_period: pd.DataFrame,
    period_label: str,
    top_n: int = 10,
    title: str = None
):
    """
    Bar chart of top manufacturers by total registrations for a given period.
    """
    if not _validate_df(df_period, ["manufacturer", "total"]):
        return px.bar(title="No data available")

    if title is None:
        title = f"Top {top_n} manufacturers — {period_label}"

    top = (
        df_period.groupby("manufacturer", as_index=False)
        .agg(total=("total", "sum"))
        .sort_values("total", ascending=False)
        .head(top_n)
    )

    fig = px.bar(
        top,
        x="manufacturer",
        y="total",
        text="total",
        title=title
    )
    fig.update_layout(
        xaxis={"categoryorder": "total descending"},
        yaxis_title="Registrations",
        xaxis_tickangle=-45
    )
    return fig


def plot_manufacturer_trend(
    agg_df: pd.DataFrame,
    maker_list,
    title: str = "Manufacturer trend"
):
    """Line chart showing trends for selected manufacturers."""
    if not _validate_df(agg_df, ["manufacturer", "total"]):
        return px.line(title="No data available")

    d = agg_df[agg_df["manufacturer"].isin(maker_list)]
    if d.empty:
        return px.line(title="No data available for selected manufacturers")

    d_sorted = d.sort_values("period")

    fig = px.line(
        d_sorted,
        x="period",
        y="total",
        color="manufacturer",
        markers=True,
        title=title
    )
    fig.update_layout(hovermode="x unified", yaxis_title="Registrations")
    return fig
