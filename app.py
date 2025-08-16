import streamlit as st
from pathlib import Path
import pandas as pd
from src.data_loader import load_and_normalize
from src.processing import aggregate, compute_growth
from src.plots import plot_total_trend, plot_yoy, plot_top_manufacturers, plot_manufacturer_trend
from src.utils import format_big_number, safe_pct
from src.db_utils import load_df_to_duckdb, sql_yoy_query

# Path to folder containing CSV files
DATA_FOLDER = Path("data")

st.set_page_config(page_title="Vehicle Registrations — Investor Dashboard", layout="wide")
st.title("🚘 Vehicle Registration Dashboard — Investor View")
st.markdown(
    "Interactive dashboard focused on vehicle type-wise (2W/3W/4W) and manufacturer-wise "
    "registrations. Shows YoY and QoQ growth with filters and exports."
)

# ------------------ Load & normalize ------------------
with st.spinner("Loading and normalizing data..."):
    df_raw, metadata = load_and_normalize(DATA_FOLDER)
    files_loaded = [f.name for f in DATA_FOLDER.glob("maker *.csv")]
    if not files_loaded:
        st.error("No 'maker *.csv' files found in the data folder.")
        st.stop()

    st.sidebar.markdown("**Data files loaded:** " + ", ".join(files_loaded))
    st.sidebar.markdown(f"**Detected granularity:** {metadata.get('granularity', 'unknown')}")

# ------------------ Decide aggregation frequency ------------------
gran = metadata.get('granularity', 'yearly')
if gran == 'monthly':
    freq = 'M'
elif gran == 'quarterly':
    freq = 'Q'
else:
    freq = 'Y'  # fallback to yearly

df_raw['period'] = pd.to_datetime(df_raw['period'])
df_raw = df_raw.sort_values('period')

# ------------------ Sidebar filters ------------------
st.sidebar.header("Filters")
min_period = df_raw['period'].min()
max_period = df_raw['period'].max()

# Date range filter
if freq == 'Y':
    years = sorted(df_raw['period'].dt.year.unique())
    start_year, end_year = st.sidebar.select_slider(
        "Year range", options=years, value=(years[0], years[-1])
    )
    mask_period = (df_raw['period'].dt.year >= start_year) & (df_raw['period'].dt.year <= end_year)
else:
    start_date, end_date = st.sidebar.date_input(
        "Date range", value=(min_period.date(), max_period.date())
    )
    mask_period = (df_raw['period'].dt.date >= start_date) & (df_raw['period'].dt.date <= end_date)

# Vehicle category filter
all_cats = sorted(df_raw['vehicle_category'].unique())
default_cats = [c for c in all_cats if c in ['2W', '3W', '4W']] or all_cats[:3]
sel_cats = st.sidebar.multiselect("Vehicle categories", all_cats, default=default_cats)

# Manufacturer filter — ALL manufacturers available
maker_totals = (
    df_raw.groupby('manufacturer', as_index=False)
    .agg(total=('registrations', 'sum'))
    .sort_values('total', ascending=False)
)
all_makers = sorted(df_raw['manufacturer'].unique())
top_makers_default = maker_totals.head(10)['manufacturer'].tolist()

sel_makers = st.sidebar.multiselect(
    "Manufacturers (select any)", all_makers, default=top_makers_default
)

# ------------------ Apply filters ------------------
df_filtered = df_raw[
    mask_period
    & df_raw['vehicle_category'].isin(sel_cats)
    & df_raw['manufacturer'].isin(sel_makers)
]

# ------------------ Export / SQL ------------------
st.sidebar.markdown("---")
st.sidebar.markdown("Export / SQL")

if st.sidebar.button("Load aggregated to DuckDB (optional)"):
    agg_for_db = aggregate(df_raw, freq=freq)
    load_df_to_duckdb(agg_for_db, table_name="agg_table")
    st.sidebar.success("Loaded to DuckDB (data/analytics.duckdb)")

if st.sidebar.button("Run example SQL YoY query (DuckDB)"):
    try:
        df_sql_yoy = sql_yoy_query(table_name="agg_table")
        st.sidebar.success("SQL YoY query executed — results available below")
        st.session_state['sql_yoy'] = df_sql_yoy
    except Exception as e:
        st.sidebar.error(f"SQL error: {e}")

# ------------------ Aggregate & Growth ------------------
agg = aggregate(df_filtered, freq=freq)
agg_growth, agg_q = compute_growth(agg, freq)  # ✅ FIXED — now passes M/Q/Y directly

# ------------------ KPIs ------------------
st.markdown("### Key performance indicators (selected filters)")
latest_period = agg_growth['period'].max()
total_latest = agg_growth[agg_growth['period'] == latest_period]['total'].sum()

# YoY %
agg_ts = agg_growth.groupby('period', as_index=False).agg(total=('total', 'sum')).sort_values('period')
if len(agg_ts) >= 2:
    last_total = agg_ts.iloc[-1]['total']
    if freq == 'M':
        prev_year_period = agg_ts['period'].iloc[-1] - pd.DateOffset(years=1)
        prev_row = agg_ts[agg_ts['period'] == prev_year_period]
        yoy_pct = ((last_total / prev_row['total'].values[0] - 1) * 100) if not prev_row.empty else None
    else:
        prev_row = agg_ts.iloc[-2]
        yoy_pct = ((last_total / prev_row['total'] - 1) * 100) if prev_row['total'] != 0 else None
else:
    yoy_pct = None

# QoQ / PoP %
if agg_q is not None and not agg_q.empty:
    q_df = agg_q.groupby('period', as_index=False).agg(total=('total', 'sum')).sort_values('period')
    if len(q_df) >= 2:
        q_last, q_prev = q_df.iloc[-1]['total'], q_df.iloc[-2]['total']
        qoq_pct = ((q_last / q_prev - 1) * 100) if q_prev != 0 else None
    else:
        qoq_pct = None
else:
    if len(agg_ts) >= 2:
        q_last, q_prev = agg_ts.iloc[-1]['total'], agg_ts.iloc[-2]['total']
        qoq_pct = ((q_last / q_prev - 1) * 100) if q_prev != 0 else None
    else:
        qoq_pct = None

col1, col2, col3 = st.columns(3)
col1.metric("Total Registrations (latest)", format_big_number(total_latest))
col2.metric("YoY % (latest)", safe_pct(yoy_pct))
col3.metric("QoQ / PoP % (latest)", safe_pct(qoq_pct))

st.markdown("---")

# ------------------ Charts ------------------
st.subheader("Overall trend")
st.plotly_chart(plot_total_trend(agg_growth), use_container_width=True)

st.subheader("YoY % change")
st.plotly_chart(plot_yoy(agg_growth), use_container_width=True)

# Top manufacturers
st.subheader(f"Top manufacturers — {latest_period.date()}")
df_latest_period = agg_growth[agg_growth['period'] == latest_period][['manufacturer', 'total']]
if not df_latest_period.empty:
    st.plotly_chart(
        plot_top_manufacturers(df_latest_period, period_label=str(latest_period.date())),
        use_container_width=True
    )
else:
    st.write("No data for the latest period with current filters.")

# Manufacturer trend — limit to 6
st.subheader("Manufacturer trend (select up to 6 manufacturers)")

available_makers = sorted(agg_growth['manufacturer'].unique())

# Only set defaults if they exist in the current list
possible_defaults = [m for m in top_makers_default if m in available_makers][:6]

maker_selection = st.multiselect(
    "Pick manufacturers to plot",
    options=available_makers,
    default=possible_defaults if possible_defaults else None
)

if len(maker_selection) > 6:
    st.warning("You can select up to 6 manufacturers only.")
    maker_selection = maker_selection[:6]

if maker_selection:
    st.plotly_chart(plot_manufacturer_trend(agg_growth, maker_selection), use_container_width=True)

# ------------------ Data table ------------------
st.subheader("Aggregated data (preview)")
st.dataframe(
    agg_growth.sort_values(['period', 'vehicle_category', 'manufacturer'])
    .reset_index(drop=True)
    .head(500)
)

# ------------------ CSV export ------------------
csv = agg_growth.to_csv(index=False)
st.download_button(
    "Download aggregated CSV",
    csv,
    file_name="aggregated_vehicle_registrations.csv",
    mime="text/csv"
)

# ------------------ SQL results ------------------
if 'sql_yoy' in st.session_state:
    st.subheader("SQL YoY results (DuckDB example)")
    st.dataframe(st.session_state['sql_yoy'].head(200))

# ------------------ Provenance ------------------
st.markdown("---")
st.markdown("#### Data provenance & notes")
st.markdown("- Source files: " + ", ".join(files_loaded) + " (from `data/` folder).")
st.markdown("- YoY uses a lag of 12 months (monthly), 4 quarters (quarterly), or 1 year (yearly).")
