import math
from typing import Tuple

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from sklearn.metrics import mean_absolute_error, root_mean_squared_error


def load_predictions(use_clustering: bool = False) -> pd.DataFrame:
    """Load predictions CSV from default path (standard or clustering)"""
    file_path = (
        "data/predictions/clustering_predictions.csv"
        if use_clustering
        else "data/predictions/predictions.csv"
    )
    df = pd.read_csv(file_path)
    df.columns = [c.strip() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["cons_h"] = pd.to_numeric(df["cons_h"], errors="coerce")
    df = df.dropna(subset=["date", "customer", "cons_h"])
    return df


def load_actuals() -> pd.DataFrame:
    """Load actual data CSV from default path"""
    df = pd.read_csv("data/processed/data.csv")
    df.columns = [c.strip() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["consumption"] = pd.to_numeric(df["consumption"], errors="coerce")
    df = df.dropna(subset=["date", "customer", "consumption"])
    return df


def merge_pred_actual(pred: pd.DataFrame, actual: pd.DataFrame) -> pd.DataFrame:
    """Merge predictions and actuals on date and customer"""
    dfp = pred.rename(columns={"cons_h": "pred"})
    dfa = actual.rename(columns={"consumption": "actual"})
    merged = pd.merge(dfp, dfa, on=["date", "customer"], how="inner")
    merged = merged.sort_values("date").reset_index(drop=True)
    return merged


def compute_metrics(
    y_true: np.ndarray, y_pred: np.ndarray
) -> Tuple[float, float, float]:
    """Compute MAE and RMSE"""
    if len(y_true) == 0:
        return (float("nan"), float("nan"), float("nan"))
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(root_mean_squared_error(y_true, y_pred))
    return mae, rmse


def aggregate_by_date(df: pd.DataFrame, agg_type: str = "sum") -> pd.DataFrame:
    """Aggregate per date across customers (sum or mean)"""
    if agg_type == "sum":
        agg = df.groupby("date").agg({"actual": "sum", "pred": "sum"}).reset_index()
    else:
        agg = df.groupby("date").agg({"actual": "mean", "pred": "mean"}).reset_index()
    return agg


def format_datetime(dt: pd.Timestamp) -> str:
    """Format datetime as readable string"""
    if pd.isna(dt):
        return "N/A"
    return pd.to_datetime(dt).strftime("%Y-%m-%d %H:%M:%S")


# ====================== Streamlit App ======================
st.set_page_config(page_title="Energy Consumption Prediction Dashboard", layout="wide")

st.title("Energy Consumption Prediction Dashboard (Hourly kWh)")
st.markdown(
    "Compare predicted vs. actual hourly energy consumption globally and by customer."
)

use_clustering = st.toggle("Clustering pipeline predictions", value=False)


# Load data
@st.cache_data(ttl=600)
def load_data(use_clustering: bool):
    pred_df = load_predictions(use_clustering)
    actual_df = load_actuals()
    merged_df = merge_pred_actual(pred_df, actual_df)
    return merged_df


try:
    merged_df = load_data(use_clustering)
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

if merged_df.empty:
    st.warning(
        "Merged dataset is empty — check that predictions and actuals overlap on date and customer."
    )
    st.stop()

# Prediction period
pred_start = merged_df["date"].min()
pred_end = merged_df["date"].max()
st.markdown(
    f"**Prediction period:** {format_datetime(pred_start)} — {format_datetime(pred_end)}"
)

# ====================== Global Metrics ======================
st.header("Global Metrics (All Customers)")

# Aggregations
agg_sum = aggregate_by_date(merged_df, "sum")
agg_mean = aggregate_by_date(merged_df, "mean")

# Metrics
mae_sum, rmse_sum = compute_metrics(agg_sum["actual"], agg_sum["pred"])
mae_mean, rmse_mean = compute_metrics(agg_mean["actual"], agg_mean["pred"])

# Layout
col1, col2 = st.columns(2)

with col1:
    st.subheader("Aggregated by SUM")
    st.metric("MAE (sum)", f"{mae_sum:.6f}" if not math.isnan(mae_sum) else "N/A")
    st.metric("RMSE (sum)", f"{rmse_sum:.6f}" if not math.isnan(rmse_sum) else "N/A")

with col2:
    st.subheader("Aggregated by MEAN")
    st.metric("MAE (mean)", f"{mae_mean:.6f}" if not math.isnan(mae_mean) else "N/A")
    st.metric("RMSE (mean)", f"{rmse_mean:.6f}" if not math.isnan(rmse_mean) else "N/A")


# ====================== Comparative Line Charts ======================
st.subheader("Comparative Line Charts")


def make_line_chart(df: pd.DataFrame, title: str) -> alt.Chart:
    df_plot = df.melt(
        id_vars=["date"],
        value_vars=["actual", "pred"],
        var_name="series",
        value_name="kWh",
    )
    chart = (
        alt.Chart(df_plot)
        .mark_line()
        .encode(
            x=alt.X("date:T", title="Datetime"),
            y=alt.Y("kWh:Q", title="kWh"),
            color=alt.Color("series:N", title="Series"),
            tooltip=["date:T", "series:N", "kWh:Q"],
        )
        .properties(width=900, height=300, title=title)
        .interactive()
    )
    return chart


chart_col1, chart_col2 = st.columns(2)
with chart_col1:
    st.altair_chart(
        make_line_chart(agg_sum, "Total SUM across all customers"),
        use_container_width=True,
    )
with chart_col2:
    st.altair_chart(
        make_line_chart(agg_mean, "MEAN across customers (per hour)"),
        use_container_width=True,
    )

# ====================== Per-Customer Analysis ======================
st.header("Per-Customer Analysis")

customers_sorted = sorted(merged_df["customer"].unique())
selected_customer = st.selectbox("Select customer", options=customers_sorted)

cust_df = merged_df[merged_df["customer"] == selected_customer].sort_values("date")
cust_mae, cust_rmse = compute_metrics(cust_df["actual"], cust_df["pred"])

cust_col1, cust_col2 = st.columns([1, 2])
with cust_col1:
    st.subheader(f"Metrics for customer {selected_customer}")
    st.metric("MAE", f"{cust_mae:.6f}" if not math.isnan(cust_mae) else "N/A")
    st.metric("RMSE", f"{cust_rmse:.6f}" if not math.isnan(cust_rmse) else "N/A")

with cust_col2:
    # Determine global min/max for y-axis from all customers
    global_y_min = merged_df[["actual", "pred"]].min().min()
    global_y_max = merged_df[["actual", "pred"]].max().max()

    cust_plot_df = cust_df.melt(
        id_vars=["date"],
        value_vars=["actual", "pred"],
        var_name="series",
        value_name="kWh",
    )
    chart = (
        alt.Chart(cust_plot_df)
        .mark_line()
        .encode(
            x=alt.X("date:T", title="Datetime"),
            y=alt.Y(
                "kWh:Q",
                title="kWh",
                scale=alt.Scale(domain=[global_y_min, global_y_max]),
            ),
            color=alt.Color("series:N", title="Series"),
            tooltip=["date:T", "series:N", "kWh:Q"],
        )
        .properties(
            width=700, height=300, title=f"Actual vs Predicted for {selected_customer}"
        )
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)
