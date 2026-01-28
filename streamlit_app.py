import os
from datetime import timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text


st.set_page_config(page_title="Crypto ETL Trustboard", page_icon="📈", layout="wide")

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&display=swap');
    html, body, [class*="css"]  {
        font-family: 'Space Grotesk', sans-serif;
    }
    .main {
        background: radial-gradient(1200px 600px at 10% 10%, #f5f9ff 0%, #f7f4ff 40%, #ffffff 100%);
    }
    .kpi-card {
        padding: 16px 18px;
        border-radius: 12px;
        background: linear-gradient(135deg, #0c1b33 0%, #142b4d 100%);
        color: #f8fafc;
        border: 1px solid rgba(255,255,255,0.08);
        box-shadow: 0 8px 24px rgba(2, 6, 23, 0.12);
    }
    .kpi-label {
        font-size: 12px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        opacity: 0.7;
    }
    .kpi-value {
        font-size: 28px;
        font-weight: 600;
        margin-top: 6px;
    }
    .panel {
        padding: 18px;
        border-radius: 14px;
        background: #ffffff;
        border: 1px solid #e2e8f0;
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.06);
    }
    .action-card {
        padding: 14px 16px;
        border-radius: 10px;
        background: #fff7ed;
        border: 1px solid #fed7aa;
        color: #9a3412;
        font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def _get_db_url() -> str | None:
    if "DATABASE_URL" in st.secrets:
        return st.secrets["DATABASE_URL"]
    return os.getenv("DATABASE_URL")


@st.cache_data(ttl=300)
def _load_from_db(db_url: str, days: int) -> pd.DataFrame:
    engine = create_engine(db_url)
    query = text(
        """
        SELECT
            source, asset, interval_min, ts_start, open, high, low, close,
            vwap, volume, count, is_missing, bad_candle, spike_flag, anomaly_flag
        FROM fact_price_candle
        WHERE ts_start >= (NOW() - (:days * INTERVAL '1 day'))
        ORDER BY ts_start ASC
        """
    )
    return pd.read_sql(query, engine, params={"days": days})


@st.cache_data(ttl=300)
def _load_from_csv(csv_path: str) -> pd.DataFrame:
    return pd.read_csv(csv_path, parse_dates=["ts_start"])


def _percent(n: float) -> str:
    return f"{n:.1%}"


def _time_ago(delta: timedelta) -> str:
    minutes = int(delta.total_seconds() // 60)
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 48:
        return f"{hours}h ago"
    days = hours // 24
    return f"{days}d ago"


st.title("Crypto ETL Trustboard")
st.caption("Actionable data-quality and market signal insights from your ETL pipeline.")

with st.sidebar:
    st.subheader("Controls")
    days = st.slider("Lookback window (days)", min_value=1, max_value=30, value=7)
    source_filter = st.multiselect("Sources", options=["coingecko", "kraken"], default=["coingecko", "kraken"])
    st.markdown("---")
    st.markdown("Set `DATABASE_URL` in Streamlit secrets to connect to Postgres.")


db_url = _get_db_url()
data: pd.DataFrame
if db_url:
    data = _load_from_db(db_url, days)
else:
data = _load_from_csv(os.path.join("src", "transformed_crypto_data.csv"))

if data.empty:
    st.warning("No data found for the selected window.")
    st.stop()

data["ts_start"] = pd.to_datetime(data["ts_start"], utc=True)
if source_filter:
    data = data[data["source"].isin(source_filter)]
else:
    st.warning("Select at least one source to display results.")
    st.stop()
data = data.sort_values("ts_start")

latest = data.dropna(subset=["close"]).groupby("source").tail(1)
latest_price = latest["close"].mean() if not latest.empty else float("nan")

now_utc = pd.Timestamp.utcnow()
freshness = now_utc - data["ts_start"].max()
anomaly_rate = data["anomaly_flag"].fillna(False).mean()
missing_rate = data["is_missing"].fillna(False).mean()

price_change = None
if data["close"].notna().sum() > 1:
    price_change = (data["close"].iloc[-1] / data["close"].iloc[0]) - 1

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(
        f"<div class='kpi-card'><div class='kpi-label'>Latest Price</div>"
        f"<div class='kpi-value'>{'N/A' if pd.isna(latest_price) else f'${latest_price:,.2f}'}</div></div>",
        unsafe_allow_html=True,
    )
with col2:
    st.markdown(
        f"<div class='kpi-card'><div class='kpi-label'>Freshness</div>"
        f"<div class='kpi-value'>{_time_ago(freshness)}</div></div>",
        unsafe_allow_html=True,
    )
with col3:
    st.markdown(
        f"<div class='kpi-card'><div class='kpi-label'>Missing Rate</div>"
        f"<div class='kpi-value'>{_percent(missing_rate)}</div></div>",
        unsafe_allow_html=True,
    )
with col4:
    st.markdown(
        f"<div class='kpi-card'><div class='kpi-label'>Anomaly Rate</div>"
        f"<div class='kpi-value'>{_percent(anomaly_rate)}</div></div>",
        unsafe_allow_html=True,
    )

st.markdown("### Action Panel")
action_col1, action_col2, action_col3 = st.columns(3)
with action_col1:
    if freshness > timedelta(hours=2):
        st.markdown("<div class='action-card'>Pipeline stale — investigate ETL run.</div>", unsafe_allow_html=True)
    else:
        st.success("Data is fresh and flowing.")
with action_col2:
    if missing_rate > 0.1:
        st.markdown("<div class='action-card'>High missing rate — backfill recent window.</div>", unsafe_allow_html=True)
    else:
        st.info("Coverage is healthy.")
with action_col3:
    if anomaly_rate > 0.02:
        st.markdown("<div class='action-card'>Spike anomalies elevated — verify source data.</div>", unsafe_allow_html=True)
    else:
        st.info("Anomaly rate within normal range.")

st.markdown("### Price & Volume")
price_df = data.dropna(subset=["close"])
fig_price = px.line(
    price_df,
    x="ts_start",
    y="close",
    color="source",
    title="Close Price",
    labels={"close": "Price", "ts_start": "Timestamp"},
)
fig_price.update_layout(legend_title_text="Source", height=380)
st.plotly_chart(fig_price, use_container_width=True)

vol_df = data.dropna(subset=["volume"])
fig_vol = px.area(
    vol_df,
    x="ts_start",
    y="volume",
    color="source",
    title="Reported Volume",
    labels={"volume": "Volume", "ts_start": "Timestamp"},
)
fig_vol.update_layout(legend_title_text="Source", height=320)
st.plotly_chart(fig_vol, use_container_width=True)

st.markdown("### Quality & Source Alignment")
quality_col1, quality_col2 = st.columns([2, 1])

with quality_col1:
    anomaly_points = data[data["anomaly_flag"].fillna(False)]
    fig_anom = px.scatter(
        anomaly_points,
        x="ts_start",
        y="close",
        color="source",
        title="Anomalies Over Time",
        labels={"close": "Close", "ts_start": "Timestamp"},
    )
    fig_anom.update_layout(height=320)
    st.plotly_chart(fig_anom, use_container_width=True)

with quality_col2:
    st.markdown("**Data health breakdown**")
    health_df = pd.DataFrame(
        {
            "Metric": ["Missing", "Bad Candle", "Spike"],
            "Rate": [
                missing_rate,
                data["bad_candle"].fillna(False).mean(),
                data["spike_flag"].fillna(False).mean(),
            ],
        }
    )
    fig_health = px.bar(health_df, x="Metric", y="Rate", text=health_df["Rate"].map(_percent))
    fig_health.update_layout(height=320, yaxis_tickformat=".0%")
    st.plotly_chart(fig_health, use_container_width=True)

st.markdown("### Source Divergence")
if set(source_filter) == {"coingecko", "kraken"}:
    pivot = data.pivot_table(index="ts_start", columns="source", values="close", aggfunc="mean")
    pivot = pivot.dropna()
    if not pivot.empty:
        pivot["pct_diff"] = (pivot["kraken"] - pivot["coingecko"]) / pivot["coingecko"]
        fig_div = go.Figure()
        fig_div.add_trace(go.Scatter(x=pivot.index, y=pivot["pct_diff"], name="Kraken vs CoinGecko"))
        fig_div.update_layout(
            yaxis_tickformat=".2%",
            title="Price Divergence (Kraken vs CoinGecko)",
            height=320,
        )
        st.plotly_chart(fig_div, use_container_width=True)
    else:
        st.info("Not enough overlapping data to compute divergence.")
else:
    st.info("Select both sources to view divergence.")

st.markdown("### Summary")
summary_cols = st.columns(3)
with summary_cols[0]:
    st.metric("Price Change", _percent(price_change) if price_change is not None else "N/A")
with summary_cols[1]:
    st.metric("Rows Loaded", f"{len(data):,}")
with summary_cols[2]:
    st.metric("Sources", ", ".join(sorted(data["source"].unique())))
