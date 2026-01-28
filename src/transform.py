import os
import pandas as pd
from config import Config
from utils import setup_logger, clean_column_names

logger = setup_logger(__name__)


DEFAULT_OUTFILE = "transformed_crypto_data.csv"
DEFAULT_SPIKE_PCT = 0.10  # 10% move between candles


def _abs_path(filename: str) -> str:
    return os.path.join(os.path.dirname(__file__), filename)


def load_data(file_path: str) -> pd.DataFrame:
    """Load CSV data into a DataFrame and clean column names.

    Args:
        file_path (str): Path to the CSV file.
    """
    df = pd.read_csv(file_path)
    return clean_column_names(df)


def _ensure_utc(series: pd.Series) -> pd.Series:
    # Handle mixed timestamp formats in API outputs.
    return pd.to_datetime(series, utc=True, format="mixed", errors="coerce")


def _flag_bad_candle(df: pd.DataFrame) -> pd.Series:
    high = df["high"]
    low = df["low"]
    open_ = df["open"]
    close = df["close"]
    bad = (high < open_) | (high < close) | (low > open_) | (low > close) | (low > high)
    bad = bad.fillna(False)
    return bad


def _flag_spike(df: pd.DataFrame, spike_pct: float) -> pd.Series:
    close = df["close"]
    pct_change = close.pct_change().abs()
    spike = pct_change > spike_pct
    spike = spike.fillna(False)
    return spike


def normalize_kraken(
    df: pd.DataFrame,
    asset: str,
    interval_min: int,
    spike_pct: float,
) -> pd.DataFrame:
    df = df.copy()
    df["time"] = _ensure_utc(df["time"])
    df = df.sort_values("time").drop_duplicates("time")

    numeric_cols = ["open", "high", "low", "close", "vwap", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["count"] = pd.to_numeric(df["count"], errors="coerce")

    df = df.rename(columns={"time": "ts_start"})
    df = df.set_index("ts_start")

    freq = f"{interval_min}T"
    full_index = pd.date_range(df.index.min(), df.index.max(), freq=freq, tz="UTC")
    df = df.reindex(full_index)
    df.index.name = "ts_start"

    df["is_missing"] = df["open"].isna()
    df["bad_candle"] = _flag_bad_candle(df)
    df["spike_flag"] = _flag_spike(df, spike_pct)
    df["anomaly_flag"] = df["bad_candle"] | df["spike_flag"]

    df["source"] = "kraken"
    df["asset"] = asset
    df["interval_min"] = interval_min

    df = df.reset_index()
    return df[
        [
            "source",
            "asset",
            "interval_min",
            "ts_start",
            "open",
            "high",
            "low",
            "close",
            "vwap",
            "volume",
            "count",
            "is_missing",
            "bad_candle",
            "spike_flag",
            "anomaly_flag",
        ]
    ]


def normalize_coingecko(
    df: pd.DataFrame,
    asset: str,
    interval_min: int,
    spike_pct: float,
) -> pd.DataFrame:
    df = df.copy()
    df["ts"] = _ensure_utc(df["ts"])
    df = df.sort_values("ts").drop_duplicates("ts")

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    df = df.set_index("ts")
    freq = f"{interval_min}T"

    ohlc = df["price"].resample(freq).ohlc()
    # CoinGecko "total_volumes" is a snapshot-like value; use mean per interval.
    volume = df["volume"].resample(freq).mean()

    out = ohlc.join(volume.rename("volume"))
    out.index.name = "ts_start"

    out["is_missing"] = out["open"].isna()
    out["bad_candle"] = _flag_bad_candle(out)
    out["spike_flag"] = _flag_spike(out, spike_pct)
    out["anomaly_flag"] = out["bad_candle"] | out["spike_flag"]

    out["source"] = "coingecko"
    out["asset"] = asset
    out["interval_min"] = interval_min
    out["vwap"] = pd.NA
    out["count"] = pd.NA

    out = out.reset_index()
    return out[
        [
            "source",
            "asset",
            "interval_min",
            "ts_start",
            "open",
            "high",
            "low",
            "close",
            "vwap",
            "volume",
            "count",
            "is_missing",
            "bad_candle",
            "spike_flag",
            "anomaly_flag",
        ]
    ]


def transform(
    coingecko_path: str,
    kraken_path: str,
    interval_min: int,
    spike_pct: float,
) -> pd.DataFrame:
    cg = load_data(coingecko_path)
    kr = load_data(kraken_path)

    asset = getattr(Config, "DEFAULT_COINGECKO_COIN_ID", "bitcoin")
    asset = asset.upper()

    cg_norm = normalize_coingecko(cg, asset, interval_min, spike_pct)
    kr_norm = normalize_kraken(kr, asset, interval_min, spike_pct)

    combined = pd.concat([cg_norm, kr_norm], ignore_index=True)
    combined = combined.sort_values(["ts_start", "source"]).reset_index(drop=True)
    return combined


def main() -> None:
    coingecko_path = _abs_path("coingecko_data.csv")
    kraken_path = _abs_path("kraken_ohlc.csv")
    interval_min = getattr(Config, "DEFAULT_KRAKEN_INTERVAL_MIN", 5)
    spike_pct = DEFAULT_SPIKE_PCT

    logger.info("Starting transform step...")
    df = transform(coingecko_path, kraken_path, interval_min, spike_pct)

    out_path = _abs_path(DEFAULT_OUTFILE)
    df.to_csv(out_path, index=False)
    logger.info(f"Wrote transformed data to {out_path}")


if __name__ == "__main__":
    main()
