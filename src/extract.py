import requests
import json
import sys
import os
import pandas as pd
from config import Config
from utils import setup_logger


logger = setup_logger(__name__)


def fetch_coingecko_market_chart(coin_id: str, vs_currency: str, days: int) -> pd.DataFrame:
    """Returns DataFrame containing market chart data from CoinGecko API."""

    url= f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days}


    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    prices = pd.DataFrame(data["prices"], columns=["ts_ms", "price"])
    vols = pd.DataFrame(data["total_volumes"], columns=["ts_ms", "volume"])


    df = prices.merge(vols, on="ts_ms", how="left")
    df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)

    return df[["ts", "price", "volume"]].sort_values("ts").drop_duplicates("ts")

def fetch_kraken_ohlc(pair: str, interval: int, since: int | None = None) -> dict:
    """
    Kraken exchange OHLCV candles.
    Docs: /public/OHLC
    interval is in minutes (e.g., 1, 5, 15, 60, 1440).
    Note: Kraken OHLC returns up to ~720 entries for the chosen interval.
    """
    url = f"{Config.KRAKEN_BASE_URL}/0/public/OHLC"
    params = {
        "pair": pair,
        "interval": interval,
    }
    if since is not None:
        params["since"] = since  # unix timestamp seconds

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"Kraken HTTP error: {http_err} | body={getattr(resp,'text',None)}")
        sys.exit(1)
    except Exception as err:
        logger.error(f"Kraken error: {err}")
        sys.exit(1)
 


def main() -> None:
    # Use defaults from Config where appropriate so the script can be
    # exercised without editing the file.
    coin_id = getattr(Config, "DEFAULT_COINGECKO_COIN_ID", "bitcoin")
    vs_currency = getattr(Config, "DEFAULT_VS_CURRENCY", "usd")
    days = getattr(Config, "DEFAULT_DAYS", 30)

    try:
        logger.info(f"Fetching CoinGecko market chart for {coin_id}...")
        df = fetch_coingecko_market_chart(coin_id, vs_currency, days)
        coingecko_csv = os.path.join(os.path.dirname(__file__), "coingecko_data.csv")
        df.to_csv(coingecko_csv, index=False)
        logger.info(f"Wrote CoinGecko data to {coingecko_csv}")
    except Exception as e:
        logger.error(f"Error fetching market chart data: {e}")

    # Fetch Kraken OHLC and present a small DataFrame sample for quick testing.
    kraken_pair = getattr(Config, "DEFAULT_KRAKEN_PAIR", "XBTUSD")
    kraken_interval = getattr(Config, "DEFAULT_KRAKEN_INTERVAL_MIN", 5)

    try:
        logger.info(f"Fetching Kraken OHLC for {kraken_pair}...")
        kraken_resp = fetch_kraken_ohlc(kraken_pair, kraken_interval)

        # Kraken returns JSON like {"error":[], "result": {"PAIRNAME": [[...], ...], "last": 12345}}
        if not kraken_resp:
            raise RuntimeError("Empty response from Kraken")

        if kraken_resp.get("error"):
            logger.error(f"Kraken API returned errors: {kraken_resp.get('error')}")
        else:
            result = kraken_resp.get("result", {})
            # find the first key that is not 'last'
            pair_key = next((k for k in result.keys() if k != "last"), None)
            if pair_key is None:
                logger.error("Could not find pair data in Kraken response")
            else:
                ohlc_list = result.get(pair_key, [])
                # columns per Kraken docs: time, open, high, low, close, vwap, volume, count
                kraken_df = pd.DataFrame(ohlc_list, columns=["time", "open", "high", "low", "close", "vwap", "volume", "count"])
                kraken_df["time"] = pd.to_datetime(kraken_df["time"], unit="s", utc=True)
                kraken_csv = os.path.join(os.path.dirname(__file__), "kraken_ohlc.csv")
                kraken_df.to_csv(kraken_csv, index=False)
                logger.info(f"Wrote Kraken OHLC data to {kraken_csv}")
    except Exception as e:
        logger.error(f"Error fetching Kraken OHLC data: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
