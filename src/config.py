
import os
from dotenv import load_dotenv, find_dotenv
from typing import Optional

# Load .env from project root (if present). Using find_dotenv() so running
# from `src/` still picks up the repository root `.env` file.
load_dotenv(find_dotenv())


class Config:
    """Application configuration read from environment variables.

    Notes:
    - Prefer explicit variable names matching the project's .env keys.
    - Provide safe defaults where appropriate.
    """

    COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
    KRAKEN_BASE_URL = "https://api.kraken.com"

    # CoinGecko API key (named COINGECKO_API_KEY in .env). Keep a backward
    # compatible alias in case CG_API_KEY is used elsewhere.
    COINGECKO_API_KEY: Optional[str] = os.getenv("COINGECKO_API_KEY") or os.getenv("CG_API_KEY")

    # Kraken API key
    KRAKEN_API_KEY: Optional[str] = os.getenv("KRAKEN_API_KEY")

    # Database connection URL (optional)
    DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")


    DEFAULT_COINGECKO_COIN_ID = "bitcoin"
    DEFAULT_VS_CURRENCY = "usd"
    DEFAULT_DAYS = 7

    # Kraken uses "XBT" for Bitcoin in many pairs
    DEFAULT_KRAKEN_PAIR = "XBTUSD"
    DEFAULT_KRAKEN_INTERVAL_MIN = 5
    # DEFAULT_KRAKEN_SINCE = 1700000000  # optional unix seconds


__all__ = ["Config"]


