import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from config import Config
from utils import setup_logger, clean_column_names

# Initialize logger
logger = setup_logger(__name__)

DEFAULT_TABLE = "fact_price_candle"


def _default_db_url() -> str:
    # Local Postgres default for macOS/Homebrew setup.
    user = os.getenv("USER", "postgres")
    return f"postgresql://{user}@localhost:5432/postgres"


def create_db_engine():
    """Create a SQL engine for connecting to PostgreSQL database.
    
    Returns:
        SQLAlchemy Engine instance or None if DATABASE_URL is not set.
    """
    try:
        db_url = Config.DATABASE_URL or _default_db_url()
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        logger.error(f"Error creating database engine: {e}")
        sys.exit(1)

def load_data_to_postegresql(df, table_name: str):
    """Load DataFrame into PostgreSQL database table.

    Args:
        df (pd.DataFrame): DataFrame to load.
        table_name (str): Target table name in the database.
    """
    engine = create_db_engine()

    try:
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        logger.info(f"Data loaded into table '{table_name}' successfully.")
    except Exception as e:
        logger.error(f"Error loading data into table '{table_name}': {e}")
        sys.exit(1)

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Clean DataFrame column names by stripping spaces and converting to lowercase.

    Args:
        df (pd.DataFrame): DataFrame with columns to clean.

    Returns:
        pd.DataFrame: DataFrame with cleaned column names.
    """
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    return df


def load_transformed_csv(csv_path: str, table_name: str = DEFAULT_TABLE) -> None:
    df = pd.read_csv(csv_path)
    df = clean_column_names(df)
    load_data_to_postegresql(df, table_name)


def main() -> None:
    csv_path = os.path.join(os.path.dirname(__file__), "transformed_crypto_data.csv")
    logger.info("Loading transformed data into Postgres...")
    load_transformed_csv(csv_path, DEFAULT_TABLE)


if __name__ == "__main__":
    main()
