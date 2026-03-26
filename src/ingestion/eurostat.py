import logging
import polars as pl
import requests
from pyjstat import pyjstat
from sqlalchemy import create_engine, inspect, text
from src.config import DATABASE_URL

# LOGGING CONFIGURATION
# Streamline process monitoring and debugging visibility
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("ingestion.log"), 
        logging.StreamHandler()              
    ]
)
logger = logging.getLogger(__name__)

# Initialize SQLAlchemy engine for PostgreSQL persistence
engine = create_engine(DATABASE_URL)

# INGESTION CONFIGURATION
# Defined scope for data filtering to optimize storage and compute resources
TARGET_COUNTRIES = ["Denmark", "Germany", "Spain", "Netherlands"]

# Mapping of database table names to Eurostat dataset codes
DATASETS = {
    "raw_housing": "prc_hpi_q",
    "raw_inflation_and_rents": "prc_hicp_midx",
    "raw_wages": "earn_nt_net",
}

API_TIMEOUT = 30  # Request timeout in seconds to prevent hanging processes

# HELPERS:idempotency step 
def table_has_data(table_name: str) -> bool:

    try:
        inspector = inspect(engine)
        if table_name not in inspector.get_table_names():
            return False
        with engine.connect() as conn:
            # Efficiently verify row count to determine if ingestion is required
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            return count > 0
    except Exception as e:
        logger.warning(f"Database state check failed for '{table_name}': {e}")
        return False


# EXTRACT
def extract(code: str) -> pl.DataFrame:
  
    url = (
        f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
        f"/{code}?format=JSON&lang=en"
    )

    response = requests.get(url, timeout=API_TIMEOUT)
    response.raise_for_status()

    # Parse JSON-stat format into a Pandas DataFrame, then cast to Polars for performance
    df_pandas = pyjstat.Dataset(response.json()).write("dataframe")
    return pl.from_pandas(df_pandas)


# TRANSFORM (Smart Ingestion Layer)
def transform(df: pl.DataFrame, table_name: str) -> pl.DataFrame:
   
    # Dynamically identify Geographic and Temporal columns to handle API schema variations
    geo_col = next(
        (c for c in df.columns if "geo" in c.lower() or "entity" in c.lower()),
        None,
    )
    if geo_col is None:
        raise ValueError(f"[{table_name}] Geographic column not identified. Available: {df.columns}")

    time_col = next(
        (c for c in df.columns if "time" in c.lower() and "frequency" not in c.lower()),
        None,
    )
    if time_col is None:
        raise ValueError(f"[{table_name}] Temporal column not identified. Available: {df.columns}")

    # Row filtering: Limit dataset to target scope (geography & timeframe >= 2010)
    return (
        df.filter(pl.col(geo_col).is_in(TARGET_COUNTRIES))
        .with_columns(
            pl.col(time_col)
            .cast(pl.String)
            .str.slice(0, 4)
            .cast(pl.Int32, strict=False)
            .alias("_ingestion_filter_year")
        )
        .filter(pl.col("_ingestion_filter_year") >= 2010)
        .drop("_ingestion_filter_year") # Maintain raw schema by dropping temporary helper column
    )


# LOAD
def load(df: pl.DataFrame, table_name: str) -> None:
  
    df.to_pandas().to_sql(
        table_name,
        engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )


# ORCHESTRATION PIPELINE
def run() -> None:
 
    logger.info("Initiating Smart Ingestion Pipeline...")

    for table_name, code in DATASETS.items():

        if table_has_data(table_name):
            logger.info(f"Dataset '{table_name}' already populated. Skipping ingestion.")
            continue

        try:
            logger.info(f"Extracting source data: {code}")
            raw = extract(code)

            logger.info(f"Applying ingestion filters: {table_name}")
            clean = transform(raw, table_name)

            logger.info(f"Loading records to PostgreSQL: {table_name}")
            load(clean, table_name)

            logger.info(f"Successful ingestion: {len(clean)} rows added.")

        except Exception as e:
            logger.error(f" Ingestion failed for '{table_name}': {e}", exc_info=True)

    logger.info("Ingestion Pipeline execution completed.")


if __name__ == "__main__":
    run()