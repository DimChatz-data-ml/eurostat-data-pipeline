import logging
import polars as pl
import requests
from pyjstat import pyjstat
from sqlalchemy import create_engine, inspect, text
from src.config import DATABASE_URL

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("ingestion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ENGINE
engine = create_engine(DATABASE_URL)

# CONFIG
TARGET_COUNTRIES = ["Denmark", "Germany", "Spain", "Netherlands"]
DATASETS = {
    "raw_housing": "prc_hpi_q",
    "raw_inflation_and_rents": "prc_hicp_midx",
    "raw_wages": "earn_nt_net",
}
API_TIMEOUT = 30


# IDEMPOTENCY CHECK 
def table_has_data(table_name: str) -> bool:
    try:
        inspector = inspect(engine)
        if table_name not in inspector.get_table_names():
            return False
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1")).scalar()
            return result is not None
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
    df_pandas = pyjstat.Dataset(response.json()).write("dataframe")
    return pl.from_pandas(df_pandas)

# INGESTION FILTER (minimal filtering – not transformation)
def filter_scope(df: pl.DataFrame, table_name: str) -> pl.DataFrame:
    """
    Applies minimal row filtering to reduce storage footprint.
    Only keeps target countries and data from 2010 onward.
    """
    # Identify geo column
    geo_col = next(
        (c for c in df.columns if "geo" in c.lower() or "entity" in c.lower()),
        None,
    )
    if geo_col is None:
        raise ValueError(f"[{table_name}] Geographic column not identified. Available: {df.columns}")

    # Identify time column
    time_col = next(
        (c for c in df.columns if "time" in c.lower() and "frequency" not in c.lower()),
        None,
    )
    if time_col is None:
        raise ValueError(f"[{table_name}] Temporal column not identified. Available: {df.columns}")

    # Filter and extract year
    return (
        df.filter(pl.col(geo_col).is_in(TARGET_COUNTRIES))
        .with_columns(
            pl.col(time_col)
            .cast(pl.String)
            .str.extract(r"(\d{4})", 1)   # more robust than slice
            .cast(pl.Int32, strict=False)
            .alias("_ingestion_filter_year")
        )
        .filter(pl.col("_ingestion_filter_year") >= 2010)
        .drop("_ingestion_filter_year")
    )

# LOAD (transactional, native Polars)
def load(df: pl.DataFrame, table_name: str) -> None:
    """
    Loads data using a transaction: either all rows are inserted or none.
    This guarantees idempotency even if the load fails mid-way.
    """
    with engine.begin() as conn:  # auto-rollback on exception
        df.write_database(
            table_name=table_name,
            connection=conn,
            if_table_exists="replace",
            engine="sqlalchemy",   # use SQLAlchemy connection
        )

# ORCHESTRATION
def run() -> None:
    logger.info("Initiating Ingestion Pipeline...")

    for table_name, code in DATASETS.items():
        if table_has_data(table_name):
            logger.info(f"Dataset '{table_name}' already populated. Skipping ingestion.")
            continue

        try:
            logger.info(f"Extracting source data: {code}")
            raw = extract(code)

            logger.info(f"Applying ingestion filters: {table_name}")
            filtered = filter_scope(raw, table_name)

            logger.info(f"Loading records to PostgreSQL: {table_name}")
            load(filtered, table_name)

            logger.info(f"Successful ingestion: {len(filtered)} rows added.")

        except Exception as e:
            logger.error(f"Ingestion failed for '{table_name}': {e}", exc_info=True)

    logger.info("Ingestion Pipeline execution completed.")

if __name__ == "__main__":
    run()