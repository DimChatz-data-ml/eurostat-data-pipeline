import logging

import polars as pl
import requests
from pyjstat import pyjstat
from sqlalchemy import create_engine, inspect, text

from src.config import DATABASE_URL

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

engine = create_engine(DATABASE_URL)

# ---------------- CONFIG ----------------
TARGET_COUNTRIES = ["Denmark", "Germany", "Spain", "Netherlands"]

DATASETS = {
    "raw_housing": "prc_hpi_q",
    "raw_inflation_and_rents": "prc_hicp_midx",
    "raw_wages": "earn_nt_net",
}

API_TIMEOUT = 30  # seconds

# ---------------- HELPERS ----------------
def table_has_data(table_name: str) -> bool:
    """Ελέγχει αν το table υπάρχει και έχει rows."""
    try:
        inspector = inspect(engine)
        if table_name not in inspector.get_table_names():
            return False
        with engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            return count > 0
    except Exception as e:
        logger.warning(f"Could not check table '{table_name}': {e}")
        return False


# ---------------- EXTRACT ----------------
def extract(code: str) -> pl.DataFrame:
    """
    Κάνουμε εμείς το HTTP request (για timeout control) και
    περνάμε το parsed JSON dict στον pyjstat constructor,
    αντί να αφήσουμε το pyjstat να κάνει το δικό του HTTP call.
    pyjstat.Dataset είναι OrderedDict subclass → δέχεται dict απευθείας.
    """
    url = (
        f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
        f"/{code}?format=JSON&lang=en"
    )

    response = requests.get(url, timeout=API_TIMEOUT)
    response.raise_for_status()

    df_pandas = pyjstat.Dataset(response.json()).write("dataframe")

    return pl.from_pandas(df_pandas)


# ---------------- TRANSFORM ----------------
def transform(df: pl.DataFrame, table_name: str) -> pl.DataFrame:
    """Φιλτράρει ανά χώρα και χρόνο. Δουλεύει αποκλειστικά με Polars."""

    geo_col = next(
        (c for c in df.columns if "geo" in c.lower() or "entity" in c.lower()),
        None,
    )
    if geo_col is None:
        raise ValueError(f"[{table_name}] No geo column found. Columns: {df.columns}")

    time_col = next(
        (c for c in df.columns if "time" in c.lower() and "frequency" not in c.lower()),
        None,
    )
    if time_col is None:
        raise ValueError(f"[{table_name}] No time column found. Columns: {df.columns}")

    return (
        df.filter(pl.col(geo_col).is_in(TARGET_COUNTRIES))
        .with_columns(
            pl.col(time_col)
            .cast(pl.String)
            .str.slice(0, 4)
            .cast(pl.Int32, strict=False)
            .alias("_year")  # underscore για να αποφύγουμε conflict με existing column
        )
        .filter(pl.col("_year") >= 2010)
        .drop("_year")
    )


# ---------------- LOAD ----------------
def load(df: pl.DataFrame, table_name: str) -> None:
    """Γράφει το DataFrame στη PostgreSQL μέσω pandas bridge."""
    df.to_pandas().to_sql(
        table_name,
        engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )


# ---------------- PIPELINE ----------------
def run() -> None:
    logger.info("🚀 Starting ETL pipeline...")

    for table_name, code in DATASETS.items():

        if table_has_data(table_name):
            logger.info(f"⏭️  {table_name}: data already exists → SKIP")
            continue

        try:
            logger.info(f"📡 Extracting {table_name} ({code})")
            raw = extract(code)

            logger.info(f"🔧 Transforming {table_name} — {len(raw)} raw rows")
            clean = transform(raw, table_name)

            logger.info(f"💾 Loading {table_name}")
            load(clean, table_name)

            logger.info(f"✅ {table_name}: {len(clean)} rows loaded")

        except Exception as e:
            logger.error(f"❌ {table_name} failed: {e}", exc_info=True)

    logger.info("🎉 Pipeline finished.")


if __name__ == "__main__":
    run()
