"""
Eurostat Data Ingestion Script
Φορτώνει data από Eurostat API → PostgreSQL
IDEMPOTENT: Δεν ξανακατεβάζει αν η βάση έχει ήδη data
"""

import pandas as pd
from sqlalchemy import create_engine, text
from pyjstat import pyjstat
import sys

# ================================
# 1. DATABASE CONNECTION
# ================================
DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/eurostat"
engine = create_engine(DATABASE_URL)

# ================================
# 2. HELPER: Έλεγχος αν table έχει data
# ================================
def table_has_data(table_name):
    """Ελέγχει αν το table υπάρχει και έχει rows"""
    try:
        query = text(f"SELECT COUNT(*) FROM {table_name}")
        with engine.connect() as conn:
            count = conn.execute(query).scalar()
            return count > 0
    except:
        return False  # Table δεν υπάρχει ή είναι άδειο

# ================================
# 3. DATASETS & ΧΩΡΕΣ
# ================================
datasets = {
    'raw_housing': 'prc_hpi_q',
    'raw_inflation_and_rents': 'prc_hicp_midx',
    'raw_wages': 'earn_nt_net'
}

target_countries = ['Denmark', 'Germany', 'Spain', 'Netherlands']

# ================================
# 4. DOWNLOAD + CLEAN + LOAD
# ================================
print("🚀 Ξεκινάω ingestion...\n")

for table_name, code in datasets.items():
    # ✅ CHECK: Αν έχει ήδη data, SKIP
    if table_has_data(table_name):
        print(f"⏭️  {table_name}: Ήδη υπάρχει data → SKIP")
        continue
    
    print(f"📡 {table_name}...")
    
    try:
        # Download
        url = f'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{code}?format=JSON&lang=en'
        df = pyjstat.Dataset.read(url).write('dataframe')
        
        # Βρες στήλες (dynamic)
        geo_col = [c for c in df.columns if 'geo' in c.lower() or 'entity' in c.lower()][0]
        time_col = [c for c in df.columns if 'time' in c.lower() and 'frequency' not in c.lower()][0]
        
        # Φίλτρο χώρες
        df = df[df[geo_col].isin(target_countries)].copy()
        
        # Φίλτρο έτη (2010+)
        df['year_int'] = pd.to_numeric(df[time_col].astype(str).str[:4], errors='coerce')
        df = df[df['year_int'] >= 2010].copy()
        df = df.drop(columns=['year_int'])
        
        # Φόρτωμα στη βάση
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        print(f"✅ {table_name}: {df.shape[0]} rows loaded\n")
        
    except Exception as e:
        print(f"❌ {table_name}: {e}\n")

# ================================
# 5. ΤΕΛΙΚΗ ΕΠΙΒΕΒΑΙΩΣΗ
# ================================
print("🎉 ΟΛΟΚΛΗΡΩΘΗΚΕ!")
print("\n📊 Tables Status:")
for table_name in datasets.keys():
    has_data = table_has_data(table_name)
    status = "✅" if has_data else "❌"
    print(f"   {status} {table_name}")