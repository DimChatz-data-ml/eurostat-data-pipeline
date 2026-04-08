{{ config(materialized='view') }}

WITH source AS (
    -- Βήμα 1: Διαβάζουμε τα raw data
    SELECT * 
    FROM {{ source('raw', 'raw_inflation_and_rents') }}
),

filtered AS (
    -- Βήμα 2: Φιλτράρουμε
    -- - Μόνο 4 χώρες μας
    -- - Μόνο Monthly (για να είμαστε σίγουροι)
    -- - Μόνο 2015=100 (ώστε να ταιριάζει με το housing)
    -- - Μόνο 2 κατηγορίες: HICP + Rents
    SELECT *
    FROM source
    WHERE "Geopolitical entity (reporting)" IN ('Denmark', 'Germany', 'Spain', 'Netherlands')
      AND "Time frequency" = 'Monthly'
      AND "Unit of measure" = 'Index, 2015=100'
      AND "Classification of individual consumption by purpose (COICOP)" IN (
          'All-items HICP',
          'Actual rentals for housing'
      )
),

renamed AS (
    -- Βήμα 3: Μετονομάζουμε στήλες και δημιουργούμε slugs
    SELECT
        "Geopolitical entity (reporting)" AS country,
        "Time" AS raw_month,
        "value" AS monthly_value,
        -- Μετατρέπουμε τις μακροσκελείς ονομασίες σε καθαρά slugs
        CASE 
            WHEN "Classification of individual consumption by purpose (COICOP)" = 'All-items HICP' THEN 'hicp'
            WHEN "Classification of individual consumption by purpose (COICOP)" = 'Actual rentals for housing' THEN 'rents'
            ELSE 'unknown'
        END AS inflation_type,
        -- Προσθέτουμε το flag για τον γενικό πληθωρισμό
        CASE 
            WHEN "Classification of individual consumption by purpose (COICOP)" = 'All-items HICP' THEN TRUE 
            ELSE FALSE 
        END AS is_hicp
    FROM filtered
),

with_quarter AS (
    -- Βήμα 4: Μετατρέπουμε το monthly "2010-01" → quarterly DATE "2010-01-01"
    -- Χρησιμοποιούμε DATE_TRUNC για να "στρογγυλέψουμε" τον μήνα στην αρχή του τριμήνου
    SELECT
        country,
        inflation_type,
        is_hicp,
        monthly_value,
        -- Μετατροπή: "2010-01" → DATE '2010-01-01' → DATE_TRUNC('quarter') → '2010-01-01' (Q1)
        DATE_TRUNC('quarter', TO_DATE(raw_month || '-01', 'YYYY-MM-DD'))::DATE AS quarter_start
    FROM renamed
    WHERE monthly_value IS NOT NULL  -- Αποφυγή NULLs από την αρχή
),

aggregated AS (
    -- Βήμα 5: Μετατρέπουμε Monthly → Quarterly μέσω AVG()
    -- Ομαδοποίηση ανά χώρα, τύπο πληθωρισμού και τρίμηνο
    SELECT
        country,
        quarter_start,
        inflation_type,
        is_hicp,
        AVG(monthly_value) AS inflation_index
    FROM with_quarter
    GROUP BY 
        country,
        quarter_start,
        inflation_type,
        is_hicp
)

-- Βήμα 6: Τελικό select
SELECT
    country,
    quarter_start,
    inflation_type,
    is_hicp,
    inflation_index
FROM aggregated
