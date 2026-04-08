{{ config(materialized='view') }}

WITH source AS (
    -- Βήμα 1: Διαβάζουμε τα raw data
    SELECT * 
    FROM {{ source('raw', 'raw_housing') }}),

filtered AS (
    -- Βήμα 2: Φιλτράρουμε
    -- - Μόνο 4 χώρες
    -- - Μόνο 'Quarterly index, 2015=100' (ώστε να ταιριάζει με το inflation)
    -- - Μόνο τα quarterly periods
    SELECT *
    FROM source
    WHERE "Geopolitical entity (reporting)" IN ('Denmark', 'Germany', 'Spain', 'Netherlands')
      AND "Unit of measure" = 'Quarterly index, 2015=100'
      AND "Time frequency" = 'Quarterly'
),

renamed AS (
    -- Βήμα 3: Μετονομάζουμε στήλες και προσθέτουμε τον τύπο αγοράς
    SELECT
        "Geopolitical entity (reporting)" AS country,
        "Time" AS raw_time,
        "value" AS housing_index,
        -- Μετατρέπουμε την πλήρη περιγραφή σε short slug
        CASE 
            WHEN "Purchases" = 'Total' THEN 'total'
            WHEN "Purchases" = 'Purchases of newly built dwellings' THEN 'new'
            WHEN "Purchases" = 'Purchases of existing dwellings' THEN 'existing'
            ELSE 'unknown'
        END AS purchase_type,
        -- Προσθέτουμε το flag για το αν είναι total
        CASE 
            WHEN "Purchases" = 'Total' THEN TRUE 
            ELSE FALSE 
        END AS is_total
    FROM filtered
),

with_date AS (
    -- Βήμα 4: Μετατρέπουμε το "2010-Q1" → DATE "2010-01-01"
    SELECT
        country,
        housing_index,
        purchase_type,
        is_total,
        -- Μετατροπή του quarterly format σε πρώτη μέρα του τριμήνου
        CASE 
            WHEN raw_time LIKE '%-Q1' THEN TO_DATE(SUBSTRING(raw_time FROM 1 FOR 4) || '-01-01', 'YYYY-MM-DD')
            WHEN raw_time LIKE '%-Q2' THEN TO_DATE(SUBSTRING(raw_time FROM 1 FOR 4) || '-04-01', 'YYYY-MM-DD')
            WHEN raw_time LIKE '%-Q3' THEN TO_DATE(SUBSTRING(raw_time FROM 1 FOR 4) || '-07-01', 'YYYY-MM-DD')
            WHEN raw_time LIKE '%-Q4' THEN TO_DATE(SUBSTRING(raw_time FROM 1 FOR 4) || '-10-01', 'YYYY-MM-DD')
            ELSE NULL
        END AS quarter_start
    FROM renamed
)

-- Βήμα 5: Τελικό select
SELECT
    country,
    quarter_start,
    purchase_type,
    is_total,
    housing_index
FROM with_date
WHERE quarter_start IS NOT NULL  -- Αποφυγή potential NULL dates