import os
import sqlite3
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
from datetime import datetime
from tkcalendar import DateEntry


# --- Helper Functions ---
def clean_column_names(df):
    """Clean column names."""
    df.columns = df.columns.str.strip().str.lower().str.replace(r'[^\w\s]', '_', regex=True).str.replace(' ', '_').str.replace(';', '')
    return df

def validate_data_types(conn, table_schemas):
    """Validate data types in the database tables."""
    cursor = conn.cursor()
    invalid_rows = {}
    for table_name, schema in table_schemas.items():
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        column_types = {col[1]: col[2] for col in columns_info}

        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        for row in rows:
            for col_name, col_type in schema:
                # Find the index of the column in the row
                col_index = next((i for i, col in enumerate(columns_info) if col[1] == col_name and col[2] == col_type), None)
                if (col_index is None):
                    continue
                value = row[col_index]
                if not validate_value_type(value, col_type):
                    print(f"Invalid value in table '{table_name}', column '{col_name}': {value} (expected type: {col_type})")
                    if table_name not in invalid_rows:
                        invalid_rows[table_name] = []
                    invalid_rows[table_name].append(row)
                    break
    return invalid_rows

def validate_value_type(value, expected_type):
    """Validate the value type."""
    if value is None:
        return False
    if expected_type == "TEXT":
        return isinstance(value, str)
    elif expected_type == "REAL":
        return isinstance(value, float) or isinstance(value, int)
    elif expected_type == "INTEGER":
        return isinstance(value, int)
    elif expected_type == "DATE":
        try:
            # Ensure value is a string and matches YYYY-MM-DD format
            if isinstance(value, str):
                datetime.strptime(value.strip(), '%Y-%m-%d')
            return True
        except ValueError:
            return False
    return False



def create_tables(conn):
    """Create database tables with corrected schema and relationships"""
    cursor = conn.cursor()
    
    # Drop old tables if they exist
    cursor.execute('DROP TABLE IF EXISTS airports')
    cursor.execute('DROP TABLE IF EXISTS offers')
    cursor.execute('DROP TABLE IF EXISTS offers_numeric')

    # Create offers table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS offers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supl_iata TEXT,
        valid_since DATE,
        valid_until DATE,
        confirmed TEXT,
        supplier TEXT,
        iata TEXT,
        platts TEXT,
        period TEXT,
        diff REAL,
        for_currency TEXT,
        per_volume TEXT,
        value_fee REAL,
        for_currency_1 TEXT,
        per_volume_1 TEXT,
        value_fee_1 REAL,
        for_currency_2 TEXT,
        per_volume_2 TEXT,
        value_fee_2 REAL,
        for_currency_3 TEXT,
        per_volume_3 TEXT,
        value_fee_3 REAL,
        for_currency_4 TEXT,
        per_volume_4 TEXT,
        value_fee_4 REAL,
        for_currency_5 TEXT,
        per_volume_5 TEXT,
        value_fee_5 REAL,
        for_currency_6 TEXT,
        per_volume_6 TEXT,
        value_fee_6 REAL,
        for_currency_7 TEXT,
        per_volume_7 TEXT,
        value_fee_7 REAL,        
        SAF_platts_name TEXT,
        SAF_period TEXT,
        SAF_diff REAL DEFAULT 0,
        SAF_currency TEXT DEFAULT 'USD',
        SAF_per_volume TEXT DEFAULT 'gallion.USG.UGL',
        SAF_included_as_percent REAL DEFAULT 0,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Create platts table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS platts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supl_iata TEXT,
        start DATE,
        end DATE,
        platts TEXT,
        value REAL,
        period TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Create mp table with foreign key
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS mp (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supl_iata TEXT,
        start DATE,
        end DATE,
        supplier TEXT,
        iata TEXT,
        value REAL,
        currency TEXT,
        per_volume TEXT,
        value_per_usdc REAL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Create currency table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS currency (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        currency TEXT,
        currency_name TEXT,
        value REAL,
        to_USDC REAL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Create metrics table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        metric_descr TEXT,
        metrick_in_usg REAL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Create airports table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS airports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        iata TEXT,
        icao TEXT,
        airport TEXT,
        city TEXT,
        state TEXT,
        est_flight_ttl REAL,
        est_volume_mt REAL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Create offers_numeric with foreign key
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS offers_numeric (
        id INTEGER PRIMARY KEY,
        supl_iata TEXT,
        valid_since DATE,
        valid_until DATE,
        confirmed TEXT,
        supplier TEXT,
        iata TEXT,
        platts TEXT,
        period TEXT,
        diff REAL,
        for_currency TEXT,
        per_volume TEXT,
        value_fee REAL,
        for_currency_1 TEXT,
        per_volume_1 TEXT,
        value_fee_1 REAL,
        for_currency_2 TEXT,
        per_volume_2 TEXT,
        value_fee_2 REAL,
        for_currency_3 TEXT,
        per_volume_3 TEXT,
        value_fee_3 REAL,
        for_currency_4 TEXT,
        per_volume_4 TEXT,
        value_fee_4 REAL,
        for_currency_5 TEXT,
        per_volume_5 TEXT,
        value_fee_5 REAL,
        for_currency_6 TEXT,
        per_volume_6 TEXT,
        value_fee_6 REAL,
        for_currency_7 TEXT,
        per_volume_7 TEXT,
        value_fee_7 REAL,        
        SAF_platts_name TEXT,
        SAF_period TEXT,
        SAF_diff REAL DEFAULT 0,
        SAF_currency TEXT DEFAULT 'USD',
        SAF_per_volume TEXT DEFAULT 'gallion.USG.UGL',
        SAF_included_as_percent REAL DEFAULT 0,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(id) REFERENCES offers(id) ON DELETE CASCADE
    )
    ''')

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_offers_iata ON offers (iata)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_offers_supplier ON offers (supplier)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_platts_group ON platts (TRIM(platts), TRIM(period), start, end)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mp_group ON mp (TRIM(supplier), TRIM(iata), start, end)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_currency_date ON currency (date, currency)")

    conn.commit()


def add_additional_currency_rows(conn):
    cursor = conn.cursor()

    # Retrieve the common date from any existing row
    cursor.execute("SELECT date FROM currency LIMIT 1")
    common_date_row = cursor.fetchone()

    if common_date_row:
        common_date = common_date_row[0]

        # Insert USDC row: Set value as 1 (1 USD = 1 USDC, or 100 cents)
        cursor.execute('''
        INSERT INTO currency (date, currency, currency_name, value, to_USDC)
        VALUES (?, 'USDC', 'USDC', 1, 100)
        ''', (common_date,))

        # Insert EUR row: Set value as 1 (1 EUR = 1 EUR), but leave to_USDC as NULL for conversion later
        cursor.execute('''
        INSERT INTO currency (date, currency, currency_name, value, to_USDC)
        VALUES (?, 'EUR', 'EUR', 100, NULL)
        ''', (common_date,))

    else:
        print("No rows found to retrieve the common date.")

    conn.commit()


def divide_to_USDC_by_100(conn):
    cursor = conn.cursor()

    # Update to_USDC: Convert from EUR-based to USD-based values
    cursor.execute('''
    UPDATE currency
    SET to_USDC = CASE
        WHEN currency = 'USD' THEN 100.0  -- USD is already in cents, set to 100
        WHEN currency = 'USDC' THEN 100.0  -- USDC is also 100 (1 USD = 100 cents)
        WHEN currency = 'EUR' THEN 
            -- Convert EUR to USD, then multiply by 100 to get USD cents
            ((SELECT value FROM currency WHERE currency = 'USD' LIMIT 1) * value * 100.0)        WHEN currency != 'USD' AND currency != 'USDC' AND currency != 'EUR' AND EXISTS (
            SELECT 1 FROM currency 
            WHERE currency = 'USD' AND value > 0
            LIMIT 1
        ) THEN
            -- For other currencies: (Currency to USD) / (Currency to USD rate) * 100
            ((SELECT value FROM currency WHERE currency = 'USD' LIMIT 1) / value) * 100.0
        ELSE NULL
    END;
    ''')

    conn.commit()

    # Divide to_USDC by 100 for all currencies except USDC
    #cursor.execute('''
    #UPDATE currency
   # SET to_USDC = to_USDC / 100.0
    #WHERE to_USDC IS NOT NULL AND currency != 'USDC'
   # ''')

    conn.commit()

def get_unique_values(conn, table_name, column_name):
    """Retrieves unique values from a specified column in a table."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT DISTINCT {column_name} FROM {table_name}")
    values = [row[0] for row in cursor.fetchall()]
    return values    

def clear_all_tables(conn):
    """Clear data from all tables in the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table in tables:
        table_name = table[0]
        cursor.execute(f"DELETE FROM {table_name}")
    conn.commit()

def update_offers_numeric(conn):
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE offers_numeric
SET platts = (
    SELECT COALESCE(              
        -- 2. Platts value (platts + period)
        (SELECT AVG(p.value)
         FROM platts p 
         WHERE offers_numeric.platts = p.platts 
           AND offers_numeric.period = p.period 
           AND DATE('now') BETWEEN DATE(p.start) AND DATE(p.end)
         ORDER BY p.end DESC
         LIMIT 1),
         
          -- 1. MP value (supplier + iata)
        (SELECT AVG(mp.value_per_usdc)
         FROM mp 
         WHERE mp.supplier = offers_numeric.supplier 
           AND mp.iata = offers_numeric.iata 
           AND DATE('now') BETWEEN DATE(mp.start) AND DATE(mp.end)
         ORDER BY mp.end DESC
         LIMIT 1),
        
        -- 3. Latest MP value (supplier + iata)
        (SELECT AVG(m.value_per_usdc) 
         FROM mp m 
         WHERE 
         -- m.supplier = offers_numeric.supplier AND 
            m.iata = offers_numeric.iata 
           AND DATE('now') BETWEEN DATE(m.start) AND DATE(m.end)
         ORDER BY m.end DESC
         LIMIT 1),
        
        -- 4. Latest Platts value (platts + period)
        (SELECT AVG(p.value) 
         FROM platts p 
         WHERE offers_numeric.platts = p.platts 
           AND offers_numeric.period = p.period 
         ORDER BY p.end DESC
         LIMIT 1),
        
        -- 5. Average MP value at IATA level (NEW)
        (SELECT AVG(m.value_per_usdc) 
         FROM mp m 
         WHERE m.iata = offers_numeric.iata 
         ORDER BY m.end DESC
         LIMIT 1),
        
        -- 6. Default fallback
        0
    )
);



        ''')
        conn.commit()
        print("offers_numeric table updated successfully")
    except sqlite3.Error as e:
        print(f"SQLite error updating offers_numeric: {str(e)}")
    except ValueError as e:
        print(f"Value error updating offers_numeric: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
    refresh_missing_platts_table(conn)
    refresh_missing_mp_table(conn)
    create_analyzed_offers_table(conn)
    create_enhanced_analyzed_table(conn)
    create_min_prices_table(conn)
    update_mp_value_per_usdc(conn)
    create_contracted_offers_table(conn)
    create_enhanced_contracted_table(conn)
    create_contracted_fuel_prices_table(conn)# Updated to use conn

def duplicate_and_replace_offers(conn):
    cursor = conn.cursor()

    # Drop offers_numeric table if it exists
    cursor.execute('DROP TABLE IF EXISTS offers_numeric')

    # Create offers_numeric table with the same schema as offers
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS offers_numeric AS
    SELECT * FROM offers WHERE 0
    ''')

    # Filter offers table by confirmed and include necessary columns
    cursor.execute('''
    INSERT INTO offers_numeric
    SELECT 
        id, supl_iata, valid_since, valid_until, confirmed, supplier, iata, platts, period, diff, 
        for_currency, per_volume, value_fee, for_currency_1, per_volume_1, value_fee_1, 
        for_currency_2, per_volume_2, value_fee_2, for_currency_3, per_volume_3, value_fee_3, 
        for_currency_4, per_volume_4, value_fee_4, for_currency_5, per_volume_5, value_fee_5, 
        for_currency_6, per_volume_6, value_fee_6, for_currency_7, per_volume_7, value_fee_7, 
        SAF_platts_name, SAF_period, SAF_diff, SAF_currency, SAF_per_volume, SAF_included_as_percent, 
        timestamp
    FROM offers 
    WHERE confirmed IS NOT NULL
    ''')

    # Replace values in columns starting with for_currency
    cursor.execute('''
    UPDATE offers_numeric
    SET
        for_currency = (SELECT to_USDC FROM currency WHERE currency = offers_numeric.for_currency),
        for_currency_1 = (SELECT to_USDC FROM currency WHERE currency = offers_numeric.for_currency_1),
        for_currency_2 = (SELECT to_USDC FROM currency WHERE currency = offers_numeric.for_currency_2),
        for_currency_3 = (SELECT to_USDC FROM currency WHERE currency = offers_numeric.for_currency_3),
        for_currency_4 = (SELECT to_USDC FROM currency WHERE currency = offers_numeric.for_currency_4),
        for_currency_5 = (SELECT to_USDC FROM currency WHERE currency = offers_numeric.for_currency_5),
        for_currency_6 = (SELECT to_USDC FROM currency WHERE currency = offers_numeric.for_currency_6),
        for_currency_7 = (SELECT to_USDC FROM currency WHERE currency = offers_numeric.for_currency_7),
        SAF_currency = (SELECT to_USDC FROM currency WHERE currency = offers_numeric.SAF_currency)
    ''')

    # Replace values in columns starting with per_volume
    cursor.execute('''
    UPDATE offers_numeric
    SET
        per_volume = CASE
            WHEN offers_numeric.per_volume = 'EA.Ops' 
            THEN  COALESCE(
                (SELECT (330.215 * a.est_volume_mt / NULLIF(a.est_flight_ttl, 0)) 
                 FROM airports a 
                 WHERE a.iata = offers_numeric.iata),
                5
            )
            ELSE (SELECT metrick_in_usg FROM metrics WHERE metric_descr = offers_numeric.per_volume)
        END,
        per_volume_1 = CASE
            WHEN offers_numeric.per_volume_1 = 'EA.Ops' 
            THEN  COALESCE(
                (SELECT (330.215 * a.est_volume_mt / NULLIF(a.est_flight_ttl, 0)) 
                 FROM airports a 
                 WHERE a.iata = offers_numeric.iata),
                5
            )
            ELSE (SELECT metrick_in_usg FROM metrics WHERE metric_descr = offers_numeric.per_volume_1)
        END,
        per_volume_2 = CASE
            WHEN offers_numeric.per_volume_2 = 'EA.Ops' 
            THEN COALESCE(
                (SELECT (330.215 * a.est_volume_mt / NULLIF(a.est_flight_ttl, 0)) 
                 FROM airports a 
                 WHERE a.iata = offers_numeric.iata),
                5
            )
            ELSE (SELECT metrick_in_usg FROM metrics WHERE metric_descr = offers_numeric.per_volume_2)
        END,
        per_volume_3 = CASE
            WHEN offers_numeric.per_volume_3 = 'EA.Ops' 
            THEN COALESCE(
                (SELECT (330.215 * a.est_volume_mt / NULLIF(a.est_flight_ttl, 0)) 
                 FROM airports a 
                 WHERE a.iata = offers_numeric.iata),
                5
            )
            ELSE (SELECT metrick_in_usg FROM metrics WHERE metric_descr = offers_numeric.per_volume_3)
        END,
        per_volume_4 = CASE
            WHEN offers_numeric.per_volume_4 = 'EA.Ops' 
            THEN COALESCE(
                (SELECT (330.215 * a.est_volume_mt / NULLIF(a.est_flight_ttl, 0)) 
                 FROM airports a 
                 WHERE a.iata = offers_numeric.iata),
                5
            )
            ELSE (SELECT metrick_in_usg FROM metrics WHERE metric_descr = offers_numeric.per_volume_4)
        END,
        per_volume_5 = CASE
            WHEN offers_numeric.per_volume_5 = 'EA.Ops' 
            THEN COALESCE(
                (SELECT (330.215 * a.est_volume_mt / NULLIF(a.est_flight_ttl, 0)) 
                 FROM airports a 
                 WHERE a.iata = offers_numeric.iata),
                5
            )
            ELSE (SELECT metrick_in_usg FROM metrics WHERE metric_descr = offers_numeric.per_volume_5)
        END,
        per_volume_6 = CASE
            WHEN offers_numeric.per_volume_6 = 'EA.Ops' 
            THEN COALESCE(
                (SELECT (330.215 * a.est_volume_mt / NULLIF(a.est_flight_ttl, 0)) 
                 FROM airports a 
                 WHERE a.iata = offers_numeric.iata),
                5
            )
            ELSE (SELECT metrick_in_usg FROM metrics WHERE metric_descr = offers_numeric.per_volume_6)
        END,
        per_volume_7 = CASE
            WHEN offers_numeric.per_volume_7 = 'EA.Ops' 
            THEN COALESCE(
                (SELECT (330.215 * a.est_volume_mt / NULLIF(a.est_flight_ttl, 0)) 
                 FROM airports a 
                 WHERE a.iata = offers_numeric.iata),
                5
            )
            ELSE (SELECT metrick_in_usg FROM metrics WHERE metric_descr = offers_numeric.per_volume_7)
        END,
        SAF_per_volume = CASE
            WHEN offers_numeric.SAF_per_volume = 'EA.Ops' 
            THEN COALESCE(
                (SELECT (330.215 * a.est_volume_mt / NULLIF(a.est_flight_ttl, 0)) 
                 FROM airports a 
                 WHERE a.iata = offers_numeric.iata),
                5
            )
            ELSE (SELECT metrick_in_usg FROM metrics WHERE metric_descr = offers_numeric.SAF_per_volume)
        END
    ''')

    conn.commit()
    cursor.close()

    # Call create_analyzed_offers_table after updating offers
    refresh_missing_platts_table(conn)
    refresh_missing_mp_table(conn)
    create_analyzed_offers_table(conn)
    create_enhanced_analyzed_table(conn)
    create_min_prices_table(conn)
    update_mp_value_per_usdc(conn)
    create_contracted_offers_table(conn)
    create_enhanced_contracted_table(conn)
    create_contracted_fuel_prices_table(conn)# Updated to use conn

def update_mp_value_per_usdc(conn):
    """
    Update mp.value_per_usdc by looking up conversion rates from currency and metrics tables.
    """
    cursor = conn.cursor()
    
    try:
        # Ensure the column exists before updating
        cursor.execute("PRAGMA table_info(mp);")
        columns = [row[1] for row in cursor.fetchall()]
        if "value_per_usdc" not in columns:
            print("Error: Column 'value_per_usdc' does not exist in mp.")
            return

        update_query = """
        UPDATE mp
        SET value_per_usdc = (
            SELECT ( mp.value * currency.to_USDC / metrics.metrick_in_usg)
            FROM currency
            JOIN metrics ON mp.per_volume = metrics.metric_descr
            WHERE mp.currency = currency.currency
            LIMIT 1
        )
        WHERE EXISTS (
            SELECT 1 
            FROM currency
            JOIN metrics ON mp.per_volume = metrics.metric_descr
            WHERE mp.currency = currency.currency
        );
        """

        cursor.execute(update_query)
        conn.commit()
        print(f"Updated {cursor.rowcount} rows in mp.value_per_usdc")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        conn.rollback()
    finally:
        cursor.close()

def export_all_tables_to_csv(conn=None, export_dir="exports"):
    """Exports all tables from SQLite database to CSV files in a specified directory."""
    own_connection = False
    if conn is None:
        conn = sqlite3.connect("tender_offers.db")
        own_connection = True
    
    try:
        # Ensure export_dir is a string
        if not isinstance(export_dir, str):
            export_dir = str(export_dir)
        
        os.makedirs(export_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[0]
            try:
                # Explicitly include 'id' for offers table
                if table_name == 'offers':
                    df = pd.read_sql_query("SELECT id, * FROM offers", conn)
                else:
                    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                
                file_path = os.path.join(export_dir, f"{table_name}_{timestamp}.csv")
                df.to_csv(file_path, index=False)
                print(f"Exported {table_name} to {file_path}")
                
            except Exception as e:
                print(f"Skipping {table_name} due to error: {str(e)}")
        
        # Add explicit handling for contracted tables
        contracted_tables = [
        'analyzed_offers_contracted',
        'analyzed_offers_enhanced_contracted',
        'Contracted_fuel_prices'
        ]
    
        for table in contracted_tables:
            try:
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                file_path = os.path.join(export_dir, f"{table}_{timestamp}.csv")
                df.to_csv(file_path, index=False)
                print(f"Exported {table} to {file_path}")
            except Exception as e:
                print(f"Skipping {table} due to error: {str(e)}")
                
    except Exception as e:
        print(f"Error during export: {str(e)}")

    finally:
        if own_connection:
            conn.close()


def export_invalid_data_to_csv(invalid_rows):
    """Exports invalid data to CSV files with a timestamp."""
    export_dir = "/Users/lyubomiriliev/airport_project/tender 2025/export"
    os.makedirs(export_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for table_name, rows in invalid_rows.items():
        try:
            df = pd.DataFrame(rows)
            file_path = os.path.join(export_dir, f"{table_name}_invalid_{timestamp}.csv")
            df.to_csv(file_path, index=False)
            print(f"Invalid data for table '{table_name}' exported to '{file_path}'")
        except Exception as e:
            print(f"Error exporting invalid data for table '{table_name}': {e}")

def update_mp_price_to_usdc_gal(conn):
    """Update the price_to_usdc_gal column in the mp table."""
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE mp
            SET price_to_usdc_gal = (
                value * (
                    SELECT to_USDC 
                    FROM currency 
                    WHERE currency.currency = mp.currency
                ) / (
                    SELECT metrick_in_usg 
                    FROM metrics 
                    WHERE metrics.metric_descr = mp.per_volume
                )
            )
        ''')
        conn.commit()
        print("price_to_usdc_gal column updated successfully")
    except Exception as e:
        print(f"Error updating price_to_usdc_gal: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

class TableEntryForm:
    # Class-level default values (ADD THIS AT THE TOP OF THE CLASS)
    DEFAULT_VALUES = {
        'for_currency': 'USD',
    'per_volume': 'gallion.USG.UGL',
    'SAF_platts_name': 'nqma SAF',
    'SAF_currency': 'USD',
    'SAF_per_volume': 'gallion.USG.UGL',
    'SAF_diff': '0',
    'SAF_included_as_percent': '0',
    'SAF_period': 'MP',
    'value_fee': '0',
    'confirmed': 'Tender' ,
    'for_currency_1': 'USD',
    'per_volume_1': 'gallion.USG.UGL',
    'value_fee_1': '0',
    'for_currency_2': 'USD',
    'per_volume_2': 'gallion.USG.UGL',
    'value_fee_2': '0',
    'for_currency_3': 'USD',
    'per_volume_3': 'gallion.USG.UGL',
    'value_fee_3': '0',
    'for_currency_4': 'USD',
    'per_volume_4': 'gallion.USG.UGL',
    'value_fee_4': '0',
    'for_currency_5': 'USD',
    'per_volume_5': 'gallion.USG.UGL',
    'value_fee_5': '0',
    'for_currency_6': 'USD',
    'per_volume_6': 'gallion.USG.UGL',
    'value_fee_6': '0',
    'for_currency_7': 'USD',
    'per_volume_7': 'gallion.USG.UGL',
    'value_fee_7': '0'
}
    

    def __init__(self, master, table_name, table_schema, conn, platts_values=None, period_values=None, currency_values=None, metrics_values=None, iata_values=None, saf_platts_values=None, saf_period_values=None, confirmed_values=None, supplier_values=None, platts_name_values=None):
        self.master = master
        self.table_name = table_name
        self.table_schema = table_schema  # List of tuples: (column_name, data_type)
        self.conn = conn
        self.cursor = conn.cursor()
        self.entries = {}
        self.form_vars = {}
        self.platts_values = platts_values
        self.period_values = period_values
        self.currency_values = currency_values
        self.metrics_values = metrics_values
        self.iata_values = iata_values
        self.saf_platts_values = saf_platts_values
        self.saf_period_values = saf_period_values
        self.confirmed_values = confirmed_values
        self.supplier_values = supplier_values
        self.platts_name_values = platts_name_values

        # Update platts_values if the table is 'platts'
        if table_name == 'platts':
            self.platts_values = get_unique_values(conn, 'platts', 'platts')

        self.create_widgets()

    def create_widgets(self):
        self.canvas = tk.Canvas(self.master)
        self.scrollbar = tk.Scrollbar(self.master, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = tk.Frame(self.canvas)

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(
                scrollregion=self.canvas.bbox("all")
            )
        )

        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        self.label = tk.Label(self.scrollable_frame, text=f"{self.table_name.capitalize()} Entry Form", font=("Arial", 14))
        self.label.grid(row=0, column=0, columnspan=2, pady=10)

        row_num = 1
        for col_name, data_type in self.table_schema:
            label = tk.Label(self.scrollable_frame, text=f"{col_name.replace('_', ' ').title()}:")
            label.grid(row=row_num, column=0, sticky=tk.E, padx=5, pady=5)

            if col_name in ["valid_since", "valid_until", "start", "end", "date"]:
                self.form_vars[col_name] = tk.StringVar()
                entry = DateEntry(self.scrollable_frame, date_pattern='yyyy-mm-dd', textvariable=self.form_vars[col_name], width=12)
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
                entry.bind("<<DateEntrySelected>>", lambda e: entry.grab_release())
            elif col_name == "confirmed":
                self.form_vars[col_name] = tk.StringVar()
                entry = ttk.Combobox(self.scrollable_frame, textvariable=self.form_vars[col_name], values=self.confirmed_values)
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            elif col_name == "supplier":
                self.form_vars[col_name] = tk.StringVar()
                entry = ttk.Combobox(self.scrollable_frame, textvariable=self.form_vars[col_name], values=self.supplier_values)
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            elif col_name == "platts":
                self.form_vars[col_name] = tk.StringVar()
                entry = ttk.Combobox(self.scrollable_frame, textvariable=self.form_vars[col_name], values=self.platts_values)
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            elif col_name == "period":
                self.form_vars[col_name] = tk.StringVar()
                entry = ttk.Combobox(self.scrollable_frame, textvariable=self.form_vars[col_name], values=self.period_values)
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            elif col_name == "SAF_platts_name":
                self.form_vars[col_name] = tk.StringVar()
                entry = ttk.Combobox(self.scrollable_frame, textvariable=self.form_vars[col_name], values=self.saf_platts_values)
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            elif col_name == "SAF_period":
                self.form_vars[col_name] = tk.StringVar()
                entry = ttk.Combobox(self.scrollable_frame, textvariable=self.form_vars[col_name], values=self.saf_period_values)
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            elif col_name == "currency":  # Add this condition
                self.form_vars[col_name] = tk.StringVar()
                entry = ttk.Combobox(self.scrollable_frame, textvariable=self.form_vars[col_name], values=self.currency_values)
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            elif col_name.startswith("for_currency"):
                self.form_vars[col_name] = tk.StringVar(value="USD")
                entry = ttk.Combobox(self.scrollable_frame, textvariable=self.form_vars[col_name], values=self.currency_values)
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            elif col_name.startswith("per_volume"):
                self.form_vars[col_name] = tk.StringVar(value="gallion.USG.UGL")
                entry = ttk.Combobox(self.scrollable_frame, textvariable=self.form_vars[col_name], values=self.metrics_values)
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            elif col_name == "iata":
                self.form_vars[col_name] = tk.StringVar()
                entry = ttk.Combobox(self.scrollable_frame, textvariable=self.form_vars[col_name], values=self.iata_values)
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            elif col_name == "SAF_diff":
                self.form_vars[col_name] = tk.StringVar(value="0")
                entry = tk.Entry(self.scrollable_frame, textvariable=self.form_vars[col_name])
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            elif col_name == "SAF_currency":
                self.form_vars[col_name] = tk.StringVar(value="USD")
                entry = ttk.Combobox(self.scrollable_frame, textvariable=self.form_vars[col_name], values=self.currency_values)
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            elif col_name == "SAF_per_volume":
                self.form_vars[col_name] = tk.StringVar(value="gallion.USG.UGL")
                entry = ttk.Combobox(self.scrollable_frame, textvariable=self.form_vars[col_name], values=self.metrics_values)
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            elif col_name == "SAF_included_as_percent":
                self.form_vars[col_name] = tk.StringVar(value="0")
                entry = tk.Entry(self.scrollable_frame, textvariable=self.form_vars[col_name])
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            elif col_name.startswith("value"):
                self.form_vars[col_name] = tk.StringVar(value="0")
                entry = tk.Entry(self.scrollable_frame, textvariable=self.form_vars[col_name])
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            else:
                self.form_vars[col_name] = tk.StringVar()
                entry = tk.Entry(self.scrollable_frame, textvariable=self.form_vars[col_name])
                entry.grid(row=row_num, column=1, sticky=tk.W, padx=5, pady=5)
            self.entries[col_name] = entry
            row_num += 1

        self.scrollable_frame.update_idletasks()  # Force UI update
        self.canvas.config(scrollregion=self.canvas.bbox("all"))  # Update scroll area

        self.scrollable_frame.grid_rowconfigure(row_num, weight=1)
        self.scrollable_frame.grid_columnconfigure(0, weight=1)
        self.scrollable_frame.grid_columnconfigure(1, weight=1)

        self.button_save = tk.Button(self.scrollable_frame, text="Save Entry", command=self.save_entry)
        self.button_save.grid(row=row_num, column=0, columnspan=2, pady=10)

        # Add reset button (optional)
        self.btn_reset = tk.Button(self.scrollable_frame, text="Reset Form", command=self.reset_form)
        self.btn_reset.grid(row=row_num + 1, column=0, columnspan=2, pady=10)

    def reset_form(self):
        """Reset all fields to defaults"""
        today = datetime.today().strftime('%Y-%m-%d')
        for col_name, var in self.form_vars.items():
            # Handle special fields
            if col_name in ["valid_since", "valid_until", "start", "end"]:
                var.set(today)
                self.entries[col_name].set_date(datetime.today())
                continue
                
            # Set default values using base name matching
            base_name = col_name.split('_')[0]  # Handle for_currency_1, etc.
            if base_name in self.DEFAULT_VALUES:
                var.set(self.DEFAULT_VALUES[base_name])
            else:
                var.set('')

            # Reset comboboxes
            entry = self.entries[col_name]
            if isinstance(entry, ttk.Combobox):
                entry.set(self.DEFAULT_VALUES.get(col_name, ''))

    def save_entry(self):
        try:
            # Collect data from form
            entry_data = {}
            for col_name, data_type in self.table_schema:
                entry_data[col_name] = self.form_vars[col_name].get()

            # Construct SQL query
            columns = ', '.join(entry_data.keys())
            placeholders = ', '.join(['?'] * len(entry_data))
            sql = "INSERT INTO {} ({}) VALUES ({})".format(self.table_name, columns, placeholders)

            # Open a new connection to the database
            with sqlite3.connect("tender_offers.db") as conn:
                cursor = conn.cursor()
                # Execute query
                values = tuple(entry_data.values())
                cursor.execute(sql, values)
                conn.commit()

                # Refresh calculations
                if self.table_name == 'offers':
                    duplicate_and_replace_offers(conn)
                    update_offers_numeric(conn)
                    create_analyzed_offers_table(conn)  # Ensure this line is present
                    create_enhanced_analyzed_table(conn)  # Add this line
                    create_min_prices_table(conn)  # Add this line
                    update_mp_value_per_usdc(conn)
                    create_contracted_offers_table(conn)
                    create_enhanced_contracted_table(conn)
                    create_contracted_fuel_prices_table(conn)  # Updated to use conn
                    export_all_tables_to_csv(conn)

                # Refresh dropdown values for platts table
                if self.table_name == 'platts':
                    self.platts_values = get_unique_values(conn, 'platts', 'platts')
                    for col_name, entry in self.entries.items():
                        if col_name == 'platts':
                            entry['values'] = self.platts_values

                # Update all calculations
                refresh_all_calculations(conn)

            # Clear the form after saving
            self.reset_form()  # Add this line

            # Optional: Show confirmation
            messagebox.showinfo("Success", "Entry saved and form reset!")

        except (sqlite3.Error, ValueError) as e:
            messagebox.showerror("Error", f"Error saving entry: {str(e)}")

class MainApp:
    def __init__(self, main_window):
        self.root = main_window
        self.root.title("Tender Offers Data Manager")
        self.root.geometry("400x300")

        # Create database and tables
        with sqlite3.connect("tender_offers.db") as conn:
            create_tables(conn)

        self.table_schemas = {
            "offers": [
                ("supl_iata", "TEXT"), ("valid_since", "DATE"), ("valid_until", "DATE"), ("confirmed", "TEXT"),
                ("supplier", "TEXT"), ("iata", "TEXT"), ("platts", "TEXT"), ("period", "TEXT"),
                ("diff", "REAL"), ("for_currency", "TEXT"), ("per_volume", "TEXT"), ("value_fee", "REAL"),
                ("for_currency_1", "TEXT"), ("per_volume_1", "TEXT"), ("value_fee_1", "REAL"),
                ("for_currency_2", "TEXT"), ("per_volume_2", "TEXT"), ("value_fee_2", "REAL"),
                ("for_currency_3", "TEXT"), ("per_volume_3", "TEXT"), ("value_fee_3", "REAL"),
                ("for_currency_4", "TEXT"), ("per_volume_4", "TEXT"), ("value_fee_4", "REAL"),
                ("for_currency_5", "TEXT"), ("per_volume_5", "TEXT"), ("value_fee_5", "REAL"),
                ("for_currency_6", "TEXT"), ("per_volume_6", "TEXT"), ("value_fee_6", "REAL"),
                ("for_currency_7", "TEXT"), ("per_volume_7", "TEXT"), ("value_fee_7", "REAL"),
                ("SAF_platts_name", "TEXT"), ("SAF_period", "TEXT"), ("SAF_diff", "REAL"), 
                ("SAF_currency", "TEXT"), ("SAF_per_volume", "TEXT"), ("SAF_included_as_percent", "REAL")
            ],
            "platts": [
                ("supl_iata", "TEXT"), ("start", "DATE"), ("end", "DATE"), ("platts", "TEXT"),
                ("value", "REAL"), ("period", "TEXT")
            ],
            "mp": [
                ("supl_iata", "TEXT"), ("start", "DATE"), ("end", "DATE"), ("supplier", "TEXT"),
                ("iata", "TEXT"), ("value", "REAL"), ("currency", "TEXT"), ("per_volume", "TEXT")
            ],
            "currency": [
                ("date", "TEXT"), ("currency", "TEXT"), ("currency_name", "TEXT"), ("value", "REAL")
            ],
            "metrics": [
                ("metric_descr", "TEXT"), ("metrick_in_usg", "REAL")
            ],
            "airports": [
                ("iata", "TEXT"), ("icao", "TEXT"), ("airport", "TEXT"), ("city", "TEXT"), ("state", "TEXT"),
                ("est_flight_ttl", "REAL"), ("est_volume_mt", "REAL")
            ]
        }
        
        self.table_schemas.update({
            "analyzed_offers_contracted": [("id", "INTEGER")],  # Add actual schema
            "analyzed_offers_enhanced_contracted": [("id", "INTEGER")],
            "Contracted_fuel_prices": [("id", "INTEGER")]
        })

        self.entry_forms = {}  # Store references to the created entry forms

        self.create_widgets()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)  # Handle window close event

    

    

    def create_widgets(self):
        # Load Data button
        self.load_data_button = tk.Button(self.root, text="Load Data", command=self.load_data, height=2, width=20)
        self.load_data_button.pack(pady=10)

        # Entry Form buttons
        self.offers_button = tk.Button(self.root, text="Offers Entry Form", command=lambda: self.open_entry_form("offers"), height=2, width=20)
        self.offers_button.pack(pady=5)

        self.platts_button = tk.Button(self.root, text="Platts Entry Form", command=lambda: self.open_entry_form("platts"), height=2, width=20)
        self.platts_button.pack(pady=5)

        self.mp_button = tk.Button(self.root, text="MP Entry Form", command=lambda: self.open_entry_form("mp"), height=2, width=20)
        self.mp_button.pack(pady=5)

        self.currency_button = tk.Button(self.root, text="Currency Entry Form", command=lambda: self.open_entry_form("currency"), height=2, width=20)
        self.currency_button.pack(pady=5)

        self.metrics_button = tk.Button(self.root, text="Metrics Entry Form", command=lambda: self.open_entry_form("metrics"), height=2, width=20)
        self.metrics_button.pack(pady=5)

        self.airports_button = tk.Button(self.root, text="Airports Entry Form", command=lambda: self.open_entry_form("airports"), height=2, width=20)
        self.airports_button.pack(pady=5)

        # View Data button
        self.view_data_button = tk.Button(self.root, text="View Data", command=self.view_data, height=2, width=20)
        self.view_data_button.pack(pady=10)

        self.feedback_button = tk.Button(
            self.root,
            text="Generate Supplier Feedback",
            command=self.generate_supplier_feedback,
            height=2,
            width=25
        )
        self.feedback_button.pack(pady=10)

        # Exit button
        self.exit_button = tk.Button(self.root, text="Exit", command=self.on_close, height=2, width=20)
        self.exit_button.pack(pady=10)

        

    def on_close(self):
        """Handles closing with analyzed_offers safeguard"""
        with sqlite3.connect("tender_offers.db") as conn:
            try:
                # Validate data types and handle invalid data
                invalid_rows = validate_data_types(conn, self.table_schemas)
                if invalid_rows:
                    self.show_invalid_data(invalid_rows)
                    return

                # Ask for confirmation before quitting
                if messagebox.askokcancel("Quit", "Do you want to quit?"):
                    export_all_tables_to_csv(conn)  # Export tables to CSV before closing
                    self.root.destroy()  # Close the application

            except Exception as e:
                print(f"Error during on_close: {str(e)}")
            finally:
                # Ensure this is executed regardless of success or failure
                try:
                    export_all_tables_to_csv(conn)  # Ensure this is executed on exit
                    self.root.destroy()
                except tk.TclError:
                    pass  # Ignore the error if the application is already destroyed

    def show_invalid_data(self, invalid_rows):
        """Display invalid data in a new window."""
        invalid_data_window = tk.Toplevel(self.root)
        invalid_data_window.title("Invalid Data")
        invalid_data_window.geometry("800x800")

        for table_name, rows in invalid_rows.items():
            frame = tk.Frame(invalid_data_window)
            frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

            label = tk.Label(frame, text=f"Invalid data in table '{table_name}':", font=("Arial", 14))
            label.pack(pady=10)

            text = tk.Text(frame)
            text.pack(fill=tk.BOTH, expand=True)

            scrollbar = tk.Scrollbar(text)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

            text.config(yscrollcommand=scrollbar.set)
            scrollbar.config(command=text.yview)

            for row in rows:
                row_str = ', '.join([str(value) for value in row])
                text.insert(tk.END, row_str + '\n')
                text.tag_add("invalid", "1.0", "end")
                text.tag_config("invalid", background="yellow")
   # Add a button to confirm exit and start export
        confirm_button = tk.Button(invalid_data_window, text="Confirm and Export", command=lambda: self.confirm_and_export(invalid_rows))
        confirm_button.pack(pady=10)

    def confirm_and_export(self, invalid_rows):
        """Confirm exit and export all tables and invalid data to CSV files."""
        with sqlite3.connect("tender_offers.db") as conn:
            export_all_tables_to_csv(conn)
            export_invalid_data_to_csv(invalid_rows)
        self.root.destroy()

      
    def load_data(self):
        """Load data from CSV files into the database."""
        # Create database connection
        conn = sqlite3.connect("tender_offers.db")

        # Clear all tables before loading new data
        clear_all_tables(conn)

        # Get file paths
        offer_file = filedialog.askopenfilename(title="Select Offers CSV file", filetypes=[("CSV files", "*.csv")])
        platts_file = filedialog.askopenfilename(title="Select Platts CSV file", filetypes=[("CSV files", "*.csv")])
        mp_file = filedialog.askopenfilename(title="Select MP CSV file", filetypes=[("CSV files", "*.csv")])
        currency_file = filedialog.askopenfilename(title="Select Currency CSV file", filetypes=[("CSV files", "*.csv")])
        metrics_file = filedialog.askopenfilename(title="Select Metrics CSV file", filetypes=[("CSV files", "*.csv")])
        airports_file = filedialog.askopenfilename(title="Select Airports CSV file", filetypes=[("CSV files", "*.csv")])

        if not all([offer_file, platts_file, mp_file, currency_file, metrics_file, airports_file]):
            messagebox.showerror("Error", "All files must be selected")
            return

        try:
            # For offer file
            offer_data = pd.read_csv(offer_file, sep=',', encoding='utf-8')
            offer_data = clean_column_names(offer_data)
            # In load_data function, add format to pd.to_datetime calls:
            offer_data['valid_since'] = pd.to_datetime(offer_data['valid_since'], errors='coerce', dayfirst=False).dt.strftime('%Y-%m-%d')
            offer_data['valid_until'] = pd.to_datetime(offer_data['valid_until'], errors='coerce', dayfirst=False).dt.strftime('%Y-%m-%d')

            # For platts file
            platts_data = pd.read_csv(platts_file, sep=',', encoding='utf-8')
            platts_data = clean_column_names(platts_data)
            platts_data['start'] = pd.to_datetime(platts_data['start'], errors='coerce', dayfirst=False).dt.strftime('%Y-%m-%d')
            platts_data['end'] = pd.to_datetime(platts_data['end'], errors='coerce', dayfirst=False).dt.strftime('%Y-%m-%d')

            # For MP file
            mp_data = pd.read_csv(mp_file, sep=',', encoding='utf-8')
            mp_data = clean_column_names(mp_data)
            mp_data['start'] = pd.to_datetime(mp_data['start'], errors='coerce', dayfirst=False).dt.strftime('%Y-%m-%d')
            mp_data['end'] = pd.to_datetime(mp_data['end'], errors='coerce', dayfirst=False).dt.strftime('%Y-%m-%d')

            # For currency file
            currency_data = pd.read_csv(currency_file, sep=',', encoding='utf-8')
            currency_data = clean_column_names(currency_data)

            # For metrics file
            metrics_data = pd.read_csv(metrics_file, sep=',', encoding='utf-8')
            metrics_data = clean_column_names(metrics_data)

            # For airports file
            airports_data = pd.read_csv(airports_file, sep=',', encoding='utf-8')
            airports_data = clean_column_names(airports_data)

            # Add column validation
            required_columns = ['iata', 'est_flight_ttl', 'est_volume_mt']
            if not all(col in airports_data.columns for col in required_columns):
                raise ValueError("Airports CSV missing required columns")

            # Convert numeric columns
            airports_data['est_flight_ttl'] = pd.to_numeric(airports_data['est_flight_ttl'], errors='coerce')
            airports_data['est_volume_mt'] = pd.to_numeric(airports_data['est_volume_mt'], errors='coerce')

            # Insert data into tables
            try:
                offer_data.to_sql("offers", conn, if_exists="append", index=False)
            except pd.io.sql.DatabaseError as e:
                print(f"Error inserting offer data: {e}")
                offer_data.to_sql("offers", conn, if_exists="replace", index=False)

            try:
                platts_data.to_sql("platts", conn, if_exists="append", index=False)
            except:
                platts_data.to_sql("platts", conn, if_exists="replace", index=False)

            try:
                mp_data.to_sql("mp", conn, if_exists="append", index=False)
            except:
                mp_data.to_sql("mp", conn, if_exists="replace", index=False)

            try:
                currency_data.to_sql("currency", conn, if_exists="append", index=False)
            except:
                currency_data.to_sql("currency", conn, if_exists="replace", index=False)

            try:
                metrics_data.to_sql("metrics", conn, if_exists="append", index=False)
            except:
                metrics_data.to_sql("metrics", conn, if_exists="replace", index=False)

            try:
                airports_data.to_sql("airports", conn, if_exists="append", index=False)
            except:
                airports_data.to_sql("airports", conn, if_exists="replace", index=False)

            divide_to_USDC_by_100(conn)
            add_additional_currency_rows(conn)
            update_mp_value_per_usdc(conn)  # Ensure this is executed right after importing mp data
            add_missing_column(conn)
            duplicate_and_replace_offers(conn)
            update_offers_numeric(conn)
            create_analyzed_offers_table(conn)
            create_enhanced_analyzed_table(conn)
            create_min_prices_table(conn)  # Add this line
            refresh_all_calculations(conn)
            refresh_missing_platts_table(conn)
            refresh_missing_mp_table(conn)
            create_contracted_offers_table(conn)
            create_enhanced_contracted_table(conn)
            create_contracted_fuel_prices_table(conn)  # Ensure this line is present

            supplier_values = get_unique_values(conn, "offers", "supplier")
            confirmed_values = get_unique_values(conn, "offers", "confirmed")
            
            messagebox.showinfo("Success", "Data loaded successfully!")
            return supplier_values, confirmed_values
        
        except (pd.errors.ParserError, pd.errors.EmptyDataError, ValueError, sqlite3.Error) as e:
            messagebox.showerror("Error", f"An error occurred: {str(e)}")

        finally:
            conn.close()

    

    def open_entry_form(self, table_name):
        """Opens the entry form for the specified table."""

        # Fetch values for dropdown lists from the database
        with sqlite3.connect("tender_offers.db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT platts FROM platts")
            platts_values = [row[0] for row in cursor.fetchall()]

            cursor.execute("SELECT DISTINCT period FROM platts")
            period_values = [row[0] for row in cursor.fetchall()]

            cursor.execute("SELECT DISTINCT currency FROM currency")
            currency_values = [row[0] for row in cursor.fetchall()]

            cursor.execute("SELECT DISTINCT metric_descr FROM metrics")
            metrics_values = [row[0] for row in cursor.fetchall()]

            cursor.execute("SELECT DISTINCT iata FROM airports")
            iata_values = [row[0] for row in cursor.fetchall()]

            cursor.execute("SELECT DISTINCT platts FROM platts")
            saf_platts_values = [row[0] for row in cursor.fetchall()]

            cursor.execute("SELECT DISTINCT period FROM platts")
            saf_period_values = [row[0] for row in cursor.fetchall()]

            cursor.execute("SELECT DISTINCT confirmed FROM offers")
            confirmed_values = [row[0] for row in cursor.fetchall()]
            # print(f"Confirmed values: {confirmed_values}")

            cursor.execute("SELECT DISTINCT supplier FROM offers")
            supplier_values = [row[0] for row in cursor.fetchall()]
            # print(f"Supplier values: {supplier_values}")

            # Fetch unique values for platts_name
            cursor.execute("SELECT DISTINCT platts FROM platts")
            platts_name_values = [row[0] for row in cursor.fetchall()]

        # Filter out None values
        confirmed_values = [value for value in confirmed_values if value is not None]
        supplier_values = [value for value in supplier_values if value is not None]
        saf_platts_values = [value for value in saf_platts_values if value is not None]
        saf_period_values = [value for value in saf_period_values if value is not None]
        platts_name_values = [value for value in platts_name_values if value is not None]


        # Create a new Toplevel window for each table
        new_window = tk.Toplevel(self.root)
        new_window.title(f"{table_name.capitalize()} Entry Form")

        # Get the table schema
        table_schema = self.table_schemas[table_name]

        # Create an instance of TableEntryForm for each table
        with sqlite3.connect("tender_offers.db") as conn:
            entry_form = TableEntryForm(new_window, table_name, table_schema, conn, 
                            platts_values, period_values, currency_values, 
                            metrics_values, iata_values, 
                            saf_platts_values, saf_period_values, confirmed_values, supplier_values)
            self.entry_forms[table_name] = entry_form  # Store the entry form instance

    

    def view_data(self):
        """Open the view data in a new window."""
        ViewDataWindow(self.root)

    def generate_supplier_feedback(self):
        """Generates supplier-specific feedback CSVs."""
        export_dir = filedialog.askdirectory(title="Select Export Directory")
        if not export_dir:
            return

        try:
            conn = sqlite3.connect("tender_offers.db")
            generate_supplier_feedback(conn, export_dir)
        
        except Exception as e:
            messagebox.showerror("Error", f"Failed to generate feedback:\n{str(e)}")
        
        finally:
            conn.close()
    

    # Correct the indentation of the on_close method
def on_close(self):
    """Handles closing of the main window."""
    with sqlite3.connect("tender_offers.db") as conn:
        try:
            # Validate data types and handle invalid data
            invalid_rows = validate_data_types(conn, self.table_schemas)
            if invalid_rows:
                self.show_invalid_data(invalid_rows)
                return

            # Ask for confirmation before quitting
            if messagebox.askokcancel("Quit", "Do you want to quit?"):
                export_all_tables_to_csv(conn)  # Export tables to CSV before closing
                self.root.destroy()  # Close the application

        except Exception as e:
            print(f"Error during on_close: {str(e)}")
            self.root.destroy()  # Close the application even if there's an error



# --- Class for viewing data ---
from tkinter import ttk
class ViewDataWindow:
    def __init__(self, master):
        self.master = master
        self.view_window = tk.Toplevel(self.master)
        self.view_window.title("View Data")
        self.view_window.geometry("1200x600")
        self.conn = sqlite3.connect("tender_offers.db")
        self.table_var = tk.StringVar()
        self.tables = self.get_table_names()
        self.sort_column = None
        self.sort_order = "DESC"
        self.style = ttk.Style()
        self.configure_styles()
        self.create_widgets()
        self.current_table = None
        
    def configure_styles(self):
        self.style.theme_use('clam')
        self.style.configure("Treeview", 
                           font=('Arial', 10), 
                           rowheight=25,
                           borderwidth=1,
                           relief='solid')
        self.style.configure("Treeview.Heading", 
                           font=('Arial', 12, 'bold'), 
                           background='#E0E0E0',
                           relief='raised',
                           borderwidth=2)
        self.style.map("Treeview.Heading",
                     relief=[('active', 'groove'), ('pressed', 'sunken')])

    def get_table_names(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [table[0] for table in cursor.fetchall()]

    def create_widgets(self):
        # Control frame for table selection
        control_frame = tk.Frame(self.view_window)
        control_frame.pack(pady=10, fill=tk.X)
        
        tk.Label(control_frame, text="Select Table:", font=('Arial', 12)).pack(side=tk.LEFT)
        self.table_var.set(self.tables[0] if self.tables else '')
        table_menu = tk.OptionMenu(control_frame, self.table_var, *self.tables, command=self.show_table_data)
        table_menu.pack(side=tk.LEFT, padx=10)

        # Container frame for treeview and scrollbars
        tree_container = tk.Frame(self.view_window)
        tree_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Horizontal scrollbar
        h_scroll = ttk.Scrollbar(tree_container, orient=tk.HORIZONTAL)
        h_scroll.pack(side=tk.BOTTOM, fill=tk.X)

        # Vertical scrollbar
        v_scroll = ttk.Scrollbar(tree_container, orient=tk.VERTICAL)
        v_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Treeview widget
        self.tree = ttk.Treeview(
            tree_container,
            xscrollcommand=h_scroll.set,
            yscrollcommand=v_scroll.set,
            selectmode='extended'
        )
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Configure scrollbars
        h_scroll.config(command=self.tree.xview)
        v_scroll.config(command=self.tree.yview)

        # Initial data load
        if self.tables:
            self.show_table_data(self.tables[0])

    def show_table_data(self, selected_table):
    # Clear previous data
        self.tree.delete(*self.tree.get_children())
        self.tree["columns"] = []
        self.current_table = selected_table    
    
    # Get table structure
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({selected_table})")
        columns_info = cursor.fetchall()    
    
    # Configure columns
        columns = [col[1] for col in columns_info]
        self.tree["columns"] = columns    
        self.tree.column("#0", width=0, stretch=tk.NO)

        for col in columns_info:
            self.tree.column(col[1], width=200, anchor=tk.W, stretch=False)
            self.tree.heading(
                col[1], 
                text=col[1], 
                anchor=tk.W,
                command=lambda c=col[1]: self.sort_by_column(c, selected_table)  # FIXED
        )

        cursor.execute(f"SELECT * FROM {selected_table}")
        rows = cursor.fetchall()    
        for row in rows:
            self.tree.insert("", tk.END, values=row)        

    # Update scroll region
        self.tree.update_idletasks()
        self.tree.config(scrollregion=self.tree.bbox("all"))
                
    # Change method name from sort_column to sort_by_column
    def sort_by_column(self, column, table_name):
        """Sort treeview by column"""
        # Toggle sort order
        if self.sort_column == column:  # This refers to the instance variable
            self.sort_order = "ASC" if self.sort_order == "DESC" else "DESC"
        else:
            self.sort_order = "DESC"
            self.sort_column = column  # This updates the instance variable

        # Clear current data
        self.tree.delete(*self.tree.get_children())
        
        # Fetch sorted data
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} ORDER BY {column} {self.sort_order}")
        rows = cursor.fetchall()
        
        # Reinsert sorted data
        for row in rows:
            self.tree.insert("", tk.END, values=row)

        # Update headers
        for col in self.tree["columns"]:
            self.tree.heading(col, text=col)
        self.tree.heading(
            col[1], 
            text=col[1], 
            anchor=tk.W,
            command=lambda c=col[1], t=self.current_table: self.sort_by_column(c, t)
        )

        # Update scroll region
        self.tree.update_idletasks()
        self.tree.config(scrollregion=self.tree.bbox("all"))

def create_analyzed_offers_table(conn):
    cursor = conn.cursor()
    try:
        # Debug: Check if offers_numeric exists
        cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='offers_numeric'")
        if cursor.fetchone()[0] == 0:
            print("Error: offers_numeric table does not exist!")
            return

        # Debug: Check offers_numeric data
        cursor.execute("SELECT count(*) FROM offers_numeric WHERE COALESCE(confirmed, 'Tender') = 'Tender'")
        count = cursor.fetchone()[0]
        print(f"Found {count} confirmed offers in offers_numeric")

        cursor.execute('DROP TABLE IF EXISTS analyzed_offers')

        cursor.execute('''
    CREATE TABLE analyzed_offers AS
WITH current_offers AS (
    SELECT *,
        CASE period
            WHEN 'PM' THEN 1
            WHEN '1/2M' THEN 2
            WHEN 'PW' THEN 3
            ELSE 4
        END AS period_rank
    FROM offers_numeric
    WHERE DATE('now') BETWEEN 
        COALESCE(valid_since, '2000-01-01') AND 
        COALESCE(valid_until, '3000-12-31')
),
ranked AS (
    SELECT 
        *,
        RANK() OVER (PARTITION BY iata ORDER BY period_rank) AS rank1,
        RANK() OVER (
            PARTITION BY iata, period_rank 
            ORDER BY 
                CASE 
                    WHEN SAF_platts_name IN ('0', 'nqma SAF') THEN
                        COALESCE(diff * for_currency / NULLIF(per_volume, 0), 0) + 
                        COALESCE(SAF_diff * SAF_currency / NULLIF(SAF_per_volume, 0), 0)
                    ELSE
                        COALESCE(SAF_diff * SAF_currency / NULLIF(SAF_per_volume, 0) * SAF_included_as_percent, 0) + 
                        (1 - COALESCE(SAF_included_as_percent, 0)) * COALESCE(diff * for_currency / NULLIF(per_volume, 0), 0)
                END + 
                COALESCE(value_fee * for_currency_1 / NULLIF(per_volume_1, 0), 0) + 
                COALESCE(value_fee_1 * for_currency_2 / NULLIF(per_volume_2, 0), 0) + 
                COALESCE(value_fee_2 * for_currency_3 / NULLIF(per_volume_3, 0), 0) + 
                COALESCE(value_fee_3 * for_currency_4 / NULLIF(per_volume_4, 0), 0) + 
                COALESCE(value_fee_4 * for_currency_5 / NULLIF(per_volume_5, 0), 0) + 
                COALESCE(value_fee_5 * for_currency_6 / NULLIF(per_volume_6, 0), 0) + 
                COALESCE(value_fee_6 * for_currency_7 / NULLIF(per_volume_7, 0), 0),
                CASE WHEN SAF_diff > 0 THEN 0 ELSE 1 END
        ) AS rank2
    FROM current_offers
)
SELECT 
    id,
    valid_since,
    valid_until,
    confirmed,
    supplier,
    iata,
    period,
        -- Base calculations
        COALESCE(diff * for_currency / NULLIF(per_volume, 0), 0) AS diff_calc,
        -- Individual fee calculations
        COALESCE(value_fee * for_currency_1 / NULLIF(per_volume_1, 0), 0) AS value_fee_calc,
        COALESCE(value_fee_1 * for_currency_2 / NULLIF(per_volume_2, 0), 0) AS value_fee_calc_1,
        COALESCE(value_fee_2 * for_currency_3 / NULLIF(per_volume_3, 0), 0) AS value_fee_calc_2,
        COALESCE(value_fee_3 * for_currency_4 / NULLIF(per_volume_4, 0), 0) AS value_fee_calc_3,
        COALESCE(value_fee_4 * for_currency_5 / NULLIF(per_volume_5, 0), 0) AS value_fee_calc_4,
        COALESCE(value_fee_5 * for_currency_6 / NULLIF(per_volume_6, 0), 0) AS value_fee_calc_5,
        COALESCE(value_fee_6 * for_currency_7 / NULLIF(per_volume_7, 0), 0) AS value_fee_calc_6,
        -- SAF calculation
        CASE 
            WHEN SAF_platts_name IN ('0', 'nqma SAF') THEN
                COALESCE(SAF_diff * SAF_currency / NULLIF(SAF_per_volume, 0), 0)
            ELSE
                COALESCE(SAF_diff * SAF_currency / NULLIF(SAF_per_volume, 0) * SAF_included_as_percent, 0)
        END AS SAF_diff_calc,
        -- Total fees
        (
            COALESCE(value_fee * for_currency_1 / NULLIF(per_volume_1, 0), 0) + 
            COALESCE(value_fee_1 * for_currency_2 / NULLIF(per_volume_2, 0), 0) + 
            COALESCE(value_fee_2 * for_currency_3 / NULLIF(per_volume_3, 0), 0) + 
            COALESCE(value_fee_3 * for_currency_4 / NULLIF(per_volume_4, 0), 0) + 
            COALESCE(value_fee_4 * for_currency_5 / NULLIF(per_volume_5, 0), 0) + 
            COALESCE(value_fee_5 * for_currency_6 / NULLIF(per_volume_6, 0), 0) + 
            COALESCE(value_fee_6 * for_currency_7 / NULLIF(per_volume_7, 0), 0)
        ) AS total_fees_calc,
        -- Differential total
        CASE 
            WHEN SAF_platts_name IN ('0', 'nqma SAF') THEN
                COALESCE(diff * for_currency / NULLIF(per_volume, 0), 0) + 
                COALESCE(SAF_diff * SAF_currency / NULLIF(SAF_per_volume, 0), 0)
            ELSE
                COALESCE(SAF_diff * SAF_currency / NULLIF(SAF_per_volume, 0) * SAF_included_as_percent, 0) + 
                (1 - COALESCE(SAF_included_as_percent, 0)) * COALESCE(diff * for_currency / NULLIF(per_volume, 0), 0)
        END AS diff_total,
        -- Rankings
        rank1,
        rank2,
        (rank1 + (rank2 / 10.0)) AS rank_final
    FROM ranked
    WHERE COALESCE(confirmed, 'Tender') = 'Tender'
''')
        conn.commit()
        print("analyzed_offers table created successfully")
    except Exception as e:
        print(f"Error creating analyzed_offers: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

# ---- Add this function ----
from datetime import datetime

def refresh_missing_mp_table(conn):
    """Refresh missing_mp_entries table with proper column reference"""
    cursor = conn.cursor()
    today = datetime.today().strftime('%Y-%m-%d')
    
    try:
        # Clean up existing table
        cursor.execute("DROP TABLE IF EXISTS missing_mp_entries")
        
        # Create new table with correct column reference
        cursor.execute(f'''
            CREATE TABLE missing_mp_entries AS
            SELECT 
                o.supplier,
                o.iata,
                o.valid_since,
                o.valid_until
            FROM offers o
            LEFT JOIN mp m 
                ON TRIM(o.supplier) = TRIM(m.supplier)
                AND TRIM(o.iata) = TRIM(m.iata)
                AND ? BETWEEN DATE(m.start) AND DATE(m.end)
            WHERE 
                TRIM(o.period) = 'MP'
                AND ? BETWEEN DATE(o.valid_since) AND DATE(o.valid_until)
                AND m.rowid IS NULL;  -- Changed from m.id to m.rowid
        ''', (today, today))
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"SQLite error refreshing MP table: {str(e)}")
    except ValueError as e:
        print(f"Value error refreshing MP table: {str(e)}")
    except Exception as e:
        print(f"Error refreshing MP table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def refresh_missing_platts_table(conn):
    """Identifies offers missing valid Platts price references"""
    cursor = conn.cursor()
    today = datetime.today().strftime('%Y-%m-%d')
    
    try:
        # Clear previous table if exists
        cursor.execute("DROP TABLE IF EXISTS missing_platts_entries")
        
        # Create new comparison table
        cursor.execute('''
            CREATE TABLE missing_platts_entries AS
            SELECT 
                o.supplier,
                o.iata,
                o.platts AS offer_platts,
                o.period AS offer_period,
                o.valid_since,
                o.valid_until
            FROM offers o
            LEFT JOIN platts p 
                ON TRIM(o.platts) = TRIM(p.platts)
                AND TRIM(o.period) = TRIM(p.period)
                AND ? BETWEEN DATE(p.start) AND DATE(p.end)
            WHERE 
                ? BETWEEN DATE(o.valid_since) AND DATE(o.valid_until)
                AND p.rowid IS NULL  -- Changed from p.id to p.rowid
                AND o.platts IS NOT NULL
                AND o.platts != '0'
        ''', (today, today))
    except sqlite3.Error as e:
        print(f"SQLite error refreshing Platts table: {str(e)}")
    except ValueError as e:
        print(f"Value error refreshing Platts table: {str(e)}")
        print("Missing Platts entries table refreshed successfully")
        
    except Exception as e:
        print(f"Error refreshing Platts table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def create_enhanced_analyzed_table(conn):
    cursor = conn.cursor()
    try:
        # Verify source tables exist
        cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='analyzed_offers'")
        if cursor.fetchone()[0] == 0:
            print("Error: analyzed_offers table does not exist!")
            return

        cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='offers_numeric'")
        if cursor.fetchone()[0] == 0:
            print("Error: offers_numeric table does not exist!")
            return

        # Drop existing enhanced table
        cursor.execute("DROP TABLE IF EXISTS analyzed_offers_enhanced")

        # Create enhanced table with proper NULL handling
        cursor.execute('''
            CREATE TABLE analyzed_offers_enhanced AS
            SELECT 
                ao.*,
                onr.platts AS platts_value,
                (COALESCE(onr.platts, 0) 
                 + COALESCE(ao.total_fees_calc, 0) 
                 + COALESCE(ao.diff_total, 0)) AS price_usc_per_gallon,
                (COALESCE(onr.platts, 0) 
                 + COALESCE(ao.total_fees_calc, 0) 
                 + COALESCE(ao.diff_total, 0)) * 3.3022 AS price_dollar_per_tonne
            FROM analyzed_offers ao
            JOIN offers_numeric onr ON ao.id = onr.id
        ''')

        # Add performance index
        cursor.execute('''
            CREATE INDEX idx_enhanced_main
            ON analyzed_offers_enhanced (iata, supplier)
        ''')

        conn.commit()
        print("Enhanced analysis table created successfully")

    except sqlite3.Error as e:
        print(f"SQLite error creating enhanced table: {str(e)}")
        conn.rollback()
    except Exception as e:
        print(f"Error creating enhanced table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()



def create_min_prices_table(conn):
    cursor = conn.cursor()
    try:
        # Drop the table explicitly
        cursor.execute("DROP TABLE IF EXISTS min_prices_per_iata")
        
        # Debug: Verify enhanced table exists
        cursor.execute("SELECT count(*) FROM analyzed_offers_enhanced")
        count = cursor.fetchone()[0]
        print(f"✅ analyzed_offers_enhanced contains {count} rows")
        
        # Debug: Test simple query
        cursor.execute("SELECT * FROM analyzed_offers_enhanced LIMIT 5")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        
        # Create minimum prices table using window functions (requires SQLite 3.25+)
        cursor.execute('''
            CREATE TABLE min_prices_per_iata AS
SELECT 
    id, iata, supplier, period, 
    price_usc_per_gallon, price_dollar_per_tonne
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY iata 
            ORDER BY 
                -- Prioritize SAF_diff_calc > 0 first
                CASE WHEN SAF_diff_calc > 0 THEN 0 ELSE 1 END,
                -- Then order by rank_final (lowest first)
                rank_final ASC
        ) AS rn
    FROM analyzed_offers_enhanced
)
WHERE rn = 1;
        ''')
        
        # Create index for performance optimization
        cursor.execute('''
            CREATE INDEX idx_min_prices_iata ON min_prices_per_iata (iata)
        ''')
        
        conn.commit()
        print("✅ Minimum prices table created successfully")

    except sqlite3.Error as e:
        print(f"❌ Error creating minimum prices table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()





def analyze_offers(conn):
    # Clear offers and platts tables
    clear_all_tables(conn)

    # Duplicate and replace offers
    duplicate_and_replace_offers(conn)
    refresh_missing_platts_table(conn)
    refresh_missing_mp_table(conn)
    # Update offers numeric
    update_offers_numeric(conn)

    # Create Analyzed table
    create_analyzed_offers_table(conn)
    create_enhanced_analyzed_table(conn)
    create_min_prices_table(conn)
    create_contracted_offers_table(conn)
    create_enhanced_contracted_table(conn)
    create_contracted_fuel_prices_table(conn)

    # Data retrieval and display functions are not defined, so these lines are removed.
    
def create_contracted_offers_table(conn):
    """Create analyzed_offers table for confirmed='yes' entries."""
    cursor = conn.cursor()
    try:
        cursor.execute('DROP TABLE IF EXISTS analyzed_offers_contracted')

        cursor.execute('''
            CREATE TABLE analyzed_offers_contracted AS
            WITH current_offers AS (
                SELECT *,
                    CASE period
                        WHEN 'PM' THEN 1
                        WHEN '1/2M' THEN 2
                        WHEN 'PW' THEN 3
                        ELSE 4
                    END AS period_rank
                FROM offers_numeric
                WHERE DATE('now') BETWEEN 
                    COALESCE(valid_since, '2000-01-01') AND 
                    COALESCE(valid_until, '3000-12-31')
            ),
            ranked AS (
                SELECT 
                    *,
                    RANK() OVER (PARTITION BY iata ORDER BY period_rank) AS rank1,
                    RANK() OVER (
                        PARTITION BY iata, period_rank 
                        ORDER BY (
                            CASE 
                                WHEN SAF_platts_name IN ('0', 'nqma SAF') THEN
                                    COALESCE(diff * for_currency / NULLIF(per_volume, 0), 0) + 
                                    COALESCE(SAF_diff * SAF_currency / NULLIF(SAF_per_volume, 0), 0)
                                ELSE
                                    COALESCE(SAF_diff * SAF_currency / NULLIF(SAF_per_volume, 0) * SAF_included_as_percent, 0) + 
                                    (1 - COALESCE(SAF_included_as_percent, 0)) * COALESCE(diff * for_currency / NULLIF(per_volume, 0), 0)
                            END
                        )
                    ) AS rank2
                FROM current_offers
            )
            SELECT 
                id,
                valid_since,
                valid_until,
                confirmed,
                supplier,
                iata,
                period,
                COALESCE(diff * for_currency / NULLIF(per_volume, 0), 0) AS diff_calc,
                COALESCE(value_fee * for_currency_1 / NULLIF(per_volume_1, 0), 0) AS value_fee_calc,
                COALESCE(value_fee_1 * for_currency_2 / NULLIF(per_volume_2, 0), 0) AS value_fee_calc_1,
                COALESCE(value_fee_2 * for_currency_3 / NULLIF(per_volume_3, 0), 0) AS value_fee_calc_2,
                COALESCE(value_fee_3 * for_currency_4 / NULLIF(per_volume_4, 0), 0) AS value_fee_calc_3,
                COALESCE(value_fee_4 * for_currency_5 / NULLIF(per_volume_5, 0), 0) AS value_fee_calc_4,
                COALESCE(value_fee_5 * for_currency_6 / NULLIF(per_volume_6, 0), 0) AS value_fee_calc_5,
                COALESCE(value_fee_6 * for_currency_7 / NULLIF(per_volume_7, 0), 0) AS value_fee_calc_6,
                CASE 
                    WHEN SAF_platts_name IN ('0', 'nqma SAF') THEN
                        COALESCE(SAF_diff * SAF_currency / NULLIF(SAF_per_volume, 0), 0)
                    ELSE
                        COALESCE(SAF_diff * SAF_currency / NULLIF(SAF_per_volume, 0) * SAF_included_as_percent, 0)
                END AS SAF_diff_calc,
                (
                    COALESCE(value_fee * for_currency_1 / NULLIF(per_volume_1, 0), 0) + 
                    COALESCE(value_fee_1 * for_currency_2 / NULLIF(per_volume_2, 0), 0) + 
                    COALESCE(value_fee_2 * for_currency_3 / NULLIF(per_volume_3, 0), 0) + 
                    COALESCE(value_fee_3 * for_currency_4 / NULLIF(per_volume_4, 0), 0) + 
                    COALESCE(value_fee_4 * for_currency_5 / NULLIF(per_volume_5, 0), 0) + 
                    COALESCE(value_fee_5 * for_currency_6 / NULLIF(per_volume_6, 0), 0) + 
                    COALESCE(value_fee_6 * for_currency_7 / NULLIF(per_volume_7, 0), 0)
                ) AS total_fees_calc,
                CASE 
                    WHEN SAF_platts_name IN ('0', 'nqma SAF') THEN
                        COALESCE(diff * for_currency / NULLIF(per_volume, 0), 0) + 
                        COALESCE(SAF_diff * SAF_currency / NULLIF(SAF_per_volume, 0), 0)
                    ELSE
                        COALESCE(SAF_diff * SAF_currency / NULLIF(SAF_per_volume, 0) * SAF_included_as_percent, 0) + 
                        (1 - COALESCE(SAF_included_as_percent, 0)) * COALESCE(diff * for_currency / NULLIF(per_volume, 0), 0)
                END AS diff_total,
                rank1,
                rank2,
                (rank1 + (rank2 / 10.0)) AS rank_final
            FROM ranked
            WHERE COALESCE(confirmed, 'yes') = 'yes'
        ''')
        conn.commit()
        print("analyzed_offers_contracted table created successfully")
    except Exception as e:
        print(f"Error creating analyzed_offers_contracted: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def create_enhanced_contracted_table(conn):
    """Create enhanced table with calculated effective dates."""
    cursor = conn.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS analyzed_offers_enhanced_contracted")
        cursor.execute('''
            CREATE TABLE analyzed_offers_enhanced_contracted AS
            SELECT 
                ao.*,
                onr.valid_since AS offer_start,
                onr.valid_until AS offer_end,
                p.start AS platts_start,
                p.end AS platts_end,
                m.start AS mp_start,
                m.end AS mp_end,
                -- Calculate effective start date (MAX of all starts)
                MAX(
                    onr.valid_since,
                    COALESCE(p.start, '0000-00-00'),
                    COALESCE(m.start, '0000-00-00')
                ) AS effective_start,
                -- Calculate effective end date (MIN of all ends)
                MIN(
                    onr.valid_until,
                    COALESCE(p.end, '3000-12-31'),
                    COALESCE(m.end, '3000-12-31')
                ) AS effective_end,
                (COALESCE(onr.platts, 0) + ao.total_fees_calc + ao.diff_total) AS price_usc_per_gallon,
                (COALESCE(onr.platts, 0) + ao.total_fees_calc + ao.diff_total) * 3.3022 AS price_dollar_per_tonne
            FROM analyzed_offers_contracted ao
            JOIN offers_numeric onr ON ao.id = onr.id
            LEFT JOIN (
                SELECT platts, period, start, end,
                    ROW_NUMBER() OVER (PARTITION BY platts, period ORDER BY end DESC) AS rn
                FROM platts
                WHERE DATE('now') BETWEEN start AND end
            ) p ON onr.platts = p.platts AND onr.period = p.period AND p.rn = 1
            LEFT JOIN (
                SELECT supplier, iata, start, end,
                    ROW_NUMBER() OVER (PARTITION BY supplier, iata ORDER BY end DESC) AS rn
                FROM mp
                WHERE DATE('now') BETWEEN start AND end
            ) m ON onr.supplier = m.supplier AND onr.iata = m.iata AND m.rn = 1
        ''')
        conn.commit()
        print("Enhanced contracted table created successfully")
    except sqlite3.Error as e:
        print(f"Error creating enhanced contracted table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def create_contracted_fuel_prices_table(conn):
    """Create final table with proper date ranges."""
    cursor = conn.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS Contracted_fuel_prices")
        cursor.execute('''
            CREATE TABLE Contracted_fuel_prices AS
SELECT 
    id, iata, supplier, period,
    price_usc_per_gallon, price_dollar_per_tonne,
    CASE 
        WHEN period = 'PM' THEN DATE('now', 'start of month')
        WHEN period = 'PW' THEN 
            CASE 
                WHEN strftime('%w', 'now') = '2' THEN DATE('now')
                ELSE DATE('now', 'weekday 2', '-7 days')
            END
        WHEN period = '1/2M' THEN
            CASE 
                WHEN DATE('now') <= DATE('now', 'start of month', '+15 day') 
                THEN DATE('now', 'start of month')
                ELSE DATE('now', 'start of month', '+15 day')
            END
        ELSE effective_start
    END AS start,
    CASE 
        WHEN period = 'PM' THEN DATE('now', 'start of month', '+1 month', '-1 day')
        WHEN period = 'PW' THEN 
            CASE 
                WHEN strftime('%w', 'now') = '1' THEN DATE('now')
                ELSE DATE('now', 'weekday 1')
            END
        WHEN period = '1/2M' THEN
            CASE 
                WHEN DATE('now') <= DATE('now', 'start of month', '+15 day') 
                THEN DATE('now', 'start of month', '+15 day')
                ELSE DATE('now', 'start of month', '+1 month', '-1 day')
            END
        ELSE effective_end
    END AS end
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY iata 
            ORDER BY price_dollar_per_tonne ASC
        ) AS rn
    FROM analyzed_offers_enhanced_contracted
)
WHERE rn = 1;

        ''')
        conn.commit()
        print("Contracted_fuel_prices table created with correct date ranges")
    except sqlite3.Error as e:
        print(f"Error creating Contracted_fuel_prices: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def refresh_all_calculations(conn):
    """Update all derived tables in proper sequence."""
    try:
        # Clear old tables first
        cursor = conn.cursor()
        cursor.executescript('''
            DROP TABLE IF EXISTS analyzed_offers_contracted;
            DROP TABLE IF EXISTS analyzed_offers_enhanced_contracted;
            DROP TABLE IF EXISTS Contracted_fuel_prices;
        ''')
        conn.commit()
        
        # Create base tables
        duplicate_and_replace_offers(conn)
        update_offers_numeric(conn)
        refresh_missing_platts_table(conn)
        refresh_missing_mp_table(conn)
        
        # Create contracted tables in sequence
        create_contracted_offers_table(conn)
        create_enhanced_contracted_table(conn)  # Fixed syntax version
        create_contracted_fuel_prices_table(conn)
        
        # Create legacy tables
        create_analyzed_offers_table(conn)
        create_enhanced_analyzed_table(conn)
        create_min_prices_table(conn)
        
        # Verify tables exist
        cursor.execute('''
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            AND name IN (
                'analyzed_offers_contracted',
                'analyzed_offers_enhanced_contracted',
                'Contracted_fuel_prices'
            )
        ''')
        existing_tables = [row[0] for row in cursor.fetchall()]
        print(f"Verified existing tables: {existing_tables}")
        
    except Exception as e:
        print(f"Critical error in refresh: {str(e)}")
    finally:
        export_all_tables_to_csv(conn)
        cursor.close()
    
    # 4. Periodic maintenance
    conn.execute("VACUUM")  # Rebuild database file to optimize space
    
    # 5. Optional: Immediate export
    export_all_tables_to_csv(conn)


DEFAULT_VALUES = {
    'for_currency': 'USD',
    'per_volume': 'gallion.USG.UGL',
    'SAF_platts_name': 'nqma SAF',
    'SAF_currency': 'USD',
    'SAF_per_volume': 'gallion.USG.UGL',
    'SAF_diff': '0',
    'SAF_included_as_percent': '0',
    'SAF_period': 'MP',
    'value_fee': '0',
    'confirmed': 'Tender' ,
    'for_currency_1': 'USD',
    'per_volume_1': 'gallion.USG.UGL',
    'value_fee_1': '0',
    'for_currency_2': 'USD',
    'per_volume_2': 'gallion.USG.UGL',
    'value_fee_2': '0',
    'for_currency_3': 'USD',
    'per_volume_3': 'gallion.USG.UGL',
    'value_fee_3': '0',
    'for_currency_4': 'USD',
    'per_volume_4': 'gallion.USG.UGL',
    'value_fee_4': '0',
    'for_currency_5': 'USD',
    'per_volume_5': 'gallion.USG.UGL',
    'value_fee_5': '0',
    'for_currency_6': 'USD',
    'per_volume_6': 'gallion.USG.UGL',
    'value_fee_6': '0',
    'for_currency_7': 'USD',
    'per_volume_7': 'gallion.USG.UGL',
    'value_fee_7': '0'
}

def generate_supplier_feedback(conn, export_dir):
    """Generates supplier-specific feedback CSVs with additional top-ranked summary."""
    if not export_dir:
        return

    try:
        cursor = conn.cursor()
        
        # Get unique suppliers
        cursor.execute("SELECT DISTINCT supplier FROM analyzed_offers")
        suppliers = [row[0] for row in cursor.fetchall()]
        
        if not suppliers:
            messagebox.showinfo("Info", "No suppliers found in analyzed_offers.")
            return

        # Get all analyzed offers data
        df = pd.read_sql("SELECT * FROM analyzed_offers", conn)
        
        # Generate original feedback files (masked suppliers)
        for supplier in suppliers:
            # Mask other suppliers' names
            df_feedback = df.copy()
            df_feedback['supplier'] = df_feedback.apply(
                lambda row: row['supplier'] if row['supplier'] == supplier else "", 
                axis=1
            )
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"feedback_{supplier}_{timestamp}.csv"
            filepath = os.path.join(export_dir, filename)
            
            # Export to CSV
            df_feedback.to_csv(filepath, index=False)
        
        # Generate top-ranked summary file
        df_summary = df.copy()
        
        # Get minimum rank_final per IATA
        df_min_rank = df_summary.loc[df_summary.groupby('iata')['rank_final'].idxmin()]
        
        # Generate summary filename with timestamp
        summary_filename = f"top_ranks_summary_{timestamp}.csv"
        summary_filepath = os.path.join(export_dir, summary_filename)
        
        # Sort by supplier and IATA before exporting
        df_sorted = df_min_rank.sort_values(['supplier', 'iata'])
        
        # Export summary to CSV
        df_sorted.to_csv(summary_filepath, index=False)
        
        messagebox.showinfo("Success", f"Feedback files saved to:\n{export_dir}\n\nSummary file: {summary_filename}")
    
    except Exception as e:
        messagebox.showerror("Error", f"Failed to generate feedback:\n{str(e)}")

import sqlite3

def add_missing_column(conn):
    """Safely add value_per_usdc column to mp table"""
    cursor = conn.cursor()
    
    try:
        # Check if column already exists
        cursor.execute("PRAGMA table_info(mp)")
        columns = [col[1] for col in cursor.fetchall()]  # Corrected syntax
        
        if 'value_per_usdc' not in columns:
            cursor.execute("ALTER TABLE mp ADD COLUMN value_per_usdc REAL;")
            print("Added value_per_usdc column")
        else:
            print("Column already exists")
            
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error adding column: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

# Run this function
#add_missing_column("tender_offers.db")

# --- Main Execution ---
if __name__ == "__main__":
    root = tk.Tk()
    app = MainApp(root)
    root.mainloop()

