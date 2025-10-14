import pandas as pd
import requests
import io
import pyodbc
from sqlalchemy import create_engine, types 
import os
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import hashlib

# -------------------------------
# Data type mapping from pandas to SQL Server
# -------------------------------
def get_sql_data_type(dtype, column_name='', column_data=None):
    """Map pandas data types to SQL Server data types with dynamic string sizing"""
    dtype_str = str(dtype).lower()
    
    # Currency/amount columns
    if any(keyword in column_name.lower() for keyword in ['amount', 'balance', 'price', 'value']):
        return types.DECIMAL(15, 2)

    # Currency/amount columns
    if any(keyword in column_name.lower() for keyword in ['address', 'location']):
        return types.TEXT()
    
    # Date/time columns
    if any(keyword in column_name.lower() for keyword in ['date', 'time']):
        if 'datetime' in dtype_str:
            return types.DATETIME()
        else:
            return types.DATE()
    
    # ID columns
    if any(keyword in column_name.lower() for keyword in ['id', 'num', 'no', 'code']):
        if 'int' in dtype_str:
            return types.INTEGER()
        else:
            return types.NVARCHAR(50)
    
    # Specific type mapping
    if 'int' in dtype_str:
        return types.BIGINT()
    elif 'float' in dtype_str:
        return types.DECIMAL(15, 4)
    elif 'bool' in dtype_str:
        return types.BIT()
    elif 'datetime' in dtype_str:
        return types.DATETIME()
    elif 'object' in dtype_str or 'string' in dtype_str:
        # Calculate appropriate length for string columns
        if column_data is not None:
            try:
                # Get max length, handling None values
                max_len = column_data.dropna().astype(str).str.len().max()
                if pd.isna(max_len) or max_len == 0:
                    return types.NVARCHAR(500)
                # Add 20% buffer, min 50, max 4000
                suggested_len = min(max(int(max_len * 1.2), 50), 4000)
                return types.NVARCHAR(suggested_len)
            except Exception as e:
                print(f"  Warning: Could not calculate length for {column_name}, using default")
                return types.NVARCHAR(500)
        return types.NVARCHAR(500)
    else:
        return types.NVARCHAR(500)

# -------------------------------
# Bronze Layer Metadata Functions
# -------------------------------
def add_bronze_metadata(df, source_url):
    """Add bronze layer metadata columns to the dataframe"""
    df_with_metadata = df.copy()
    
    # Add metadata columns
    df_with_metadata['_source_file_url'] = source_url
    df_with_metadata['_ingestion_timestamp'] = pd.Timestamp.now('UTC')
    df_with_metadata['_source_hash'] = generate_source_hash(df)
    
    return df_with_metadata

def generate_source_hash(df):
    """Generate a hash of the source data for change detection"""
    # Create a string representation of the entire dataframe (without metadata columns)
    data_string = df.astype(str).to_string(index=False).encode('utf-8')
    
    # Generate hash
    return hashlib.md5(data_string).hexdigest()

def get_sql_data_type_with_metadata(dtype, column_name='', column_data=None):
    """Updated data type mapping that includes bronze metadata columns"""
    # Handle bronze metadata columns specifically
    if column_name == '_source_file_url':
        return types.NVARCHAR(1000)  # Long enough for URLs
    elif column_name == '_ingestion_timestamp':
        return types.DATETIME()
    elif column_name == '_source_hash':
        return types.NVARCHAR(32)  # MD5 hash length
    
    # Use your existing logic for other columns
    return get_sql_data_type(dtype, column_name, column_data)

# -------------------------------
# Data preprocessing function
# -------------------------------
def preprocess_dataframe(df, table_name):
    """Preprocess dataframe to handle problematic data types for SQL Server compatibility"""
    df = df.copy()
    
    # Convert boolean columns to int (0/1) for SQL Server BIT compatibility
    bool_columns = df.select_dtypes(include=['bool']).columns
    for col in bool_columns:
        df[col] = df[col].astype('int64')
    
    # Handle datetime columns - remove timezone info and handle NaT
    datetime_columns = df.select_dtypes(include=['datetime64']).columns
    for col in datetime_columns:
        if df[col].dtype.kind == 'M':
            # Convert timezone-aware to timezone-naive
            if hasattr(df[col].dt, 'tz') and df[col].dt.tz is not None:
                df[col] = df[col].dt.tz_localize(None)
            # Replace NaT with None for SQL Server compatibility
            df[col] = df[col].where(pd.notna(df[col]), None)
    
    # Handle date columns in object format
    for col in df.columns:
        if any(keyword in col.lower() for keyword in ['date', 'time']):
            if df[col].dtype == 'object':
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    if hasattr(df[col].dt, 'tz') and df[col].dt.tz is not None:
                        df[col] = df[col].dt.tz_localize(None)
                    df[col] = df[col].where(pd.notna(df[col]), None)
                except:
                    pass
    
    return df

# -------------------------------
# Improved download function with retry logic
# -------------------------------
def download_with_retry(url, max_retries=3, initial_delay=5):
    """Download with retry logic for network issues"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=max_retries,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        backoff_factor=1
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            return response
        except (requests.exceptions.ConnectTimeout, 
                requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.HTTPError) as e:
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt)
                print(f"  Retry {attempt + 1}/{max_retries} after {delay}s delay...")
                time.sleep(delay)
            else:
                raise e

def download_parquet_file(url):
    """Download and parse parquet file with error handling"""
    try:
        response = download_with_retry(url)
        
        # Try different parquet engines
        for engine in [None, 'pyarrow', 'fastparquet']:
            try:
                df = pd.read_parquet(io.BytesIO(response.content), engine=engine)
                return df, True
            except Exception as e:
                continue
        return None, False
                    
    except Exception as e:
        print(f"  Download error: {str(e)}")
        return None, False

# -------------------------------
# Database setup
# -------------------------------
def setup_database(server, database, schema):
    """Create database and schema if they don't exist"""
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE=master;"
            f"Trusted_Connection=yes;"
        )
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()

        # Create database if it doesn't exist
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = ?)
            BEGIN
                EXEC('CREATE DATABASE ' + ?)
            END
        """, (database, database))

        cursor.execute(f"USE {database}")
        
        # Create schema if it doesn't exist
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = ?)
            BEGIN
                EXEC('CREATE SCHEMA ' + ?)
            END
        """, (schema, schema))

        cursor.close()
        conn.close()
        print(f"  Database '{database}' and schema '{schema}' ready")
        return True
    except Exception as e:
        print(f"ERROR: Database setup failed: {str(e)}")
        return False

# -------------------------------
# Main execution
# -------------------------------
def main():
    print("Starting banking data import process with Bronze layer metadata...")
    
    # Generate URLs
    transactions_urls = [
        f"https://raw.githubusercontent.com/inhamo/Datasets-Final/main/banking_data/transactions_by_year/transactions_{year}_{x:02d}.parquet"
        for year in [2023, 2024]
        for x in range(0, 13)
    ]
    data_urls = transactions_urls + [
        f"https://raw.githubusercontent.com/inhamo/Datasets-Final/main/banking_data/accounts_{year}.parquet"
        for year in [2023, 2024]
    ] + [
        f"https://raw.githubusercontent.com/inhamo/Datasets-Final/main/banking_data/debit_orders_{year}.parquet"
        for year in [2023, 2024]
    ] + [
        f"https://raw.githubusercontent.com/inhamo/Datasets-Final/main/banking_data/customers_{year}.parquet"
        for year in [2023, 2024]
    ] + [
        f"https://raw.githubusercontent.com/inhamo/Datasets-Final/main/banking_data/loans_{year}.parquet"
        for year in [2023, 2024]
    ] 
    
    # Download datasets
    print(f"Downloading {len(data_urls)} datasets...")
    datasets = []
    failed_downloads = []

    for url in data_urls:
        filename = os.path.basename(url)
        print(f"  Downloading: {filename}")
        df, success = download_parquet_file(url)
        
        if success:
            table_name = (
                f"crm_{filename.replace('.parquet', '')}" 
                if 'customers' in filename
                else f"erp_{filename.replace('.parquet', '')}"
            )
            # Store tuple with (table_name, dataframe, source_url)
            datasets.append((table_name, df, url))
            print(f"   Downloaded: {filename} ({df.shape[0]} rows, {df.shape[1]} columns)")
        else:
            failed_downloads.append(url)
            print(f"   Failed: {filename}")

    if not datasets:
        print("No data downloaded. Exiting.")
        exit(1)

    # Database configuration
    server = "localhost\\SQLEXPRESS"
    database = "banking_db"
    schema = "bronze"

    # Setup database
    print(f"\nSetting up database '{database}' and schema '{schema}'...")
    if not setup_database(server, database, schema):
        exit(1)

    # Create SQLAlchemy engine
    try:
        engine = create_engine(
            f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        )
    except Exception as e:
        print(f"ERROR: SQLAlchemy engine creation failed: {str(e)}")
        exit(1)

    # Import data to SQL Server with Bronze metadata
    print(f"\nImporting data to SQL Server with Bronze layer metadata...")
    successful_imports = 0
    failed_imports = []

    for table_name, df, source_url in datasets:
        full_table_name = f"{schema}.{table_name}"
        
        try:
            print(f"Processing: {full_table_name}...")
            
            # STEP 1: Add Bronze layer metadata
            df_with_metadata = add_bronze_metadata(df, source_url)
            print(f"   Added Bronze metadata columns")
            
            # STEP 2: Preprocess dataframe (keep original data types for bronze!)
            df_processed = preprocess_dataframe(df_with_metadata, table_name)
            # Note: We're only preprocessing for SQL compatibility, NOT changing data types yet
            
            # STEP 3: Create data type mapping
            dtype_mapping = {}
            for column in df_processed.columns:
                dtype_mapping[column] = get_sql_data_type_with_metadata(
                    df_processed[column].dtype, 
                    column,
                    df_processed[column]
                )
            
            # STEP 4: Import to Bronze layer
            if len(df_processed) > 100000:
                df_processed.to_sql(
                    table_name, engine, schema=schema, 
                    if_exists="replace", index=False, 
                    chunksize=10000, dtype=dtype_mapping
                )
            else:
                df_processed.to_sql(
                    table_name, engine, schema=schema, 
                    if_exists="replace", index=False, 
                    dtype=dtype_mapping
                )
            
            successful_imports += 1
            print(f"   Imported to Bronze: {full_table_name} ({df_processed.shape[0]} rows)")
            
        except Exception as e:
            failed_imports.append((full_table_name, str(e)))
            print(f"   Failed: {full_table_name}")
            print(f"    Error: {str(e)[:200]}")

    # Final summary
    print(f"\n" + "="*60)
    print("BRONZE LAYER IMPORT SUMMARY")
    print("="*60)
    print(f"Successful downloads: {len(datasets)}/{len(data_urls)}")
    print(f"Successful imports: {successful_imports}/{len(datasets)}")
    
    if failed_downloads:
        print(f"\nFailed downloads ({len(failed_downloads)}):")
        for url in failed_downloads:
            print(f"  - {os.path.basename(url)}")
    
    if failed_imports:
        print(f"\nFailed imports ({len(failed_imports)}):")
        for table, error in failed_imports:
            print(f"  - {table}")
            print(f"    {error[:150]}")
    
    # Display Bronze layer structure info
    print(f"\nBronze Layer Implementation:")
    print(f"   Added _source_file_url column")
    print(f"   Added _ingestion_timestamp column") 
    print(f"   Added _source_hash column for change detection")
    print(f"   Preserved original source data types")
    print(f"   All data stored in '{schema}' schema")
    
    print(f"\nProcess completed! Bronze layer is ready for Silver processing.")

if __name__ == "__main__":
    main()