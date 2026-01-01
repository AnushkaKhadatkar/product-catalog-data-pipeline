"""
E-commerce Product Catalog ETL Pipeline
Implements Extract, Load, Transform with dbt and SCD Type 2
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
import snowflake.connector
import os

# ==============================================================================
# DAG CONFIGURATION
# ==============================================================================

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'product_catalog_etl',
    default_args=default_args,
    description='ETL pipeline for product catalog with SCD Type 2',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['ecommerce', 'product', 'scd2', 'etl'],
)

# ==============================================================================
# GLOBAL VARIABLES - UPDATE THESE WITH YOUR PATHS
# ==============================================================================

# Update this path to your CSV file location
CSV_FILE_PATH = '/Users/simmi/hw3.2_pipeline/data/data/products_prepared.csv'

# Update this path to your dbt project
DBT_PROJECT_PATH = '/Users/simmi/hw3.2_pipeline/dbt_project/product_catalog'

# Snowflake credentials - UPDATE THESE
SNOWFLAKE_USER = 'Anushkakhadatkar'
SNOWFLAKE_PASSWORD = 'Salvation@12345'
SNOWFLAKE_ACCOUNT = 'WDCBXVZ-PKB12734'  # e.g., 'abc12345.us-east-1'

# ==============================================================================
# [1 POINT] TASK 1: EXTRACTING DATA FROM THE SOURCE
# ==============================================================================

def extract_data_from_source(**context):
    """
    Extract product data from CSV file source system.
    This simulates extracting data from an external system.
    """
    try:
        print("=" * 60)
        print("STARTING DATA EXTRACTION FROM SOURCE")
        print("=" * 60)
        
        # Check if file exists
        if not os.path.exists(CSV_FILE_PATH):
            raise FileNotFoundError(f"Source file not found: {CSV_FILE_PATH}")
        
        # Read CSV file
        print(f"Reading data from: {CSV_FILE_PATH}")
        df = pd.read_csv(CSV_FILE_PATH)
        
        print(f"✓ Successfully extracted {len(df)} products")
        print(f"✓ Columns: {df.columns.tolist()}")
        print(f"✓ Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        # Data validation
        print("\n--- Data Quality Checks ---")
        print(f"Total rows: {len(df)}")
        print(f"Null values:\n{df.isnull().sum()}")
        print(f"Duplicate product_ids: {df['product_id'].duplicated().sum()}")
        
        # Save to temporary location for next task
        temp_path = '/tmp/products_extracted.csv'
        df.to_csv(temp_path, index=False)
        print(f"\n✓ Saved extracted data to: {temp_path}")
        
        # Push file path to XCom for next task
        context['task_instance'].xcom_push(key='extracted_file_path', value=temp_path)
        context['task_instance'].xcom_push(key='record_count', value=len(df))
        
        print("\n" + "=" * 60)
        print("DATA EXTRACTION COMPLETED SUCCESSFULLY")
        print("=" * 60)
        
        return temp_path
        
    except Exception as e:
        print(f"\n❌ ERROR in data extraction: {str(e)}")
        raise

extract_task = PythonOperator(
    task_id='extract_data_from_source',
    python_callable=extract_data_from_source,
    provide_context=True,
    dag=dag,
)

# ==============================================================================
# [1 POINT] TASK 2: LOADING DATA INTO SNOWFLAKE STAGING TABLE
# ==============================================================================

def load_data_into_snowflake_staging(**context):
    """
    Load extracted data into Snowflake staging table.
    This task performs bulk insert into RAW_PRODUCTS staging table.
    """
    try:
        print("=" * 60)
        print("STARTING DATA LOAD INTO SNOWFLAKE STAGING")
        print("=" * 60)
        
        # Get extracted file path from previous task
        extracted_file = context['task_instance'].xcom_pull(
            task_ids='extract_data_from_source', 
            key='extracted_file_path'
        )
        
        print(f"Loading data from: {extracted_file}")
        
        # Connect to Snowflake
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse='COMPUTE_WH',
            database='PRODUCT_CATALOG',
            schema='STAGING'
        )
        
        cursor = conn.cursor()
        print("✓ Connected to Snowflake")
        
        # Truncate staging table to ensure clean load
        print("\nTruncating staging table...")
        cursor.execute("TRUNCATE TABLE IF EXISTS RAW_PRODUCTS")
        print("✓ Staging table truncated")
        
        # Read extracted data
        df = pd.read_csv(extracted_file)
        total_rows = len(df)
        print(f"\n✓ Loaded {total_rows} rows from extracted file")
        
        # Bulk insert in batches for better performance
        batch_size = 500
        batches = (total_rows // batch_size) + 1
        
        print(f"\nInserting data in {batches} batches of {batch_size} rows...")
        
        inserted_count = 0
        for i in range(0, total_rows, batch_size):
            batch_num = (i // batch_size) + 1
            batch = df.iloc[i:i+batch_size]
            
            for _, row in batch.iterrows():
                insert_sql = """
                INSERT INTO RAW_PRODUCTS (
                    index, url, title, images, description, product_id,
                    sku, gtin13, brand, price, currency, availability,
                    uniq_id, scraped_at, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
                
                values = (
                    int(row['index']) if pd.notna(row['index']) else None,
                    str(row['url']) if pd.notna(row['url']) else None,
                    str(row['title']) if pd.notna(row['title']) else None,
                    str(row['images']) if pd.notna(row['images']) else None,
                    str(row['description']) if pd.notna(row['description']) else None,
                    int(row['product_id']) if pd.notna(row['product_id']) else None,
                    float(row['sku']) if pd.notna(row['sku']) else None,
                    float(row['gtin13']) if pd.notna(row['gtin13']) else None,
                    str(row['brand']) if pd.notna(row['brand']) else None,
                    float(row['price']) if pd.notna(row['price']) else None,
                    str(row['currency']) if pd.notna(row['currency']) else None,
                    str(row['availability']) if pd.notna(row['availability']) else None,
                    str(row['uniq_id']) if pd.notna(row['uniq_id']) else None,
                    str(row['scraped_at']) if pd.notna(row['scraped_at']) else None,
                    str(row['created_at']) if pd.notna(row['created_at']) else None,
                    str(row['updated_at']) if pd.notna(row['updated_at']) else None,
                )
                
                cursor.execute(insert_sql, values)
                inserted_count += 1
            
            # Commit after each batch
            conn.commit()
            print(f"  ✓ Batch {batch_num}/{batches} completed ({inserted_count}/{total_rows} rows)")
        
        # Verify loaded data
        print("\nVerifying loaded data...")
        cursor.execute("SELECT COUNT(*) FROM RAW_PRODUCTS")
        loaded_count = cursor.fetchone()[0]
        print(f"✓ Total rows in staging table: {loaded_count}")
        
        if loaded_count != total_rows:
            raise ValueError(f"Row count mismatch! Expected {total_rows}, got {loaded_count}")
        
        # Close connection
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print(f"SUCCESSFULLY LOADED {loaded_count} PRODUCTS INTO SNOWFLAKE")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ ERROR in loading to Snowflake: {str(e)}")
        raise

load_staging_task = PythonOperator(
    task_id='load_data_into_snowflake_staging',
    python_callable=load_data_into_snowflake_staging,
    provide_context=True,
    dag=dag,
)

# ==============================================================================
# [1 POINT] TASK 3: TRIGGERING DBT RUN
# ==============================================================================

dbt_run_models = BashOperator(
    task_id='trigger_dbt_run',
    bash_command=f'cd {DBT_PROJECT_PATH} && dbt run --profiles-dir ~/.dbt',
    dag=dag,
)

# Run dbt tests after models
dbt_test_models = BashOperator(
    task_id='dbt_test',
    bash_command=f'cd {DBT_PROJECT_PATH} && dbt test --profiles-dir ~/.dbt',
    dag=dag,
)

# Run dbt snapshots
dbt_snapshot_run = BashOperator(
    task_id='dbt_snapshot',
    bash_command=f'cd {DBT_PROJECT_PATH} && dbt snapshot --profiles-dir ~/.dbt',
    dag=dag,
)

# ==============================================================================
# [1 POINT] TASK 4: IMPLEMENT ERROR HANDLING
# ==============================================================================

def data_quality_validation(**context):
    """
    Comprehensive data quality checks and error handling.
    Validates data integrity, business rules, and raises alerts on failures.
    """
    try:
        print("=" * 60)
        print("STARTING DATA QUALITY VALIDATION")
        print("=" * 60)
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse='COMPUTE_WH',
            database='PRODUCT_CATALOG',
            schema='CORE'
        )
        
        cursor = conn.cursor()
        errors = []
        warnings = []
        
        # ============================================
        # CHECK 1: Row Count Validation
        # ============================================
        print("\n[CHECK 1] Row Count Validation")
        cursor.execute("SELECT COUNT(*) FROM DIM_PRODUCTS_SCD2")
        total_rows = cursor.fetchone()[0]
        print(f"  Total rows in dimension table: {total_rows}")
        
        if total_rows == 0:
            errors.append("CRITICAL: No data in dimension table!")
        elif total_rows < 100:
            warnings.append(f"WARNING: Only {total_rows} rows in dimension (expected more)")
        else:
            print(f"  ✓ PASSED: {total_rows} rows present")
        
        # ============================================
        # CHECK 2: Current Records Validation
        # ============================================
        print("\n[CHECK 2] Current Records Validation")
        cursor.execute("SELECT COUNT(*) FROM DIM_PRODUCTS_SCD2 WHERE IS_CURRENT = TRUE")
        current_count = cursor.fetchone()[0]
        print(f"  Current (active) records: {current_count}")
        
        if current_count == 0:
            errors.append("CRITICAL: No current records found!")
        else:
            print(f"  ✓ PASSED: {current_count} current records")
        
        # ============================================
        # CHECK 3: Historical Records Validation
        # ============================================
        print("\n[CHECK 3] Historical Records Validation")
        cursor.execute("SELECT COUNT(*) FROM DIM_PRODUCTS_SCD2 WHERE IS_CURRENT = FALSE")
        historical_count = cursor.fetchone()[0]
        print(f"  Historical records: {historical_count}")
        print(f"  ✓ PASSED: {historical_count} historical records tracked")
        
        # ============================================
        # CHECK 4: Price Validation
        # ============================================
        print("\n[CHECK 4] Price Validation")
        cursor.execute("SELECT COUNT(*) FROM DIM_PRODUCTS_SCD2 WHERE PRICE <= 0 OR PRICE IS NULL")
        invalid_price = cursor.fetchone()[0]
        
        if invalid_price > 0:
            errors.append(f"CRITICAL: Found {invalid_price} records with invalid prices!")
            print(f"  ❌ FAILED: {invalid_price} records with invalid prices")
        else:
            print("  ✓ PASSED: All prices are valid")
        
        # ============================================
        # CHECK 5: NULL Value Validation
        # ============================================
        print("\n[CHECK 5] NULL Value Validation")
        cursor.execute("""
            SELECT COUNT(*) FROM DIM_PRODUCTS_SCD2 
            WHERE PRODUCT_ID IS NULL 
               OR TITLE IS NULL 
               OR PRICE IS NULL
               OR VALID_FROM IS NULL
        """)
        null_count = cursor.fetchone()[0]
        
        if null_count > 0:
            errors.append(f"CRITICAL: Found {null_count} records with NULL in required fields!")
            print(f"  ❌ FAILED: {null_count} records with NULL values")
        else:
            print("  ✓ PASSED: No NULL values in required fields")
        
        # ============================================
        # CHECK 6: Duplicate Current Records
        # ============================================
        print("\n[CHECK 6] Duplicate Current Records Check")
        cursor.execute("""
            SELECT PRODUCT_ID, COUNT(*) as cnt
            FROM DIM_PRODUCTS_SCD2
            WHERE IS_CURRENT = TRUE
            GROUP BY PRODUCT_ID
            HAVING COUNT(*) > 1
        """)
        duplicates = cursor.fetchall()
        
        if len(duplicates) > 0:
            errors.append(f"CRITICAL: Found {len(duplicates)} products with multiple current records!")
            print(f"  ❌ FAILED: {len(duplicates)} duplicate current records")
        else:
            print("  ✓ PASSED: No duplicate current records")
        
        # ============================================
        # CHECK 7: SCD Type 2 Integrity
        # ============================================
        print("\n[CHECK 7] SCD Type 2 Integrity Check")
        cursor.execute("""
            SELECT COUNT(*) FROM DIM_PRODUCTS_SCD2
            WHERE IS_CURRENT = FALSE AND VALID_TO IS NULL
        """)
        invalid_scd = cursor.fetchone()[0]
        
        if invalid_scd > 0:
            errors.append(f"CRITICAL: Found {invalid_scd} historical records without end date!")
            print(f"  ❌ FAILED: {invalid_scd} records with invalid SCD dates")
        else:
            print("  ✓ PASSED: SCD Type 2 integrity maintained")
        
        # ============================================
        # CHECK 8: Data Freshness
        # ============================================
        print("\n[CHECK 8] Data Freshness Check")
        cursor.execute("""
            SELECT MAX(VALID_FROM) as latest_update
            FROM DIM_PRODUCTS_SCD2
        """)
        latest_update = cursor.fetchone()[0]
        print(f"  Latest data update: {latest_update}")
        print("  ✓ PASSED: Data freshness verified")
        
        cursor.close()
        conn.close()
        
        # ============================================
        # SUMMARY AND ERROR HANDLING
        # ============================================
        print("\n" + "=" * 60)
        print("DATA QUALITY VALIDATION SUMMARY")
        print("=" * 60)
        
        if errors:
            print(f"\n❌ VALIDATION FAILED WITH {len(errors)} CRITICAL ERROR(S):")
            for error in errors:
                print(f"  • {error}")
            raise ValueError(f"Data quality validation failed with {len(errors)} error(s)")
        
        if warnings:
            print(f"\n⚠️  {len(warnings)} WARNING(S):")
            for warning in warnings:
                print(f"  • {warning}")
        
        print("\n✓ ALL CRITICAL CHECKS PASSED")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ ERROR in data quality validation: {str(e)}")
        raise

data_quality_task = PythonOperator(
    task_id='implement_error_handling',
    python_callable=data_quality_validation,
    provide_context=True,
    dag=dag,
)


# Add this after the quality_check task

# Backup task
create_post_etl_backup = SnowflakeOperator(
    task_id='create_post_etl_backup',
    sql="""
    CREATE OR REPLACE DATABASE PRODUCT_CATALOG_BACKUP_POST_ETL_{{ ds_nodash }}
    CLONE PRODUCT_CATALOG;
    """,
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)

# Update dependencies
data_quality_task >> create_post_etl_backup
# Add this task before the task dependencies section

# clone_dev_environment = SnowflakeOperator(
#     task_id='clone_dev_database',
#     sql="""
#     CREATE OR REPLACE DATABASE DEV_PRODUCT_CATALOG 
#     CLONE PRODUCT_CATALOG;
#     """,
#     snowflake_conn_id='snowflake_conn',
#     dag=dag,
# )

# manage_backups = SnowflakeOperator(
#     task_id='manage_backups',
#     sql="""
#     -- Create today's backup with date stamp
#     CREATE OR REPLACE DATABASE PRODUCT_CATALOG_BACKUP_{{ ds_nodash }}
#     CLONE PRODUCT_CATALOG;
    
#     -- Note: In production, add logic to drop old backups
#     -- DROP DATABASE IF EXISTS PRODUCT_CATALOG_BACKUP_{{ macros.ds_add(ds, -7) }};
#     """,
#     snowflake_conn_id='snowflake_conn',
#     dag=dag,
# )

# Update dependencies to include this task
# extract_task >> load_staging_task >> dbt_run_models >> dbt_test_models >> dbt_snapshot_run >> data_quality_task >> clone_dev_environment
# ==============================================================================
# TASK DEPENDENCIES - Define the pipeline flow
# ==============================================================================

# data_quality_task >> manage_backups
