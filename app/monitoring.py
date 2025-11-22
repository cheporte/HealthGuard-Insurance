import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime
import os

DB_INFERENCE_LOG_PATH = 'data/db/inference_log.db'
DB_EXTENDED_PATH = 'data/db/extended_data.db'
CSV_PATH = 'data/ObesityDataSet_raw_and_data_sinthetic.csv'

categories = [
    "Insufficient_Weight", "Normal_Weight", "Overweight_Level_I", 
    "Overweight_Level_II", "Obesity_Type_I", "Obesity_Type_II", 
    "Obesity_Type_III"
]


def migrate_inference_to_extended():
    """
    Migrate data from inference_log.db to extended_data.db.
    Converts prediction_coded (int) to NObeyesdad (category string).
    """
    try:
        print("Reading inference log data...")
        inference_conn = sqlite3.connect(DB_INFERENCE_LOG_PATH)
        inference_df = pd.read_sql_query("SELECT * FROM inference_inputs", inference_conn)
        inference_conn.close()
        
        print(f"Found {len(inference_df)} records in inference_log")
        
        if len(inference_df) == 0:
            print("No data to migrate.")
            return
        
        # Convert prediction_coded to NObeyesdad (category name)
        inference_df['NObeyesdad'] = inference_df['prediction_coded'].map(
            lambda x: categories[int(x)] if 0 <= int(x) < len(categories) else None
        )
        
        # Drop prediction_coded column as it's now represented in NObeyesdad
        inference_df = inference_df.drop('prediction_coded', axis=1)
        
        print("Preparing data for insertion...")
        
        # Connect to extended_data.db
        extended_conn = sqlite3.connect(DB_EXTENDED_PATH)
        
        # Append inference data to extended_data table
        inference_df.to_sql("extended_data", extended_conn, if_exists='append', index=False)
        
        extended_conn.close()
        print(f"✓ Successfully migrated {len(inference_df)} records to extended_data.db")
        
    except Exception as e:
        print(f"Error during migration: {e}")
        import traceback
        traceback.print_exc()


def create_extended_data_from_original():
    """
    Create extended_data.db from original obesity dataset with random timestamps.
    """
    try:
        print("Loading original obesity data from CSV...")
        original_data = pd.read_csv(CSV_PATH)
        
        start_date = pd.to_datetime('2024-01-01')
        end_date = pd.to_datetime('2024-12-31')
        
        n = len(original_data)
        random_dates = pd.to_datetime(
            np.random.randint(start_date.value // 10**9, end_date.value // 10**9, n), unit='s'
        )
        original_data['timestamp'] = random_dates
        
        # Reorder columns to put timestamp first
        cols = ['timestamp'] + [col for col in original_data.columns if col != 'timestamp']
        original_data = original_data[cols]
        
        # Create extended_data.db with original data
        conn = sqlite3.connect(DB_EXTENDED_PATH)
        original_data.to_sql("extended_data", conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"✓ Created extended_data.db with {len(original_data)} original records")
        
    except Exception as e:
        print(f"Error creating extended_data: {e}")
        import traceback
        traceback.print_exc()


def get_extended_data_summary():
    """
    Display summary of extended_data.db contents.
    """
    try:
        if not os.path.exists(DB_EXTENDED_PATH):
            print("extended_data.db does not exist yet. Run 'create' first.")
            return
        
        print("\n" + "="*70)
        print("EXTENDED_DATA.DB SUMMARY")
        print("="*70)
        
        conn = sqlite3.connect(DB_EXTENDED_PATH)
        extended_df = pd.read_sql_query("SELECT * FROM extended_data", conn)
        
        print(f"\nTotal records: {len(extended_df)}")
        print(f"Columns: {extended_df.columns.tolist()}")
        print(f"\nObesity distribution:")
        print(extended_df['NObeyesdad'].value_counts())
        print(f"\nDate range: {extended_df['timestamp'].min()} to {extended_df['timestamp'].max()}")
        print(f"\nSample records (first 3):\n{extended_df.head(3)}")
        print("="*70)
        
        conn.close()
        
    except Exception as e:
        print(f"Error getting extended_data summary: {e}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "create":
            print("Creating extended_data.db from original dataset...")
            create_extended_data_from_original()
            
        elif command == "migrate":
            print("Migrating inference data to extended_data.db...")
            migrate_inference_to_extended()
            
        elif command == "both":
            print("Creating extended_data.db and migrating inference data...")
            create_extended_data_from_original()
            migrate_inference_to_extended()
            
        elif command == "summary":
            get_extended_data_summary()
            
        else:
            print("Usage: python monitoring.py [create|migrate|both|summary]")
            print("  create  - Create extended_data.db from original CSV dataset")
            print("  migrate - Migrate inference_log data to extended_data.db")
            print("  both    - Do both operations (create + migrate)")
            print("  summary - Show extended_data.db summary")
    else:
        # Default: migrate inference data
        print("Running default migration...")
        migrate_inference_to_extended()




