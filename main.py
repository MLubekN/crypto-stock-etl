import yaml
import glob
import json
import pandas as pd
import os
import shutil
from datetime import datetime
from dotenv import load_dotenv
from src.utils.logger import setup_logging
from src.extractors.crypto_extractor import CryptoExtractor
from src.extractors.stock_extractor import StockExtractor
from src.transformers.data_transformer import DataTransformer
from src.loaders.db_loader import PostgresLoader


def archive_processed_files(files, category):
    """Moves processed files into archive/{category}."""
    if not files:
        return

    archive_dir = os.path.join("data", "archive", category)
    os.makedirs(archive_dir, exist_ok=True)

    for f_path in files:
        file_name = os.path.basename(f_path)
        target_path = os.path.join(archive_dir, file_name)
        shutil.move(f_path, target_path)
    
    print(f"Archived {len(files)} files in: {archive_dir}")


def main():
    load_dotenv(override=True)
    setup_logging()

    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
        crypto_ex = CryptoExtractor(config['assets']['crypto'])
        crypto_data = crypto_ex.fetch_prices()
        crypto_ex.save_to_json(crypto_data)

        stock_ex = StockExtractor(config['assets']['stocks'])
        stock_data = stock_ex.fetch_prices()
        stock_ex.save_to_json(stock_data)

    transformer = DataTransformer()

    crypto_files = glob.glob(os.path.join("data", "raw", "crypto", "*.json"))
    stock_files = glob.glob(os.path.join("data", "raw", "stocks", "*.json"))

    crypto_dfs = []
    for f_path in crypto_files:
        with open(f_path, 'r') as f:
            raw_json = json.load(f)
            df = transformer.transform_crypto(raw_json)
            crypto_dfs.append(df)
    
    all_crypto_df = pd.concat(crypto_dfs, ignore_index=True) if crypto_dfs else pd.DataFrame()

    stock_dfs = []
    for f_path in stock_files:
        with open(f_path, 'r') as f:
            raw_json = json.load(f)
            df = transformer.transform_stocks(raw_json)
            stock_dfs.append(df)
            
    all_stocks_df = pd.concat(stock_dfs, ignore_index=True) if stock_dfs else pd.DataFrame()

    if not all_crypto_df.empty or not all_stocks_df.empty:
        final_df = transformer.combine_data(all_crypto_df, all_stocks_df)

        final_df = final_df.drop_duplicates(subset=['asset_symbol', 'timestamp_utc'])

        print("--- FINAL DATASET PREVIEW ---")
        print(final_df)
        print(f"Total records: {len(final_df)}")

        loader = PostgresLoader()
        loader.execute_sql_file('sql/create_tables.sql')
        try:
            loader.load_data(final_df, 'raw_assets')
            print("--- ARCHIVING FILES ---")
            archive_processed_files(crypto_files, "crypto")
            archive_processed_files(stock_files, "stocks")
        except Exception as e:
            print(f"Error during files loading, archiving interrupted: {e}")
    else:
        print("No records to process.")

    

if __name__ == "__main__":
    main()