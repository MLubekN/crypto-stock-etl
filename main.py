import yaml
import glob
import json
import pandas as pd
import os
from src.utils.logger import setup_logging
from src.transformers.data_transformer import DataTransformer

def main():
    setup_logging()
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
        
        print("--- FINAL DATASET PREVIEW ---")
        print(final_df.head())
        print(f"Total records: {len(final_df)}")
        
        return final_df
    else:
        print("No records to process.")

if __name__ == "__main__":
    main()