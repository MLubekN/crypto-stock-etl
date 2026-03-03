import pandas as pd
import json
import os
import logging
from datetime import datetime

class DataTransformer:
    def __init__(self):
        self.processed_at = datetime.utcnow()

    def transform_crypto(self, raw_data):
        records = []
        for asset_id, details in raw_data.items():
            records.append({
                'asset_symbol': asset_id.upper(),
                'price_usd': details.get('usd'),
                'timestamp_utc': pd.to_datetime(details.get('last_updated_at'), unit='s'),
                'source': 'coingecko'
            })
        return pd.DataFrame(records)

    def transform_stocks(self, raw_data):
        records = []
        for ticker, details in raw_data.items():
            records.append({
                'asset_symbol': ticker,
                'price_usd': details.get('price'),
                'timestamp_utc': pd.to_datetime(details.get('timestamp')),
                'source': 'yfinance'
            })
        return pd.DataFrame(records)

    def combine_data(self, crypto_df, stocks_df):
        combined_df = pd.concat([crypto_df, stocks_df], ignore_index=True)
        combined_df['etl_processed_at'] = self.processed_at
        return combined_df