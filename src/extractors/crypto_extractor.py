import requests
import json
import os
from datetime import datetime
from src.utils.logger import *

class CryptoExtractor:
    def __init__(self, assets, currency='usd'):
        self.assets = assets
        self.currency = currency
        self.base_url = "https://api.coingecko.com/api/v3/simple/price"
        self.output_dir = "data/raw/crypto"

    def fetch_prices(self):
        """
        Downloads current prices for defined assets.
        """
        params = {
            'ids': ','.join(self.assets),
            'vs_currencies': self.currency,
            'include_24hr_vol': 'true',
            'include_last_updated_at': 'true'
        }

        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error downloading crypto data: {e}")
            return None
        
    def save_to_json(self, data):
        if not data:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"crypto_prices_{timestamp}.json"
        filepath = os.path.join(self.output_dir, filename)

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)

        logging.info(f"Data saved to {filepath}")
        return filepath