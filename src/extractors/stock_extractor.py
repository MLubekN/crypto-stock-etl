import yfinance as yf
import json
import os
from datetime import datetime
from src.utils.logger import *

class StockExtractor:
    def __init__(self, tickers):
        self.tickers = tickers
        self.output_dir = "data/raw/stocks"

    def fetch_prices(self):
        """
        Downloads current prices for defined stock assets.
        """
        data_to_save = {}
        try:
            for ticker in self.tickers:
                stock = yf.Ticker(ticker)
                hist = stock.history(period='1d')
                if not hist.empty:
                    data_to_save[ticker] = {
                        "price": float(hist['Close'].iloc[-1]),
                        "currency": stock.info.get('currency', 'USD'),
                        "timestamp": datetime.now().isoformat()
                    }
            return data_to_save
        except Exception as e:
            logging.error(f"Error downloading stock data: {e} ")
            return None
        
    def save_to_json(self, data):
        if not data:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"stock_prices_{timestamp}.json"
        filepath = os.path.join(self.output_dir, filename)

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)

        logging.info(f"Data saved to {filepath}")
        return filepath