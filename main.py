import yaml
from src.extractors.crypto_extractor import CryptoExtractor
from src.extractors.stock_extractor import StockExtractor
from src.utils.logger import setup_logging

def main():
    setup_logging()

    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    crypto_ex = CryptoExtractor(config['assets']['crypto'])
    crypto_data = crypto_ex.fetch_prices()
    crypto_ex.save_to_json(crypto_data)

    stock_ex = StockExtractor(config['assets']['stocks'])
    stock_data = stock_ex.fetch_prices()
    stock_ex.save_to_json(stock_data)


if __name__ == "__main__":
    main()