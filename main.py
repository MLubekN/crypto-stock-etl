from src.extractors.crypto_extractor import CryptoExtractor
import yaml



with open('config.yaml', 'r') as f:
    assets_all = yaml.safe_load(f)

assets = assets_all['assets']['crypto']


c = CryptoExtractor(assets)
c.save_to_json(c.fetch_prices())