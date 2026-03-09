from sqlalchemy import create_engine, text
import os
import logging

class PostgresLoader:
    def __init__(self):
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        db = os.getenv('DB_NAME')
        
        connection_string = f"postgresql+pg8000://{user}:{password}@{host}:{port}/{db}"
        self.engine = create_engine(connection_string)

    def execute_sql_file(self, file_path):
        with open(file_path, 'r') as f:
            sql_script = f.read()

        with self.engine.begin() as conn:
            conn.execute(text(sql_script))
            logging.info(f"SQL script executed from: {file_path}")

    def load_data(self, df, table_name):
        staging_table = f"{table_name}_staging"
        try:
            df.to_sql(staging_table, con=self.engine, if_exists='replace', index=False)
            
            upsert_query = text(f"""
                INSERT INTO {table_name} (asset_symbol, price_usd, timestamp_utc, source, etl_processed_at)
                SELECT asset_symbol, price_usd, timestamp_utc, source, etl_processed_at 
                FROM {staging_table}
                ON CONFLICT (asset_symbol, timestamp_utc) DO NOTHING;
            """)
            
            with self.engine.begin() as conn:
                conn.execute(upsert_query)
                logging.info(f"Successfully upserted data to {table_name}.")
                
        except Exception as e:
            logging.error(f"Error during upsert: {e}")