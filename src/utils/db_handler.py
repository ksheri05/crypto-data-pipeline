import psycopg2
import pandas as pd
import logging
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


class DatabaseHandler:

    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.engine = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )

            # Create SQLAlchemy engine for pandas
            connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            self.engine = create_engine(connection_string)

            logger.info(f"Connected to database: {self.database}")

        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

    def execute_sql_file(self, file_path):
        try:
            with open(file_path, 'r') as file:
                sql_script = file.read()

            cursor = self.connection.cursor()
            cursor.execute(sql_script)
            self.connection.commit()
            cursor.close()

            logger.info(f"Executed SQL file: {file_path}")

        except Exception as e:
            logger.error(f"Error executing SQL file: {e}")
            self.connection.rollback()
            raise

    def load_gold_data(self, df, table_name):
        try:
            logger.info(f"Loading data to table: {table_name}")

            # Use pandas to_sql for easy insertion
            df.to_sql(
                table_name,
                self.engine,
                if_exists='append',  # Append to existing table
                index=False,
                method='multi'
            )

            rows_inserted = len(df)
            logger.info(f"Loaded {rows_inserted} records to {table_name}")
            return rows_inserted

        except Exception as e:
            logger.error(f"Error loading data to database: {e}")
            raise