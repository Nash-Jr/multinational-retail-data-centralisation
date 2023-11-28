import yaml
from sqlalchemy import create_engine, MetaData, Table
import requests
import tabula
import argparse
import pandas as pd
from sqlalchemy import text
from data_cleaning import DataCleaning as MyDataCleaning


data_cleaner = MyDataCleaning()


class DatabaseConnector:
    """
    Class for connecting to a database, retrieving PDF data, and uploading data to the database.

    Methods:
    - __init__: Initialize the DatabaseConnector instance.
    - init_db_engine: Initialize the database engine.
    - read_db_creds: Read database credentials from a YAML file.
    - list_db_tables: List the tables in the connected database.
    - retrieve_pdf_data: Retrieve data from a PDF linked in a URL.
    - upload_to_db: Upload dataframes to the connected database.
    - some_other_method: An example method demonstrating how to use the DataCleaning class and upload data to the database.
    - from_yaml: Create an instance of DatabaseConnector from a YAML file.

    """

    def __init__(self, config_file_path=r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\db_creds.yml"):
        """
        Initialize the DatabaseConnector instance.

        Parameters:
        - config_file_path: Path to the YAML file containing database credentials.
        """
        self.config_file_path = config_file_path
        self.db_credentials = self.read_db_creds()
        self.db_engine = self.init_db_engine()

    def init_db_engine(self):
        """
        Initialize the database engine.

        Returns:
        - engine: SQLAlchemy database engine.
        """
        engine = create_engine(
            f"postgresql://{self.db_credentials['RDS_USER']}:{self.db_credentials['RDS_PASSWORD']}@{self.db_credentials['RDS_HOST']}:{self.db_credentials['RDS_PORT']}/{self.db_credentials['RDS_DATABASE']}"
        )
        return engine

    def read_db_creds(self):
        """
        Read database credentials from a YAML file.

        Returns:
        - db_credentials: Dictionary containing database credentials.
        """
        with open(self.config_file_path, 'r') as file:
            try:
                data = yaml.safe_load(file)
                return {
                    'RDS_HOST': data['RDS_HOST'],
                    'RDS_PASSWORD': data['RDS_PASSWORD'],
                    'RDS_USER': data['RDS_USER'],
                    'RDS_DATABASE': data['RDS_DATABASE'],
                    'RDS_PORT': data['RDS_PORT']
                }
            except yaml.YAMLError as e:
                print(f"Error reading YAML file {self.config_file_path}: {e}")
                return None

    def list_db_tables(self):
        """
        List the tables in the connected database.

        Returns:
        - table_names: List of table names in the database.
        """
        metadata = MetaData()
        metadata.reflect(bind=self.db_engine)
        table_names = metadata.tables.keys()
        return table_names

    def retrieve_pdf_data(self, url):
        """
        Retrieve data from a PDF linked in a URL.

        Parameters:
        - url: URL of the PDF.

        Returns:
        - final_df: Pandas DataFrame containing data from the PDF.
        """
        response = requests.get(url)
        if response.status_code == 200:
            with open("downloaded_file.pdf", 'wb') as f:
                f.write(response.content)

            df_list = tabula.read_pdf(
                "downloaded_file.pdf", pages='all', multiple_tables=True)

            final_df = pd.concat(df_list)
            return final_df
        else:
            print(f"Failed to download the PDF from the URL:{url}")
            return pd.DataFrame()

    def upload_to_db(self, dataframes_and_tables):
        """
        Upload dataframes to the connected database.

        Parameters:
        - dataframes_and_tables: List of tuples containing dataframes and corresponding table names.
        """
        metadata = MetaData()
        metadata.reflect(bind=self.db_engine)

        for df, table_name in dataframes_and_tables:
            table = Table(table_name, metadata, autoload=True)
            data = df.to_dict(orient='records')
            with self.db_engine.connect() as connection:
                connection.execute(table.insert(), data)

    def some_other_method(self):
        """
        A method demonstrating how to use the DataCleaning class and upload data to the database.
        """
        data_cleaner = MyDataCleaning()
        dataframes_and_tables = [
            (data_cleaner.cleaned_data_df, 'dim_users'),
            (data_cleaner.clean_card_data_df, 'dim_card_details'),
            (data_cleaner.clean_stores_dataframe, 'dim_store_details'),
            (data_cleaner.clean_products_data, 'dim_products'),
            (data_cleaner.cleaned_orders_df, 'orders_table'),
            (data_cleaner.Clean_sales_date, 'dim_date_times')
        ]
        self.upload_to_db(dataframes_and_tables)
        tables = self.list_db_tables()
        print("Tables in the database:", tables)

        dim_store_details_data = self.fetch_data_from_table(
            'dim_store_details')
        print("Data from 'dim_store_details' table:")
        print(dim_store_details_data)

    def fetch_data_from_table(self, table_name):
        """
        Fetch data from a specific table in the connected database.

        Parameters:
        - table_name: Name of the table.

        Returns:
        - table_data: Pandas DataFrame containing data from the table.
        """
        metadata = MetaData()
        metadata.reflect(bind=self.db_engine)

        if table_name in metadata.tables:
            table = metadata.tables[table_name]
            with self.db_engine.connect() as connection:
                try:
                    query = text(f"SELECT * FROM {table_name}")
                    result = connection.execute(query)
                    table_data = pd.DataFrame(
                        result.fetchall(), columns=result.keys())
                    print(f"Data fetched from table '{table_name}':")
                    print(table_data)
                    return table_data
                except Exception as e:
                    print(
                        f"Error fetching data from table '{table_name}': {e}")
                    return pd.DataFrame()
        else:
            print(f"Table '{table_name}' not found in the database.")
            return pd.DataFrame()

    def change_datatype(self, database_identifier):
        with self.db_engine.connect() as connection:
            if database_identifier == 'orders_table':
                sql_statments = [
                    "ALTER TABLE orders_table ALTER COLUMN date_uuid SET DATA TYPE UUID",
                    "ALTER TABLE orders_table ALTER COLUMN user_uuid SET DATA TYPE UUID",
                    "ALTER TABLE orders_table ALTER COLUMN card_number SET DATA TYPE VARCHAR(16)",
                    "ALTER TABLE orders_table ALTER COLUMN store_code SET DATA TYPE VARCHAR(10)",
                    "ALTER TABLE orders_table ALTER COLUMN product_code SET DATA TYPE VARCHAR(10)",
                    "ALTER TABLE orders_table ALTER COLUMN product_quantity SET DATA TYPE SMALLINT"
                ]

            elif database_identifier == 'dim_users_table':
                sql_statments = [
                    "ALTER TABLE dim_users_table ALTER COLUMN first_name SET DATA TYPE VARCHAR(255)"
                    "ALTER TABLE dim_users_table ALTER COLUMN last_name SET DATA TYPE VARCHAR(255)"
                    "ALTER TABLE dim_users_table ALTER COLUMN date_of_birth DATA TYPE DATE"
                    "ALTER TABLE dim_users_table ALTER COLUMN country_code DATA TYPE VARCHAR(3)"
                    "ALTER TABLE dim_users_table ALTER COLUMN user_uuid DATA TYPE UUID"
                    "ALTER TABLE dim_users_table ALTER COLUMN join_date DATA TYPE DATE"
                ]

            elif database_identifier == 'dim_store_details':
                sql_statments = [
                    "ALTER TABLE dim_store_details ALTER COLUMN longitude SET DATA TYPE FLOAT"
                    "ALTER TABLE dim_store_details ALTER COLUMN locality SET DATA TYPE VARCHAR(255)"
                    "ALTER TABLE dim_store_details ALTER COLUMN store_code SET DATA TYPE CARCHAR(?)"
                    "ALTER TABLE dim_store_details ALTER COLUMN staff_numbers SET DATA TYPE SMALLINT"
                    "ALTER TABLE dim_store_details ALTER COLUMN opening_data SET DATA TYPE DATE"
                    "ALTER TABLE dim_store_details ALTER COLUMN store_type SET DATA TYPE VARCHAR(255) ALTER COLUMN DROP NOT NULL"
                    "ALTER TABLE dim_store_details ALTER COLUMN latitude SET DATA TYPE FLOAT"
                    "ALTER TABLE dim_store_details ALTER COLUMN country_code SET DATA TYPE VARCHAR(3)"
                    "ALTER TABLE dim_store_details ALTER COLUMN continent SET DATA TYPE VARCHAR(255)"
                ]

            for sql_statement in sql_statments:
                connection.execute(text(sql_statement))

    @classmethod
    def from_yaml(cls, file_path=r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\db_creds.yml"):
        """
        Create an instance of DatabaseConnector from a YAML file.

        Parameters:
        - file_path: Path to the YAML file containing database credentials.

        Returns:
        - DatabaseConnector instance.
        """
        with open(file_path, 'r') as file:
            try:
                data = yaml.safe_load(file)
                return cls(
                    host=data['RDS_HOST'],
                    database=data['RDS_DATABASE'],
                    user=data['RDS_USER'],
                    password=data['RDS_PASSWORD'],
                    port=data['RDS_PORT']
                )
            except yaml.YAMLError as e:
                print(f"Error reading YAML file {file_path}: {e}")
                return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Retrieve and process data from a PDF linked in a URL.")
    parser.add_argument("url", type=str, help="URL of the PDF")
    args = parser.parse_args()

    db_connector = DatabaseConnector.from_yaml(
        r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\db_creds.yml")
    result_df = db_connector.retrieve_pdf_data(args.url)

    if not result_df.empty:
        print(result_df)

    db_credentials = {
        'config_file_path': r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\db_creds.yml"
    }
    db_connector = DatabaseConnector(**db_credentials)
    db_connector.upgrade_schema()
