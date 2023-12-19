import yaml
from sqlalchemy import create_engine, MetaData, Table
import requests
import argparse
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from data_cleaning import DataCleaning
from sqlalchemy.exc import SQLAlchemyError


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

    def __init__(self, config_file_path):
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

    def test_db_upload(self, df, table_name):
        print(self.db_engine)
        df.to_sql(table_name, con=self.db_engine, if_exists='replace')

    def upload_to_db(self, data_cleaner, table_name):
        """
        Upload cleaned dataframes to the connected database.

        Parameters:
        - data_cleaner: An instance of the DataCleaning class.
        - table_name: Name of the table to upload the data to.
        """
        metadata = MetaData()
        metadata.reflect(bind=self.db_engine)

        # Get the corresponding cleaning method based on the table name
        cleaning_method_map = {
            'dim_users': data_cleaner.clean_user_data,
            'dim_card_details': data_cleaner.clean_card_data,
            'dim_store_details': data_cleaner.clean_store_data,
            'dim_products': data_cleaner.clean_products_data,
            'orders_table': data_cleaner.clean_orders_data,
            'dim_date_times': data_cleaner.clean_sales_date,
        }

        clean_method = cleaning_method_map.get(table_name)

        if clean_method is None:
            print(f"Cleaning method for table '{table_name}' not found.")
            return

        if table_name not in metadata.tables:
            print(f"Table '{table_name}' does not exist. Creating...")
            cleaned_df = clean_method(data_cleaner)
            cleaned_df.head(0).to_sql(
                table_name, self.db_engine, if_exists='replace', index=False)

        metadata.reflect(bind=self.db_engine)
        table = metadata.tables[table_name]

        data = clean_method(data_cleaner).to_dict(orient='records')

        try:
            with self.db_engine.connect() as connection:
                connection.execute(table.insert(), data)
            print(f"Data uploaded to '{table_name}' table.")
        except SQLAlchemyError as e:
            print(f"Error uploading data to '{table_name}' table: {e}")

    def some_other_method(self, data_cleaner):
        """
        A method demonstrating how to use the DataCleaning class and upload data to the database.

        Parameters:
        - data_cleaner: An instance of the DataCleaning class.
        """
        self.upload_to_db(data_cleaner)

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
            sql_statements = []

            print(
                f"DELETE FROM {'dim_users_table'} WHERE date_of_birth = 'KBTI';")
            if database_identifier == 'orders_table':
                sql_statements.extend([
                    "UPDATE orders_table SET card_number = LEFT(card_number, 16)",
                    "UPDATE orders_table SET store_code = LEFT(store_code, 10)",
                    "UPDATE orders_table SET product_code = LEFT(product_code, 10)",
                    "ALTER TABLE orders_table ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID",
                    "ALTER TABLE orders_table ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID",
                    "ALTER TABLE orders_table ALTER COLUMN card_number TYPE VARCHAR(16)",
                    "ALTER TABLE orders_table ALTER COLUMN store_code TYPE VARCHAR(10)",
                    "ALTER TABLE orders_table ALTER COLUMN product_code TYPE VARCHAR(10)",
                    "UPDATE orders_table SET product_quantity = NULL WHERE LENGTH(product_quantity) > 5",
                    "ALTER TABLE orders_table ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::SMALLINT",
                ])
            elif database_identifier == 'dim_users_table':
                sql_statements.extend([
                    "DELETE FROM dim_users_table WHERE NOT (date_of_birth ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')",
                    "UPDATE dim_users_table SET first_name = LEFT(first_name, 255) WHERE LENGTH(first_name) > 255",
                    "UPDATE dim_users_table SET last_name = LEFT(last_name, 255) WHERE LENGTH(last_name) > 255",
                    "UPDATE dim_users_table SET country_code = LEFT(country_code, 2) WHERE LENGTH(country_code) > 2",
                    "UPDATE dim_users_table SET date_of_birth = NULL WHERE date_of_birth = 'KBTI';",
                    "ALTER TABLE dim_users_table ALTER COLUMN first_name TYPE VARCHAR(255) USING first_name::VARCHAR(255)",
                    "ALTER TABLE dim_users_table ALTER COLUMN last_name TYPE VARCHAR(255) USING last_name::VARCHAR(255)",
                    "ALTER TABLE dim_users_table ALTER COLUMN date_of_birth TYPE DATE USING to_date(date_of_birth, 'YYYY-MM-DD')",
                    "ALTER TABLE dim_users_table ALTER COLUMN country_code TYPE VARCHAR(3) USING country_code::VARCHAR(2)",
                    "ALTER TABLE dim_users_table ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID",
                    "ALTER TABLE dim_users_table ALTER COLUMN join_date TYPE DATE USING to_date(join_date, 'YYYY-MM-DD')",
                    "ALTER TABLE dim_users_table ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date"
                ])

            elif database_identifier == 'dim_store_details':
                sql_statements.extend([
                    "DELETE FROM dim_store_details WHERE LENGTH(locality) > 255",
                    "DELETE FROM dim_store_details WHERE LENGTH(store_code) > 11",
                    "UPDATE dim_store_details SET store_type = COALESCE(LEFT(store_type, 255), '') WHERE LENGTH(store_type) > 255 OR store_type IS NULL",
                    "DELETE FROM dim_store_details WHERE LENGTH(country_code) > 2",
                    "DELETE FROM dim_store_details WHERE LENGTH(continent) > 255",
                    "ALTER TABLE dim_store_details ALTER COLUMN longitude SET DATA TYPE FLOAT USING NULLIF(longitude, '')::FLOAT",
                    "ALTER TABLE dim_store_details ALTER COLUMN locality SET DATA TYPE VARCHAR(255) USING locality::VARCHAR(255)",
                    "ALTER TABLE dim_store_details ALTER COLUMN store_code SET DATA TYPE VARCHAR(11) USING store_code::VARCHAR(11)",
                    "ALTER TABLE dim_store_details ALTER COLUMN staff_numbers SET DATA TYPE SMALLINT",
                    "ALTER TABLE dim_store_details ALTER COLUMN opening_date TYPE DATE",
                    "ALTER TABLE dim_store_details ALTER COLUMN store_type TYPE VARCHAR(255)",
                    "ALTER TABLE dim_store_details ALTER COLUMN latitude SET DATA TYPE FLOAT USING NULLIF(longitude, '')::FLOAT",
                    "ALTER TABLE dim_store_details ALTER COLUMN country_code SET DATA TYPE VARCHAR(2) USING country_code::VARCHAR(2)",
                    "ALTER TABLE dim_store_details ALTER COLUMN continent SET DATA TYPE VARCHAR(255) USING continent::VARCHAR(255)"

                ])

            elif database_identifier == 'dim_products':
                sql_statements.extend([
                    "ALTER TABLE dim_products ALTER COLUMN product_price TYPE FLOAT USING NULLIF(replace(replace(product_price::text, '£', ''), ',', ''), '')::FLOAT",
                    "ALTER TABLE dim_products ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT",
                    "ALTER TABLE dim_products ALTER COLUMN \"EAN\" TYPE VARCHAR(13)",
                    "ALTER TABLE dim_products ALTER COLUMN product_code TYPE VARCHAR(11)",
                    "ALTER TABLE dim_products ALTER COLUMN date_added TYPE DATE USING date_added::DATE",
                    "ALTER TABLE dim_products ALTER COLUMN uuid TYPE UUID USING uuid::UUID",
                    "ALTER TABLE dim_products RENAME COLUMN removed TO still_available",
                    "ALTER TABLE dim_products ALTER COLUMN still_available TYPE BOOLEAN USING CASE WHEN still_available = 'Still_avaliable' THEN TRUE ELSE FALSE END"
                ])

            elif database_identifier == 'dim_date_times':
                sql_statements.extend([
                    "ALTER TABLE dim_date_times ALTER COLUMN month TYPE VARCHAR(12)",
                    "ALTER TABLE dim_date_times ALTER COLUMN year TYPE VARCHAR(30)",
                    "ALTER TABLE dim_date_times ALTER COLUMN day TYPE VARCHAR(31)",
                    "DELETE FROM dim_date_times WHERE LENGTH(time_period) > 5",
                    "DELETE FROM dim_date_times WHERE NOT (LENGTH(date_uuid) = 36 AND SUBSTRING(date_uuid FROM 9 FOR 1) = '-' AND SUBSTRING(date_uuid FROM 14 FOR 1) = '-' AND SUBSTRING(date_uuid FROM 19 FOR 1) = '-' AND SUBSTRING(date_uuid FROM 24 FOR 1) = '-')",
                    "ALTER TABLE dim_date_times ALTER COLUMN date_uuid TYPE UUID USING NULLIF(date_uuid, '')::UUID",
                ])

            elif database_identifier == 'dim_card_details':
                sql_statements.extend([
                    "UPDATE dim_card_details SET card_number = NULL WHERE card_number::text ~ '[^\d]+'",
                    "ALTER TABLE dim_card_details ALTER COLUMN card_number TYPE VARCHAR(16) USING LEFT(card_number::text, 16)",
                    "UPDATE dim_card_details SET date_payment_confirmed = CASE WHEN date_payment_confirmed::text ~ '^\s*$' THEN NULL ELSE date_payment_confirmed::DATE END",
                    "ALTER TABLE dim_card_details ALTER COLUMN card_number TYPE VARCHAR(16)",
                    "ALTER TABLE dim_card_details ALTER COLUMN expiry_date TYPE VARCHAR(5)",
                    "ALTER TABLE dim_card_details ALTER COLUMN date_payment_confirmed TYPE DATE"
                ])

            elif database_identifier == 'orders_table':
                sql_statements.extend([
                    "ALTER TABLE dim_users_table ADD PRIMARY KEY (user_uuid)",
                    "ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code)",
                    "ALTER TABLE dim_products ADD PRIMARY KEY (product_code)",
                    "ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid)",
                    "ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number)",
                    "ALTER TABLE orders_table ADD CONSTRAINT fk_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users_table(user_uuid)",
                    "ALTER TABLE orders_table ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code)",
                    "ALTER TABLE orders_table ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products(product_code)",
                    "ALTER TABLE orders_table ADD CONSTRAINT fk_date_uuid FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid)",
                    "ALTER TABLE orders_table ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number)"
                ])

            for sql_statement in sql_statements:
                print(f"Executing SQL statement: {sql_statement}")
                connection.execute(text(sql_statement))
                print("Statement executed successfully")

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


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description="Retrieve and process data from a PDF linked in a URL.")
#     parser.add_argument("url", type=str, help="URL of the PDF")
#     args = parser.parse_args()

#     db_credentials = {
#         'config_file_path': r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\db_creds.yml"
#     }

#     db_connector = DatabaseConnector(**db_credentials)
#     db_connector.upgrade_schema()

#     data_cleaner_instance = DataCleaning()

#     result_df = db_connector.retrieve_pdf_data(args.url)

#     if not result_df.empty:
#         print("Original DataFrame:")
#         print(result_df.head())

#         cleaned_data_df = data_cleaner_instance.clean_user_data(result_df)
#         print("Cleaned DataFrame:")
#         print(cleaned_data_df.head())

#         # Continue with other cleaning steps...

#         uploader = DatabaseConnector(db_connector.db_engine)
#         uploader.upload_to_db(cleaned_data_df, 'dim_users')
#     else:
#         print("No data retrieved from the PDF.")
