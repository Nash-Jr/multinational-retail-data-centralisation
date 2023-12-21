from DatabaseConnector import DatabaseConnector
from data_extraction import DataExtractor
import requests
import argparse
from data_cleaning import DataCleaning


db_credentials = {
    'config_file_path': r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\db_creds.yml"
}
db_connector = DatabaseConnector(**db_credentials)
db_local_con = DatabaseConnector(
    config_file_path=r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\cred_local.yml")

data_cleaner = DataCleaning()
data_extractor = DataExtractor(db_connector)

# User data
df_from_db = data_extractor.read_rds_table('legacy_users')
cleaned_user_data = data_cleaner.clean_user_data(df_from_db)
db_local_con.test_db_upload(cleaned_user_data, 'dim_users_table')

# card_data
pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
df_from_pdf = data_extractor.retrieve_pdf_data(pdf_url)
cleaned_card_data = data_cleaner.clean_card_data(df_from_pdf)
db_local_con.test_db_upload(cleaned_card_data, 'dim_card_details')

# store_data
number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
number_of_stores = data_extractor.list_number_of_stores(
    number_of_stores_endpoint)
if number_of_stores is not None:
    data_extractor_data = data_extractor.retrieve_stores_data(
        'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}',
        headers,
        number_of_stores
    )
else:
    print("Failed to retrieve the number of stores.")
cleaned_store_data = data_cleaner.clean_store_data(
    data_extractor_data)
db_local_con.test_db_upload(cleaned_store_data, 'dim_store_details')
# products data
s3_adress = 's3://data-handling-public/products.csv'
df_from_extractor = data_extractor.extract_from_s3(s3_adress)
cleaned_products_data = data_cleaner.clean_products_data(df_from_extractor)
db_local_con.test_db_upload(cleaned_products_data, 'dim_products')

# orders data
df_from_db2 = data_extractor.read_rds_table('orders_table')
cleaned_orders_data = data_cleaner.clean_orders_data(df_from_db2)
db_local_con.test_db_upload(cleaned_orders_data, 'orders_table')

# date times
json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
df_from_s3 = data_extractor.retrieve_sales_date(json_url)
cleaned_date_times = data_cleaner.clean_sales_date(df_from_s3)
db_local_con.test_db_upload(cleaned_date_times, 'dim_date_times')

# sql data types change
db_local_con.change_datatype('orders_table')
db_local_con.change_datatype('dim_users_table')
db_local_con.change_datatype('dim_store_details')
db_local_con.change_datatype('dim_products')
db_local_con.change_datatype('dim_date_times')
db_local_con.change_datatype('dim_card_details')
