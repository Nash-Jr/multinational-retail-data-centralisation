
from DatabaseConnector import DatabaseConnector
from data_extraction import DataExtractor
import requests
import argparse


parser = argparse.ArgumentParser(
    description="Retrieve and process data from a PDF linked in a URL.")
parser.add_argument("url", type=str, help="URL of the PDF")
args = parser.parse_args()


db_credentials = {
    'config_file_path': r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\db_creds.yml"
}
db_connector = DatabaseConnector(**db_credentials)


result_df = db_connector.retrieve_pdf_data(args.url)

if not result_df.empty:
    print(result_df)

data_extractor = DataExtractor(db_connector)

number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

response = requests.get(number_of_stores_endpoint, headers=headers)
try:
    response.raise_for_status()
    if response.status_code == 200:
        number_of_stores = response.json()
        print(number_of_stores)
    else:
        print(
            f"Failed to retrieve the number of stores. Status code: {response.status_code}")
except requests.exceptions.HTTPError as err:
    print(f"HTTP error occurred: {err}")
except Exception as e:
    print(f"An error occurred: {e}")

tables = data_extractor.list_db_tables()

# Print the table names
print("Available tables:")
for table in tables:
    print(table)

my_instance = DataExtractor(db_connector)
json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
sales_date_df = my_instance.retrieve_sales_date(json_url)
if sales_date_df is not None:
    print(sales_date_df)

# Assuming 'result' is the DataFrame you want to assign
data_extractor.df = result_df
data_extractor.extract_df = data_extractor.extract_from_s3(
    's3://data-handling-public/products.csv')
data_extractor.stores_dataframe = data_extractor.retrieve_stores_data(
    'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}')
data_extractor.final_df = data_extractor.read_rds_table('orders_table')

# Print the attributes
print(data_extractor.df)
print(data_extractor.extract_df)
print(data_extractor.stores_dataframe)
print(data_extractor.final_df)
