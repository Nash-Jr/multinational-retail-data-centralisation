from DatabaseConnector import DatabaseConnector
from data_extraction import DataExtractor
import requests  # Make sure to import the requests module

# Replace these values with your actual database credentials
db_connector = DatabaseConnector(
    host='data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com',
    database='postgres',
    user='aicore_admin',
    password='AiCore2022',
    port=5432
)

# Now, create an instance of DataExtractor with the db_connector argument
data_extractor = DataExtractor(db_connector)

# Define the number_of_stores_endpoint variable
number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

# Make the API request
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
