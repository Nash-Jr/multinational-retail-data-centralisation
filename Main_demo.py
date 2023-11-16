# Main_demo.py

from DatabaseConnector import DatabaseConnector
from data_extraction import DataExtractor

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

# Continue with the rest of your code...
number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
result = data_extractor.retrieve_stores_data(number_of_stores_endpoint)

print(result)
