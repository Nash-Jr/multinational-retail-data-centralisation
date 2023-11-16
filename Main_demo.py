from DatabaseConnector import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Create an instance of DatabaseConnector from the YAML file
db_connector = DatabaseConnector.from_yaml(
    r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\.gitignore\db_creds.yml")

# Create an instance of DataExtractor with the db_connector argument
data_extractor = DataExtractor(db_connector)

# Now you can use data_extractor for further operations
number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
result = data_extractor.list_number_of_stores(
    number_of_stores_endpoint, headers)
print(result)
