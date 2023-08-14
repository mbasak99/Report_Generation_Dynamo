"""
Monark Basak
1056289
mbasak@uoguelph.ca
"""

import sys

from dynamo_functions import DynamoFunctions
from load_csv_data import LoadCSVData


class TableLoader:
    def __init__(self):
        self.dynamo = DynamoFunctions()
        self.dynamo.setup()
        self.csv_parser = LoadCSVData()

    def start(self):
        # call missing_info script

        return

    def load_country_data(self):
        table_name = "mbasak_country_data"
        # create a table
        table = self.dynamo.create_table(
            table_name,
            [
                {"AttributeName": "Country", "KeyType": "HASH"},
                {"AttributeName": "ISO3", "KeyType": "RANGE"},
            ],
            [
                {"AttributeName": "Country", "AttributeType": "S"},
                {"AttributeName": "ISO3", "AttributeType": "S"},
            ],
        )

        if table in [1, -1]:
            print("Failed to load country data into DynamoDB.")
            return 1

        # get a list of dicts from load_csv_data
        country_data = self.csv_parser.create_country_data()
        # print(country_data)

        # load the data into table
        if self.dynamo.load_records(table_name, country_data) == 1:
            sys.exit(1)

        print(f"Loaded data to {table_name} successfully.")

        return 0

    def load_country_population_data(self):
        table_name = "mbasak_population_data"

        # create a table
        table = self.dynamo.create_table(
            table_name,
            [
                {"AttributeName": "Country", "KeyType": "HASH"},
                {"AttributeName": "ISO3", "KeyType": "RANGE"},
            ],
            [
                {"AttributeName": "Country", "AttributeType": "S"},
                {"AttributeName": "ISO3", "AttributeType": "S"},
            ],
        )

        if table in [1, -1]:
            print("Failed to load population data into DynamoDB.")
            return 1

        # get a list of dicts from load_csv_data
        population_data = self.csv_parser.create_country_population()
        # print(population_data)

        # load the data into table
        if self.dynamo.load_records(table_name, population_data) == 1:
            sys.exit(1)

        print(f"Loaded data to {table_name} successfully.")

        return 0

    def load_country_economic_data(self):
        table_name = "mbasak_economic_data"

        # create a table
        table = self.dynamo.create_table(
            table_name,
            [
                {"AttributeName": "Country", "KeyType": "HASH"},
                {"AttributeName": "Currency", "KeyType": "RANGE"},
            ],
            [
                {"AttributeName": "Country", "AttributeType": "S"},
                {"AttributeName": "Currency", "AttributeType": "S"},
            ],
        )

        if table in [1, -1]:
            print("Failed to load economic data into DynamoDB.")
            return 1

        # get a list of dicts from load_csv_data
        economic_data = self.csv_parser.create_country_economic()
        # print(economic_data)

        # load the data into table
        if self.dynamo.load_records(table_name, economic_data) == 1:
            sys.exit(1)

        print(f"Loaded data to {table_name} successfully.")

        return 0
