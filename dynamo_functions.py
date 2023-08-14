"""
Monark Basak
1056289
mbasak@uoguelph.ca
"""

import configparser
import os
import sys

import boto3
from boto3.dynamodb.conditions import Key, Attr
import botocore


class DynamoFunctions:
    def __init__(self) -> None:
        # create boto3 resource
        self.session = None
        self.dynamo = None
        self.dynamo_res = None
        self.tables = {}

    def debug(self, print_s: str):
        print(print_s)

    def table_exists(self, table_name: str):
        # check if data exists in dynamo
        try:
            self.dynamo.describe_table(TableName=table_name)
            # checks if table exists, if not, goes to exception
            self.dynamo_res.Table(table_name)
        except self.dynamo.exceptions.ResourceNotFoundException:
            return False
        except Exception as e:
            return False

        return True

    # load the config file
    def setup(self):
        try:
            config_file = [
                file for file in os.listdir(".") if file.lower() == "config.conf"
            ]

            if len(config_file) == 0:
                print("Config file is missing!")
                sys.exit(1)

            # grab aws key id and access key
            config_parser = configparser.ConfigParser()
            config_parser.read(config_file)

            # set the secrets
            self.aws_access_key_id = config_parser["default"]["aws_access_key_id"]
            self.aws_secret_access_key = config_parser["default"][
                "aws_secret_access_key"
            ]
        except:
            print("Something went wrong reading the config file.")
            return

        # make session connection
        self.create_dynamo_session()

        return

    def create_dynamo_session(self):
        try:
            self.session = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name="ca-central-1",
            )
            self.dynamo = self.session.client("dynamodb")
            self.dynamo_res = self.session.resource("dynamodb")
            # self.debug("Successfully connected to DynamoDB.")
        except Exception as e:
            # invalid
            print("Something went wrong connecting to DynamoDB.")
            # self.debug(e)
            sys.exit(1)

        return 0

    # create table
    def create_table(self, table_name: str, table_schema: list, attribute_def: list):
        try:
            table = None

            # create table
            table = self.dynamo_res.create_table(
                TableName=table_name,
                KeySchema=table_schema,
                AttributeDefinitions=attribute_def,
                ProvisionedThroughput={
                    "ReadCapacityUnits": 10,
                    "WriteCapacityUnits": 10,
                },
            )
            table.wait_until_exists()
            # self.debug(f"Table {table_name} made.")
            self.tables.update({table_name: table})
        except self.dynamo.exceptions.ResourceInUseException:
            print(f"Table {table_name} already exists!")
            return 1
        except Exception as e:
            print("Couldn't create Dynamo table.")
            # self.debug(e)

            return -1
        else:
            return table

    # delete table
    def delete_table(self, table_name: str):
        try:
            table = None
            self.dynamo.describe_table(TableName=table_name)
            # checks if table exists, if not, goes to exception
            table = self.dynamo_res.Table(table_name)
            table.delete()
            table.wait_until_not_exists()
            print(f"Successfully deleted {table_name}.")
        except self.dynamo.exceptions.ResourceNotFoundException:
            print(f"Table {table_name} doesn't exist!")
            return -1
        except Exception as e:
            print("An error occurred trying to delete the table.")
            # self.debug(e)

            return -1
        return 0

    # load records
    def load_records(self, table_name: str, table_data: list):
        try:
            # checks if table exists, if not, goes to exception
            self.dynamo.describe_table(TableName=table_name)
            table = self.dynamo_res.Table(table_name)
            # print(table.creation_date_time)

            # batch write the data into table
            with table.batch_writer() as batch:
                for item in table_data:
                    # print(item)
                    batch.put_item(Item=item)
        except self.dynamo.exceptions.ResourceNotFoundException:
            print(f"Table {table_name} doesn't exist!")
            return 1
        except Exception as e:
            print("Failed to load records into Dynamo.")
            # self.debug(e)
            return -1
        return 0

    # add record to table
    def add_record(self, record: dict, table_name: str):
        try:
            # checks if table exists, if not, goes to exception
            self.dynamo.describe_table(TableName=table_name)
            table = self.dynamo_res.Table(table_name)
            table.put_item(Item=record)
        except self.dynamo.exceptions.ResourceNotFoundException:
            print(f"Table {table_name} doesn't exist!")
            return -1
        except Exception as e:
            print(f"Error trying to add record to table {table_name}.")
            # self.debug(e)
            return -1
        return 0

    # delete record from table
    def delete_record(self, record: dict, table_name: str):
        try:
            # checks if table exists, if not, goes to exception
            self.dynamo.describe_table(TableName=table_name)
            table = self.dynamo_res.Table(table_name)
            table.delete_item(Key=record)
        except self.dynamo.exceptions.ResourceNotFoundException:
            print(f"Table {table_name} doesn't exist!")
            return -1
        except Exception as e:
            print(f"Error trying to delete record from table {table_name}.")
            # self.debug(e)
            return -1
        return 0

    def update_record(
        self,
        record: dict,
        new_record_value: dict,
        update_expression: str,
        table_name: str,
    ):
        try:
            # checks if table exists, if not, goes to exception
            self.dynamo.describe_table(TableName=table_name)
            table = self.dynamo_res.Table(table_name)

            table.update_item(
                Key=record,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=new_record_value,
            )
        except self.dynamo.exceptions.ResourceNotFoundException:
            print(f"Table {table_name} doesn't exist!")
            return -1
        except Exception as e:
            print(f"Error trying to update record from table {table_name}.")
            # self.debug(e)
            return -1
        return 0

    # dump/display all data in a selected table
    def display_data_from_table(self, table_name: str, key: str = "Country"):
        try:
            # checks if table exists, if not, goes to exception
            self.dynamo.describe_table(TableName=table_name)
            table = self.dynamo_res.Table(table_name)

            # get items from table
            response = table.scan(FilterExpression=Attr(key).exists())

            return response["Items"]
        except self.dynamo.exceptions.ResourceNotFoundException:
            print(f"Table {table_name} doesn't exist!")
            return -1
        except Exception as e:
            print("Failed to display data from table.")
            # self.debug(e)
            return -1

    # query module builds reports
    def query_table(self, table_name: str, query: dict):
        try:
            # checks if table exists, if not, goes to exception
            self.dynamo.describe_table(TableName=table_name)
            table = self.dynamo_res.Table(table_name)

            # get items from table
            response = table.query(
                KeyConditionExpression=Key(query["key"]).eq(query["value"])
            )

            return response["Items"]
        except self.dynamo.exceptions.ResourceNotFoundException:
            print(f"Table {table_name} doesn't exist!")
            return -1
        except Exception as e:
            print("Failed to query data from table.")
            # self.debug(e)
            return -1
