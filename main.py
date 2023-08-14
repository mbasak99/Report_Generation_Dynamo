import sys
import pprint
from dynamo_functions import DynamoFunctions
from load_csv_data import LoadCSVData
from table_loader import TableLoader
from missing_info import MissingInfo
from report_gen import ReportGenerator

# THIS FILE IS MAINLY FOR TESTING PURPOSES

if __name__ == "__main__":
    dynamo = DynamoFunctions()
    dynamo.setup()
    # dynamo.display_data_from_table("mbasak_country_data", "Canada")
    # dynamo.add_record({"Country": "Test2", "ISO3": "TST3"}, "mbasak_country_data")
    # dynamo.delete_record({"Country": "Canada", "ISO3": "CAN"}, "mbasak_country_data")

    # create table
    # dynamo.create_table(
    #     "country_population", [{"AttributeName": "year", "KeyType": "HASH"}, {"AttributeName": "title", "KeyType": "RANGE"}], [{"AttributeName": "year", "AttributeType": "N"}, {"AttributeName": "title", "AttributeType": "S"}])

    # deleete table
    # dynamo.delete_table("should_fail")

    # proper delete table
    # dynamo.delete_table("mbasak_country_data")
    # dynamo.delete_table("mbasak_economic_data")
    # dynamo.delete_table("mbasak_population_data")
    # sys.exit()

    # csv = LoadCSVData()
    # csv.create_country_data()
    # loader = TableLoader()
    # loader.load_country_data()
    # loader.load_country_economic_data()
    # loader.load_country_population_data()
    # pprint.pprint(dynamo.display_data_from_table(
    #     "mbasak_population_data",  "Country"))
    # print(dynamo.query_table("mbasak_economic_data",
    #       {"key": "Country", "value": "Canada"}))

    # report = ReportGenerator()
    # generate population density report for country
    # report.generate_population_report("Canada")

    # generate economic report for country
    # report.generate_economic_report("Bosnia and Herzegovina")

    # report.generate_country_level_report("Canada")

    # generate global population report for specified year
    # report.generate_global_level_report(1970)

    # generate global gdppc for all countries for every decade
    # TODO
    # info = MissingInfo()
    # info.add_missing_info()
