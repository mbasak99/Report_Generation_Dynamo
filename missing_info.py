"""
Monark Basak
1056289
mbasak@uoguelph.ca
"""

from decimal import Decimal

from prettytable import PrettyTable

from dynamo_functions import DynamoFunctions
from report_gen import ReportGenerator
from table_loader import TableLoader


class MissingInfo:
    def __init__(self):
        self.dynamo = DynamoFunctions()
        self.dynamo.setup()
        self.loader = TableLoader()

    def country_report(self):
        try:
            # check if data exists in dynamo
            table_exists = self.dynamo.table_exists("mbasak_country_data")

            # if table doesn't exist, create it and then add the missing data
            if table_exists == False:
                self.loader.load_country_data()

            table_exists = self.dynamo.table_exists("mbasak_economic_data")

            # if table doesn't exist, create it and then add the missing data
            if table_exists == False:
                self.loader.load_country_economic_data()

            table_exists = self.dynamo.table_exists("mbasak_population_data")

            # if table doesn't exist, create it and then add the missing data
            if table_exists == False:
                self.loader.load_country_population_data()

            print("Please choose a country from the given table:")
            country_data = self.dynamo.display_data_from_table("mbasak_country_data")

            table_print = PrettyTable()
            countries = [x["Country"] for x in country_data]
            table_print.add_column("Country", countries)
            print(table_print)

            user_input = None
            while user_input not in countries:
                user_input = input("Please enter a country (default Canada): ")

                if len(user_input) == 0:
                    break

            reports = ReportGenerator()
            if reports.generate_country_level_report(user_input or "Canada") != 0:
                return -1
        except Exception as e:
            print("Something went wrong generating country reports.")
            return -1

        return 0

    def global_report(self):
        try:
            # check if data exists in dynamo
            table_exists = self.dynamo.table_exists("mbasak_country_data")

            # if table doesn't exist, create it and then add the missing data
            if table_exists == False:
                self.loader.load_country_data()

            table_exists = self.dynamo.table_exists("mbasak_economic_data")

            # if table doesn't exist, create it and then add the missing data
            if table_exists == False:
                self.loader.load_country_economic_data()

            table_exists = self.dynamo.table_exists("mbasak_population_data")

            # if table doesn't exist, create it and then add the missing data
            if table_exists == False:
                self.loader.load_country_population_data()

            user_input = "-1"
            while user_input.isnumeric() == False or int(user_input) not in [
                x for x in range(1970, 2020)
            ]:
                user_input = input(
                    "Please choose a year from 1970-2019 (default 1970): "
                )

                if user_input == "":
                    break

            reports = ReportGenerator()
            reports.generate_global_level_report(user_input or 1970)
        except Exception as e:
            print("Something went wrong generating global reports.")
            return -1

        return 0

    def missing_country_data(self):
        table_name = "mbasak_country_data"

        # check if data exists in dynamo
        table_exists = self.dynamo.table_exists(table_name)

        # if table doesn't exist, create it and then add the missing data
        if table_exists == False:
            self.loader.load_country_data()

        # retrieve information so user can choose which country
        country_data = self.dynamo.display_data_from_table(table_name)
        countries = [x["Country"] for x in country_data]
        isos = [x["ISO3"] for x in country_data]
        table_print = PrettyTable()
        table_print.field_names = ["Country", "ISO", "Languages"]
        table_print.add_rows(
            [[x["Country"], x["ISO3"], x["Languages"]] for x in country_data]
        )
        print(table_print)

        print(
            "Please enter the Country Name and ISO3 for the Country you want to modify."
        )

        country = input("Country: ")
        iso = input("ISO3: ")
        while country not in countries or iso not in isos:
            print("Enter a valid Country and/or ISO3.")
            country = input("Country: ")
            iso = input("ISO3: ")

        language = input(f"Please enter the language(s) for {country}: ")

        if (
            self.dynamo.update_record(
                {"Country": country, "ISO3": iso},
                {":language": language},
                "SET Languages = :language",
                table_name,
            )
            != 0
        ):
            print(f"Failed to update language for {country}.")
            return -1

        print(f"Successfully updated language for {country}.")

        return 0

    def missing_economic_data(self):
        table_name = "mbasak_economic_data"

        # check if data exists in dynamo
        table_exists = self.dynamo.table_exists(table_name)

        # if table doesn't exist, create it and then add the missing data
        if table_exists == False:
            self.loader.load_country_economic_data()

        # retrieve information so user can choose which country
        economic_data = self.dynamo.display_data_from_table(table_name)
        countries = [x["Country"] for x in economic_data]
        currencies = [x["Currency"] for x in economic_data]

        table_print = PrettyTable()
        table_print.field_names = ["Country", "Currency"]
        table_print.add_rows([[x["Country"], x["Currency"]] for x in economic_data])
        print(table_print)

        print(
            "Please enter the Country Name and Currency for the Country you want to modify."
        )

        country = input("Country: ")
        currency = input("Currency: ")
        while country not in countries or currency not in currencies:
            print("Enter a valid Country and/or Currency.")
            country = input("Country: ")
            currency = input("Currency: ")

        year = input(f"Please enter the Year for {country}'s GDPPC: ")
        gdppc = input(f"Please enter the GDPPC for {country}: ")

        index = 0
        for data in economic_data:
            if data["Country"] == country:
                index = economic_data.index(data)

        years = [x for x in list(economic_data[index]["Economy"].keys())]
        while year not in years or gdppc.isnumeric() == False:
            print("Enter a valid Year and/or GDPPC.")
            year = input(f"Please enter the Year for {country}'s GDPPC: ")
            gdppc = input(f"Please enter the GDPPC for {country}: ")

        country_to_update = [
            x["Economy"]
            for x in economic_data
            if x["Country"] == country and x["Currency"] == currency
        ]

        country_to_update[0][year] = Decimal(gdppc)

        if (
            self.dynamo.update_record(
                {"Country": country, "Currency": currency},
                {":economy": country_to_update[0]},
                "SET Economy = :economy",
                table_name,
            )
            != 0
        ):
            print(f"Failed to update economy for {country}.")
            return -1

        print(f"Successfully updated economy for {country}.")

        return 0

    def missing_population_data(self):
        table_name = "mbasak_population_data"

        # check if data exists in dynamo
        table_exists = self.dynamo.table_exists(table_name)

        # if table doesn't exist, create it and then add the missing data
        if table_exists == False:
            self.loader.load_country_population_data()

        # retrieve information so user can choose which country
        population_data = self.dynamo.display_data_from_table(table_name)
        countries = [x["Country"] for x in population_data]
        isos = [x["ISO3"] for x in population_data]

        table_print = PrettyTable()
        table_print.field_names = ["Country", "ISO3"]
        table_print.add_rows([[x["Country"], x["ISO3"]] for x in population_data])
        print(table_print)

        print(
            "Please enter the Country Name and Currency for the Country you want to modify."
        )

        country = input("Country: ")
        iso = input("ISO3: ")
        while country not in countries or iso not in isos:
            print("Enter a valid Country and/or ISO3.")
            country = input("Country: ")
            iso = input("ISO3: ")

        year = input(f"Please enter the Year for {country}'s Population: ")
        population = input(f"Please enter the Population for {country}: ")

        index = 0
        for data in population_data:
            if data["Country"] == country:
                index = population_data.index(data)

        years = [x for x in list(population_data[index]["Population"].keys())]
        while year not in years or population.isnumeric() == False:
            print("Enter a valid Year and/or Population.")
            year = input(f"Please enter the Year for {country}'s Population: ")
            population = input(f"Please enter the Population for {country}: ")

        country_to_update = [
            x["Population"]
            for x in population_data
            if x["Country"] == country and x["ISO3"] == iso
        ]

        country_to_update[0][year] = Decimal(population)

        if (
            self.dynamo.update_record(
                {"Country": country, "ISO3": iso},
                {":population": country_to_update[0]},
                "SET Population = :population",
                table_name,
            )
            != 0
        ):
            print(f"Failed to update population for {country}.")
            return -1

        print(f"Successfully updated population for {country}.")

        return 0

    def add_missing_info(self):
        user_input = input("Would you like to add missing information? (Y/N): ").lower()

        while user_input not in ["y", "n"]:
            user_input = input("Please re-enter your choice: ").lower()

        if user_input == "y":
            print(
                """
Here are your options to add missing information:
1. Modify the Language(s) in a Country
2. Modify the GDPPC of a Country
3. Modify the Population of a Country.
"""
            )
            user_input = input("Please enter your choice: ")
            while int(user_input) not in [x for x in range(1, 4)]:
                user_input = input("Please choose a valid option: ")

            if int(user_input) == 1:
                self.missing_country_data()
            elif int(user_input) == 2:
                self.missing_economic_data()
            elif int(user_input) == 3:
                self.missing_population_data()
        elif user_input == "n":
            print(
                """
Which reports would you like printed?
1. Country Report
2. Global Report
3. Country and Global Report"""
            )
            user_input = ""
            while user_input not in [str(x) for x in range(1, 4)]:
                user_input = input("Please enter your choice: ")

            if int(user_input) == 1:
                # country
                self.country_report()
            elif int(user_input) == 2:
                self.global_report()
            elif int(user_input) == 3:
                self.country_report()
                print("")
                self.global_report()
        return 0


if __name__ == "__main__":
    info = MissingInfo()
    info.add_missing_info()
