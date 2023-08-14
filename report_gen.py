"""
Monark Basak
1056289
mbasak@uoguelph.ca
"""

import operator
import pprint

from prettytable import PrettyTable
from tabulate import tabulate

from dynamo_functions import DynamoFunctions


class ReportGenerator:
    def __init__(self):
        self.dynamo_funcs = DynamoFunctions()
        self.dynamo_funcs.setup()
        # self.colour = {
        #     "PURPLE": "\033[95m",
        #     "CYAN": "\033[96m",
        #     "DARKCYAN": "\033[36m",
        #     "BLUE": "\033[94m",
        #     "GREEN": "\033[92m",
        #     "YELLOW": "\033[93m",
        #     "RED": "\033[91m",
        #     "BOLD": "\033[1m",
        #     "UNDERLINE": "\033[4m",
        #     "END": "\033[0m",
        # }

    def sort(*item):
        return item[1]["Area"]

    def sorted_country_population_and_density_rank(
        self, population_data, country_data
    ) -> dict:
        sorted_country_population_rank = {}
        sorted_country_population_density_rank = {}

        # create a list of population
        country_populations = {}

        for row in population_data:
            curr_country_data = [x for x in country_data if x["ISO3"] == row["ISO3"]][0]

            for population in row["Population"].items():
                temp = (
                    row["Country"],
                    population[1],
                    round(population[1] / curr_country_data["Area"], 2),
                )

                if population[0] not in country_populations:
                    country_populations[population[0]] = [temp]
                else:
                    country_populations[population[0]].append(temp)

        sorted_country_population_rank = {}
        sorted_country_population_density_rank = {}

        for year_population in country_populations.items():
            year = year_population[0]
            population_in_year = year_population[1]

            # sort on secondary key first (ISO3), then population
            _sorted = sorted(population_in_year, key=operator.itemgetter(0))
            _sorted = sorted(_sorted, key=operator.itemgetter(1), reverse=True)
            sorted_country_population_rank[year] = _sorted

        for year_population_density in country_populations.items():
            year = year_population_density[0]
            population_density_in_year = year_population_density[1]

            # sort on secondary key first (Country), then population density
            _sorted = sorted(population_density_in_year, key=operator.itemgetter(0))
            _sorted = sorted(_sorted, key=operator.itemgetter(2), reverse=True)
            sorted_country_population_density_rank[year] = _sorted

        # sort the years of dictionary
        temp_keys = list(sorted_country_population_rank.keys())
        temp_keys.sort()
        sorted_country_population_rank = {
            i: sorted_country_population_rank[i] for i in temp_keys
        }

        temp_keys = list(sorted_country_population_density_rank.keys())
        temp_keys.sort()
        sorted_country_population_density_rank = {
            i: sorted_country_population_density_rank[i] for i in temp_keys
        }

        return {
            "Population": sorted_country_population_rank,
            "Density": sorted_country_population_density_rank,
        }

    def sorted_country_area_rank(self, country_data) -> list:
        return sorted(country_data, key=self.sort, reverse=True)

    def sort_country_economic_rank(self, economic_data, country: str = None) -> dict:
        sorted_country_economic_rank = {}

        # create a list of rank
        countries_to_rank = {}

        # iterate over list and initialize rank
        country_currency = None
        for row in economic_data:
            if country != None and row["Country"] == country:
                # store currency for use during report generation
                country_currency = row["Currency"]

            # insert countries' GDPPC by year
            for gdp in row["Economy"].items():
                temp = (row["Country"], float(gdp[1]))

                if gdp[0] not in countries_to_rank:
                    countries_to_rank[gdp[0]] = [temp]
                else:
                    countries_to_rank[gdp[0]].append(temp)

        # sort each country's GDPPC for each year
        # the index of the country in the year is its rank during the year
        sorted_country_economic_rank = {}
        for year_gdppc in countries_to_rank.items():
            year = year_gdppc[0]
            gdppc_in_year = year_gdppc[1]

            # sort on secondary key first (country name), then gdppc
            _sorted = sorted(gdppc_in_year, key=operator.itemgetter(0))
            _sorted = sorted(_sorted, key=operator.itemgetter(1), reverse=True)
            sorted_country_economic_rank[year] = _sorted

        # sort the years of dictionary
        temp_keys = list(sorted_country_economic_rank.keys())
        temp_keys.sort()
        sorted_country_economic_rank = {
            i: sorted_country_economic_rank[i] for i in temp_keys
        }

        return {"Economic": sorted_country_economic_rank, "Currency": country_currency}

    def generate_global_level_report(self, year: int = 1970):
        if year == None:
            print("No year provided!")
            return -1

        # get data from dynamo
        population_data = self.dynamo_funcs.display_data_from_table(
            "mbasak_population_data"
        )
        country_data = self.dynamo_funcs.display_data_from_table("mbasak_country_data")
        economic_data = self.dynamo_funcs.display_data_from_table(
            "mbasak_economic_data"
        )

        # sort country data by area for world ranking
        country_data = self.sorted_country_area_rank(country_data)

        try:
            print("|GLOBAL LEVEL REPORT|")
            print(f"Year: {year}")
            print(f"Number of countries: {len(country_data)}")
            print("")

            # global population
            sorted_population_and_density = (
                self.sorted_country_population_and_density_rank(
                    population_data, country_data
                )
            )
            self.generate_global_population_report(
                sorted_population_and_density["Population"][str(year)]
            )
            print("")
            # if ()
            # print(sorted_population)
            # global area
            self.generate_global_area_report(country_data)
            print("")
            # global population density
            self.generate_global_population_density_report(
                sorted_population_and_density["Density"][str(year)]
            )
            print("")
            # global economic data
            sorted_economic_data = self.sort_country_economic_rank(economic_data)[
                "Economic"
            ]
            country_data = sorted(country_data, key=lambda x: x["Country"])
            self.generate_global_economic_report(sorted_economic_data, country_data)
        except Exception as e:
            print("Year provided doesn't!")
            # print(f"DEBUG: {e}")
            return -1

        return 0

    def generate_global_population_report(self, population_data):
        table_print = PrettyTable()
        table_print.field_names = ["Country Name", "Population", "Rank"]

        for index, item in enumerate(population_data):
            table_print.add_row([item[0], "{:,}".format(item[1]), index + 1])

        print("Table of Countries Ranked by Population (largest to smallest)")
        print(table_print)

        return 0

    def generate_global_area_report(self, country_data):
        table_print = PrettyTable()
        table_print.field_names = ["Country Name", "Area", "Rank"]

        for index, item in enumerate(country_data):
            table_print.add_row(
                [item["Country"], "{:,}".format(item["Area"]), index + 1]
            )

        print("Table of Countries Ranked by Area (largest to smallest)")
        print(table_print)

        return 0

    def generate_global_population_density_report(self, population_data):
        table_print = PrettyTable()
        table_print.field_names = ["Country Name", "Density (people/sq. km)", "Rank"]

        for index, item in enumerate(population_data):
            table_print.add_row([item[0], "{:,.2f}".format(item[2]), index + 1])

        print("Table of Countries Ranked by Density (largest to smallest)")
        print(table_print)

        return 0

    def generate_global_economic_report(self, economic_data, country_data):
        table_print = PrettyTable()

        # find all the decades for the table
        decades = []
        for year in list(economic_data.keys()):
            decade_last_partition = int(str(year)[-2:]) - (int(str(year)[-2:]) % 10)
            decade_first_partition = str(year)[:2]
            full_decade = (
                decade_first_partition + str(decade_last_partition)
                if len(str(decade_last_partition)) > 1
                else f"{decade_first_partition + str(decade_last_partition)}0"
            )

            if full_decade not in decades:
                decades.append(full_decade)

        # print(decades)

        print("GDP Per Capita for All Countries")

        # iterate through all the decades and create a table for each
        for decade in decades:
            table_print.clear()
            table_print.add_column(
                "Country Name", [item["Country"] for item in country_data]
            )
            print(f"{decade}'s Table")

            for year in range(int(decade), int(decade) + 10):
                economic_data[f"{year}"] = sorted(
                    economic_data[f"{year}"], key=lambda x: x[0]
                )
                table_print.add_column(
                    str(year),
                    [
                        data[1] if data[1] != 0 else ""
                        for data in economic_data[f"{year}"]
                    ],
                )

            print(table_print)
            print("")

        return 0

    def generate_country_level_report(self, country: str = "Canada"):
        if country == None:
            print("No Country provided!")
            return -1
        # get data from dynamo
        population_data = self.dynamo_funcs.display_data_from_table(
            "mbasak_population_data"
        )
        country_data = self.dynamo_funcs.display_data_from_table("mbasak_country_data")
        economic_data = self.dynamo_funcs.display_data_from_table(
            "mbasak_economic_data"
        )

        # sort country data by area for world ranking
        country_data = self.sorted_country_area_rank(country_data)

        # country general info
        country_area = None
        country_languages = None
        country_capital = None
        country_official = None
        country_area_ranking = -1

        for row in country_data:
            # print(row)
            if row["Country"] == country:
                country_area = row["Area"]
                country_languages = row["Languages"]
                country_capital = row["Capital"]
                country_official = row["OfficialName"]
                country_area_ranking = country_data.index(row) + 1

        if None in [country_area, country_languages, country_capital, country_official]:
            print("Country specified doesn't exist!")
            return -1

        # make country report header
        # tprint(country, font="wizard")
        print("|COUNTRY LEVEL REPORT|")
        print(country)
        print(f"[Official Name: {country_official}]")
        print("")
        print(
            tabulate(
                [
                    [f"Area: {country_area} sq. km ({country_area_ranking})"],
                    [
                        f"""
Official/National Languages: {country_languages.replace(",", ", ")}
Capital City: {country_capital}
"""
                    ],
                ],
                tablefmt="grid",
            )
        )

        self.generate_population_report(country, population_data, country_data)
        print("")
        self.generate_economic_report(country, economic_data)
        return 0

    def generate_population_report(self, country: str, population_data, country_data):
        table_print = PrettyTable()

        # set columns
        table_print.field_names = [
            "Year",
            "Population",
            "Rank_Pop",
            "Population Density (people/sq. km)",
            "Rank_Den",
        ]

        # get sorted population data back
        return_data = self.sorted_country_population_and_density_rank(
            population_data, country_data
        )
        sorted_countries_to_rank_population = return_data["Population"]
        sorted_countries_to_rank_population_density = return_data["Density"]

        row_data_list = []

        # pprint.pprint(sorted_countries_to_rank_population)
        # pprint.pprint(sorted_countries_to_rank_population_density)

        # set ranking based on index in list
        for rank in sorted_countries_to_rank_population.items():
            for country_item in rank[1]:
                if country_item[0] == country:
                    row_data_list.append(
                        [
                            rank[0],
                            int(country_item[1]),
                            rank[1].index(country_item) + 1,
                            float(country_item[2]),
                            sorted_countries_to_rank_population_density[rank[0]].index(
                                country_item
                            )
                            + 1,
                        ]
                    ) if country_item[1] != 0 else row_data_list.append(
                        [rank[0], "", "", "", ""]
                    )

        # store row data in list to be processed for empty rows
        while True:
            if row_data_list[0][1] == "":
                row_data_list.pop(0)
            if row_data_list[-1][1] == "":
                row_data_list.pop(-1)

            if row_data_list[0][1] != "" and row_data_list[-1][1] != "":
                break

        table_print.add_rows(row_data_list)

        print(table_print)

        return 0

    def generate_economic_report(self, country: str, economic_data):
        # initialize variables
        table_print = PrettyTable()

        # set columns
        table_print.field_names = ["Year", "GDPPC", "Rank"]

        # get sorted economic data back
        return_data = self.sort_country_economic_rank(economic_data, country)
        sorted_countries_to_rank = return_data["Economic"]
        country_currency = return_data["Currency"]

        # set ranking based on index in list
        row_data_list = []
        for rank in sorted_countries_to_rank.items():
            for country_item in rank[1]:
                if country_item[0] == country:
                    row_data_list.append(
                        [rank[0], int(country_item[1]), rank[1].index(country_item) + 1]
                    ) if country_item[1] != 0 else row_data_list.append(
                        [rank[0], "", ""]
                    )

        # store row data in list to be processed for empty rows
        while True:
            if row_data_list[0][1] == "":
                row_data_list.pop(0)
            if row_data_list[-1][1] == "":
                row_data_list.pop(-1)

            if row_data_list[0][1] != "" and row_data_list[-1][1] != "":
                break

        table_print.add_rows(row_data_list)
        # pprint.pprint(sorted_countries_to_rank)

        print(f"Currency: {country_currency}")
        print(table_print)

        return 0
