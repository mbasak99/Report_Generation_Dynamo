"""
Monark Basak
1056289
mbasak@uoguelph.ca
"""

import pandas as pd
import math
import numpy
import csv
from decimal import Decimal


class LoadCSVData:
    def __init__(self) -> None:
        pass

    def create_country_data(self):
        country_data_list = []

        area_df = pd.read_csv("shortlist_area.csv")
        area_df.rename(columns={"Country Name": "Country"}, inplace=True)

        capitals_df = pd.read_csv("shortlist_capitals.csv")
        capitals_df.rename(columns={"Country Name": "Country"}, inplace=True)

        area_df = pd.DataFrame(area_df).merge(
            capitals_df, left_on=["ISO3", "Country"], right_on=["ISO3", "Country"]
        )

        # need to read languages through csv reader since it can have more than 1 languages
        languages_dict = {"ISO3": [], "Country": [], "Languages": []}
        with open("shortlist_languages.csv", "r") as file:
            languages_csv = csv.reader(file)
            next(languages_csv)
            for row in languages_csv:
                languages_dict["ISO3"].append(row[0])
                languages_dict["Country"].append(row[1])
                languages_dict["Languages"].append(",".join(row[2:]))

        languages_df = pd.DataFrame(languages_dict)

        area_df = pd.DataFrame(area_df).merge(
            languages_df, left_on=["ISO3", "Country"], right_on=["ISO3", "Country"]
        )

        un_df = pd.read_csv("un_shortlist.csv")
        un_df.columns = ["ISO3", "Country", "OfficialName", "Short"]
        temp = pd.DataFrame(
            {
                "ISO3": ["ALB"],
                "Country": ["Albania"],
                "OfficialName": ["the Republic of Albania"],
                "Short": ["AL"],
            }
        )
        un_df = pd.concat([un_df, temp])
        countries_df = pd.DataFrame(un_df).merge(
            area_df, left_on=["ISO3", "Country"], right_on=["ISO3", "Country"]
        )
        for index, item in countries_df.iterrows():
            country_data_list.append(
                {
                    "ISO3": item["ISO3"],
                    "Country": item["Country"],
                    "OfficialName": item["OfficialName"],
                    "Short": item["Short"],
                    "Area": item["Area"],
                    "Capital": item["Capital"],
                    "Languages": item["Languages"],
                }
            )

        return country_data_list

    def create_country_economic(self):
        country_economics_list = []

        gdppc_df = pd.read_csv("shortlist_gdppc.csv")
        pop_df = pd.read_csv("shortlist_curpop.csv")
        economy_df = pd.DataFrame(gdppc_df).merge(
            pop_df[["Country", "Currency"]], left_on=["Country"], right_on=["Country"]
        )

        # print(economy_df)
        # fill in basic info first
        for index, item in economy_df[["Country", "Currency"]].iterrows():
            country_economics_list.append(
                {
                    "Country": item["Country"],
                    "Currency": item["Currency"],
                    "Economy": {},
                }
            )

        # fill in the economy
        # only get the year columns
        cols = economy_df.columns[
            ~economy_df.columns.isin(["Currency", "Country"])
        ].to_list()
        for index, gdp in economy_df[cols].iterrows():
            for gdp_year in list(gdp.keys()):
                country_economics_list[index]["Economy"][gdp_year] = (
                    Decimal(gdp[gdp_year])
                    if Decimal(gdp[gdp_year]).is_nan() == False
                    else Decimal(0)
                )

        return country_economics_list

    def create_country_population(self):
        country_population_list = []

        pop_df = pd.read_csv("shortlist_curpop.csv")
        pop_df.rename(columns={"Population 1970": "1970"}, inplace=True)

        area_df = pd.read_csv("shortlist_area.csv")
        area_df.rename(columns={"Country Name": "Country"}, inplace=True)

        # only want the country and years from population
        filter_cols = pop_df.columns[~pop_df.columns.isin(["Currency"])].to_list()
        pop_df = pd.DataFrame(pop_df[filter_cols]).merge(
            area_df[["ISO3", "Country"]], left_on="Country", right_on="Country"
        )

        for index, item in pop_df[["Country", "ISO3"]].iterrows():
            country_population_list.append(
                {"ISO3": item["ISO3"], "Country": item["Country"], "Population": {}}
            )

        filter_cols = pop_df.columns[
            ~pop_df.columns.isin(["ISO3", "Country"])
        ].to_list()
        for index, pop in pop_df[filter_cols].iterrows():
            for pop_year in list(pop.keys()):
                country_population_list[index]["Population"][pop_year] = (
                    Decimal(pop[pop_year])
                    if Decimal(pop[pop_year]).is_nan() == False
                    else Decimal(0)
                )

        return country_population_list
