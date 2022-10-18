# -*- coding: utf-8 -*-

import pickle
import requests
import json
import re
import os
import pandas as pd
import random
import time

from datetime import datetime
from database import Database


def load_data():
    dataset = pd.read_csv("./dataset/Fishdata.csv", header=0, encoding="utf8")
    print(dataset)
    return dataset


def data_extraction(data_input, column_name):

    items = []

    for i in range(0, len(data_input)):
        items.append(data_input[column_name][i])

    # dedup
    new_items = list(set(items))
    return new_items


def relation_extraction(rawdata):
    relation_list = []
    country_list = []
    rowitems_list = []

    merged_links_dict = {}

    for row_index in range(0, len(rawdata)):
        column_index = 0
        country = rawdata[rawdata.columns[column_index + 13]][row_index]

        # sereach all
        while column_index < (len(rawdata.columns) - 21):
            relation_list.append(rawdata.columns[column_index])
            rowitems_list.append(rawdata[rawdata.columns[column_index]][row_index])
            country_list.append(country)
            column_index += 1

    country_list = [str(i) for i in country_list]
    rowitems_list = [str(i) for i in rowitems_list]

    merged_links_dict["country"] = country_list
    merged_links_dict["relation"] = relation_list
    merged_links_dict["rowitems"] = rowitems_list

    df_data = pd.DataFrame(merged_links_dict)

    return df_data


if __name__ == "__main__":

    rawdata = load_data()
    # retrive fish name
    fish_name_list = data_extraction(rawdata, "Family")
    # retrive country name
    country_name_list = data_extraction(rawdata, "Country")

    df_data = relation_extraction(rawdata)

    db = Database()
    db.create_node(fish_name_list, country_name_list)
    db.create_relation(df_data)
