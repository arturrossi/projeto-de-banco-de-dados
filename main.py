from dotenv import load_dotenv
from database import Database
from data import populate_countries_table, populate_players_data, populate_leagues_data, populate_players_stats, get_country_id
from helpers import addapt_numpy_int64, addapt_numpy_float64

import numpy
import os
from psycopg2.extensions import register_adapter

if __name__ == '__main__':
    load_dotenv()

    print("asd")
    register_adapter(numpy.float64, addapt_numpy_float64)
    register_adapter(numpy.int64, addapt_numpy_int64)

    database = Database()
    database.create_tables()
    # print(get_country_id(database, 'United Kingdom'))
    populate_countries_table(database)
    populate_players_data(database)
    populate_leagues_data(database)
    populate_players_stats(database)