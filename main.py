from dotenv import load_dotenv
from database import Database
from data import populate_countries_table, populate_players_data, populate_leagues_data, populate_players_stats, get_top_10_yellow_cards, get_top_10_red_cards, get_youngest_in_2021
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
    # # database.create_tables()
    # cursor = database.connection.cursor()

    # insert_stmt = f"insert into positions(id, name) VALUES (1, 'Goalkeeper')"
    # cursor.execute(insert_stmt)
    
    # database.connection.commit()

    # insert_stmt = f"insert into positions(id, name) VALUES (2, 'Defender')"
    # cursor.execute(insert_stmt)
    
    # database.connection.commit()

    # insert_stmt = f"insert into positions(id, name) VALUES (3, 'Midfielder')"
    # cursor.execute(insert_stmt)
    
    # database.connection.commit()

    # insert_stmt = f"insert into positions(id, name) VALUES (4, 'Forward')"
    # cursor.execute(insert_stmt)
    
    # database.connection.commit()
    # cursor.close()
    print(get_top_10_yellow_cards(database))
    print(get_top_10_red_cards(database))
    print(get_youngest_in_2021(database))
    # populate_countries_table(database)
    # populate_players_data(database)
    # populate_leagues_data(database)
    # populate_players_stats(database)