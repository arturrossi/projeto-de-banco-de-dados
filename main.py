from dotenv import load_dotenv
from database import Database
from data import populate_countries_table, populate_players_data, populate_leagues_data, populate_players_stats, get_top_10_yellow_cards, get_top_10_red_cards, get_youngest_in_2021, get_top_goals_against_2020, get_top_shots_on_target_2019, get_top_assists_2022, get_top_minutes_played_2018, get_avg_age_by_league, get_total_goals_by_league, get_top_scorer_by_year_league
from helpers import addapt_numpy_int64, addapt_numpy_float64
import seaborn as sns
import matplotlib.pyplot as plt

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
    # # cursor.close()
    # print(get_top_10_yellow_cards(database))
    # print(get_top_10_red_cards(database))
    # print(get_youngest_in_2021(database))
    # print(get_top_goals_against_2020(database))

    # sns.set_context('paper')
    # fig, axs = plt.subplots(nrows=4)

    # avg_age = get_avg_age_by_league(database)

    # sns.barplot(x = 'year', y = 'player_age', hue = 'league', data = avg_age,
    #         palette = 'Blues', edgecolor = 'w', ax=axs[0])

    # goals = get_total_goals_by_league(database)

    # sns.barplot(x = 'year', y = 'total_goals', hue = 'league', data = goals,
    #         palette = 'Blues', edgecolor = 'w', ax=axs[1])
    # plt.show()

    top_scorers = get_top_scorer_by_year_league(database)
    print(top_scorers)

    # avg_age.groupby(['day','sex']).mean()

    # shots_on_target = get_top_shots_on_target_2019(database)
    # assists = get_top_assists_2022(database)
    # minutes= get_top_minutes_played_2018(database)

    # print(minutes['name'].values)
    # sns.barplot(x = minutes['name'].values, y = 'minutes', data = minutes,
    #         palette = 'Blues', edgecolor = 'w', ax=axs[0])
    
    # sns.barplot(x = assists['name'].values, y = 'assists', data = assists,
    #         palette = 'Blues', edgecolor = 'w', ax=axs[1])

    # sns.barplot(x = shots_on_target['name'].values, y = 'shots_on_target', data = shots_on_target,
    #         palette = 'Blues', edgecolor = 'w', ax=axs[2])
    
    # plt.show()


    # populate_countries_table(database)
    # populate_players_data(database)
    # populate_leagues_data(database)
    # populate_players_stats(database)