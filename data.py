import pandas as pd
from helpers import insert_dataframe_values_to_table, add_country_id_column
from constants import positions_dict, positions_id_dict, leagues_dict, leagues
import pandas.io.sql as psql

def populate_countries_table(database):
    countries_df = pd.read_csv("./datasets/countries.txt")
    countries_df = countries_df.drop(['alpha-2', 'country-code', 'iso_3166-2', 'region-code', 'sub-region-code', 'intermediate-region-code'], axis=1)
    countries_df.rename(columns={'alpha-3': 'alpha_3_code', 'sub-region': 'sub_region', 'intermediate-region': 'intermediate_region'}, inplace=True)

    insert_dataframe_values_to_table(database, countries_df, "countries")

def populate_positions_table(database):
    # Seeding positions table
    data_2018_std = pd.read_csv("./datasets/2018/2018-std.txt")
    data_2018_std = data_2018_std.filter(['Pos'])
    data_2018_std.drop_duplicates(subset=['Pos'], inplace=True)

    data_2019_std = pd.read_csv("./datasets/2019/2019-std.txt")
    data_2019_std = data_2019_std.filter(['Pos'])
    data_2019_std.drop_duplicates(subset=['Pos'], inplace=True)
    data_2019_std

    data_2020_std = pd.read_csv("./datasets/2020/2020-std.txt")
    data_2020_std = data_2020_std.filter(['Pos'])
    data_2020_std.drop_duplicates(subset=['Pos'], inplace=True)

    data_2021_std = pd.read_csv("./datasets/2021/2021-std.txt")
    data_2021_std = data_2021_std.filter(['Pos'])
    data_2021_std.drop_duplicates(subset=['Pos'], inplace=True)

    data_2022_std = pd.read_csv("./datasets/2022/2022-std.txt")
    data_2022_std = data_2022_std.filter(['Pos'])
    data_2022_std.drop_duplicates(subset=['Pos'], inplace=True)

    positions = pd.concat([data_2018_std, data_2019_std, data_2020_std, data_2021_std, data_2022_std], ignore_index = True)
    positions.drop_duplicates(subset=['Pos'], inplace=True)
    positions['name'] = positions['Pos'].map(positions_dict)
    positions = positions.filter(['name', 'id'])
    positions = positions.apply(lambda x: x.str.split(',').explode())
    positions.drop_duplicates(subset=['name'], inplace=True)
    positions['id'] = positions['name'].map(positions_id_dict)
    
    insert_dataframe_values_to_table(database, positions, "positions")

def get_countries_table(database):
    countries = psql.read_sql_query('select * from countries', database.connection)
    
    return countries

def get_country_id(database, country_name):
    select_stmt = f"select * from countries where name LIKE '%{country_name}%'"
    query = psql.read_sql_query(select_stmt, database.connection)

    print(query)
    return query['id'].values[0]

def search_player(database, name, birth_year, country_id):
    select_stmt = f"select * from players where name LIKE '%{name}%' and birth_year = {birth_year} and country_id = {country_id}"
    try:
        query = psql.read_sql_query(select_stmt, database.connection)
    except:
        name_double_escape = name.replace("'", "''")
        select_stmt = f"select * from players where name LIKE '%{name_double_escape}%' and birth_year = {birth_year} and country_id = {country_id}"
        query = psql.read_sql_query(select_stmt, database.connection)
    return query['id'].values[0]

def search_team(database, name):
    select_stmt = f"select * from teams where name LIKE '%{name}%'"
    try:
        query = psql.read_sql_query(select_stmt, database.connection)
    except:
        name_double_escape = name.replace("'", "''")
        select_stmt = f"select * from teams where name LIKE '%{name_double_escape}%'"
        query = psql.read_sql_query(select_stmt, database.connection)
    return query['id'].values[0]

def add_player_id_column(database, players_df):
    players_df['player_id'] = players_df.apply(lambda x: search_player(database, x['Player'], x['Born'], x['country_id']), axis=1)
    return players_df

def add_team_id_column(database, players_df):
    players_df['team_id'] = players_df.apply(lambda x: search_team(database, x['Squad']), axis=1)
    return players_df

def search_position(database, name):
    select_stmt = f"select * from position where name LIKE '%{name}%'"
    query = psql.read_sql_query(select_stmt, database.connection)
    return query['id'].values[0]

def insert_player_position(database, player_id, team_id, year, positions):
    positions_list = positions.split(',')
    cursor = database.connection.cursor()

    for position in positions_list:
        insert_stmt = f"insert into players_positions_in_season(player_id, team_id, year, position_id) VALUES ({player_id}, {team_id}, {year}, {positions_id_dict[position]})"
        cursor.execute(insert_stmt)
    
    database.connection.commit()
    cursor.close()

def populate_players_data(database):
    data_2018_std = pd.read_csv("./datasets/2018/2018-std.txt")
    data_2018_player = add_country_id_column(database, data_2018_std)
    data_2018_player.rename(columns={'Player': 'name', 'Born': 'birth_year'}, inplace=True)
    data_2018_player  = data_2018_player[['name', 'birth_year', 'country_id']]

    data_2019_std = pd.read_csv("./datasets/2019/2019-std.txt")
    data_2019_player = add_country_id_column(database, data_2019_std)
    data_2019_player.rename(columns={'Player': 'name', 'Born': 'birth_year'}, inplace=True)
    data_2019_player = data_2019_player[['name', 'birth_year', 'country_id']]

    data_2020_std = pd.read_csv("./datasets/2020/2020-std.txt")
    data_2020_player = add_country_id_column(database, data_2020_std)
    data_2020_player.rename(columns={'Player': 'name', 'Born': 'birth_year'}, inplace=True)
    data_2020_player = data_2020_player[['name', 'birth_year', 'country_id']]

    data_2021_std = pd.read_csv("./datasets/2021/2021-std.txt")
    data_2021_player = add_country_id_column(database, data_2021_std)
    data_2021_player.rename(columns={'Player': 'name', 'Born': 'birth_year'}, inplace=True)
    data_2021_player = data_2021_player[['name', 'birth_year', 'country_id']]

    data_2022_std = pd.read_csv("./datasets/2022/2022-std.txt")
    data_2022_player = add_country_id_column(database, data_2022_std)
    data_2022_player.rename(columns={'Player': 'name', 'Born': 'birth_year'}, inplace=True)
    data_2022_player = data_2022_player[['name', 'birth_year', 'country_id']]

    all_players = pd.concat([data_2018_std, data_2019_std, data_2020_std, data_2021_std, data_2022_std], ignore_index = True)
    all_players.drop_duplicates(subset=['name', 'birth_year'], inplace=True)
    all_players = all_players.filter(['name', 'birth_year', 'country_id'])
    all_players['birth_year'] = all_players['birth_year'].astype(int) 

    insert_dataframe_values_to_table(database, all_players, "players")

def get_leagues_obj(database):
    leagues_obj = { 
        'name': leagues['name'],
        'country_id': []
    }

    for country in leagues['countries']:
        leagues_obj['country_id'].append(get_country_id(database, country))

    return leagues_obj

def get_leagues_dataframe(database):
    leagues_df = pd.DataFrame(data=get_leagues_obj(database))

    return leagues_df

def get_top_10_yellow_cards(database):
    countries = psql.read_sql_query("""
    SELECT p.name, ps.year, t.name as team_name, yellow_cards 
    FROM players_stats_in_season pss 
    JOIN players_seasons ps ON pss.player_id = ps.player_id 
    JOIN players p ON ps.player_id = p.id 
    JOIN teams t ON t.id = ps.team_id 
    ORDER BY yellow_cards 
    DESC LIMIT 10""", database.connection)
    
    return countries    

def get_top_10_red_cards(database):
    countries = psql.read_sql_query("""
    SELECT p.name, ps.year, t.name as team_name, red_cards 
    FROM players_stats_in_season pss 
    JOIN players_seasons ps ON pss.player_id = ps.player_id 
    JOIN players p ON ps.player_id = p.id 
    JOIN teams t ON t.id = ps.team_id 
    ORDER BY red_cards 
    DESC LIMIT 10""", database.connection)
    
    return countries    

def get_youngest_in_2021(database):
    countries = psql.read_sql_query("""
    SELECT distinct p.name, ps.year, p.birth_year, t.name as team_name 
    FROM players_stats_in_season pss 
    JOIN players_seasons ps ON pss.player_id = ps.player_id 
    JOIN players p ON ps.player_id = p.id 
    JOIN teams t ON t.id = ps.team_id 
    WHERE ps.year = 2021 AND pss.year = 2021 
    ORDER BY p.birth_year 
    DESC LIMIT 10""", database.connection)
    
    return countries    

def get_top_goals_against_2020(database):
    countries = psql.read_sql_query("""
    SELECT p.name, ps.year, pss.goals_against, t.name as team_name 
    FROM players_stats_in_season pss 
    JOIN players_seasons ps ON pss.player_id = ps.player_id 
    JOIN players p ON ps.player_id = p.id 
    JOIN teams t ON t.id = ps.team_id 
    WHERE ps.year = 2020 AND pss.year = 2020 
    ORDER BY pss.goals_against 
    DESC LIMIT 10""", database.connection)
    
    return countries

def get_top_shots_on_target_2019(database):
    countries = psql.read_sql_query("""
    SELECT p.name, ps.year, pss.shots_on_target, t.name as team_name 
    FROM players_stats_in_season pss 
    JOIN players_seasons ps ON pss.player_id = ps.player_id 
    JOIN players p ON ps.player_id = p.id 
    JOIN teams t ON t.id = ps.team_id 
    WHERE ps.year = 2019 AND pss.year = 2019 
    ORDER BY pss.shots_on_target 
    DESC LIMIT 10""", database.connection)
    
    return countries

def get_top_assists_2022(database):
    countries = psql.read_sql_query("""
    SELECT p.id, p.name, ps.year, pss.year, pss.assists, t.name as team_name 
    FROM players_stats_in_season pss 
    JOIN players_seasons ps ON pss.player_id = ps.player_id 
    JOIN players p ON ps.player_id = p.id 
    JOIN teams t ON t.id = ps.team_id 
    WHERE ps.year = 2022 AND pss.year = 2022 
    ORDER BY pss.assists 
    DESC LIMIT 10""", database.connection)
    
    return countries

def get_top_minutes_played_2018(database):
    countries = psql.read_sql_query("""
    SELECT p.id, p.name as name, pos.name as position, pss.year, pss.minutes_played as minutes, t.name as team_name 
    FROM players_stats_in_season pss 
    JOIN players_seasons ps 
    ON pss.player_id = ps.player_id 
    JOIN players p 
    ON ps.player_id = p.id 
    JOIN players_positions_in_season pos_season 
    ON p.id = pos_season.player_id AND pss.year = pos_season.year 
    JOIN positions pos ON pos_season.position_id = pos.id 
    JOIN teams t ON t.id = ps.team_id 
    WHERE ps.year = 2018 AND pss.year = 2018 
    ORDER BY pss.minutes_played 
    DESC LIMIT 10""", database.connection)
    
    return countries
    
def get_avg_age_by_league(database):
    query = psql.read_sql_query("""
    SELECT AVG(ps.year - p.birth_year) as player_age, l.name as league, ps.year
    FROM players_seasons ps
    JOIN players p ON ps.player_id = p.id
    JOIN teams t ON t.id = ps.team_id
    JOIN leagues l ON l.id = t.league_id
    GROUP BY ps.year, l.name
    ORDER BY ps.year, l.name
    """, database.connection)

    return query

def get_total_goals_by_league(database):
    query = psql.read_sql_query("""
    SELECT SUM(pss.non_penalty_goals + pss.penalty_goals) as total_goals, l.name as league, pss.year
    FROM players_stats_in_season pss
    JOIN teams t ON t.id = pss.team_id
    JOIN leagues l ON l.id = t.league_id
    GROUP BY pss.year, l.name
    ORDER BY pss.year, l.name
    """, database.connection)

    return query

def get_top_scorer_by_year_league(database):
    query = psql.read_sql_query("""
    WITH top_scorers AS(
    SELECT MAX(pss.non_penalty_goals + pss.penalty_goals) as total_goals, pss.player_id, l.name as league, pss.year
    FROM players_stats_in_season pss 
    JOIN players p ON pss.player_id = p.id 
    JOIN teams t ON t.id = pss.team_id
    JOIN leagues l ON l.id = t.league_id
    GROUP BY pss.year, l.name
    ORDER BY pss.year, l.name)
    SELECT name
    FROM players pl
    JOIN top_scorers ON top_scorers.id = pl.id""", database.connection)

    return query

def populate_leagues_data(database):
    leagues_df =get_leagues_dataframe(database)
    insert_dataframe_values_to_table(database, leagues_df, "leagues")

    data_2018_std = pd.read_csv("./datasets/2018/2018-std.txt")
    data_2019_std = pd.read_csv("./datasets/2019/2019-std.txt")
    data_2020_std = pd.read_csv("./datasets/2020/2020-std.txt")
    data_2021_std = pd.read_csv("./datasets/2021/2021-std.txt")
    data_2022_std = pd.read_csv("./datasets/2022/2022-std.txt")
    all_teams = pd.concat([data_2018_std, data_2019_std, data_2020_std, data_2021_std, data_2022_std], ignore_index = True)
    all_teams = all_teams.filter(['Squad', 'Comp'])
    all_teams.drop_duplicates(subset=['Squad', 'Comp'], inplace=True, ignore_index=True)
    all_teams["league_id"] = all_teams["Comp"].map(leagues_dict)
    all_teams.rename(columns={'Squad': 'name'}, inplace=True)
    all_teams = all_teams.filter(['name', 'league_id'])
    insert_dataframe_values_to_table(database, all_teams, 'teams')

def populate_2018_stats(database):
    data_2018_std = pd.read_csv("./datasets/2018/2018-std.txt")
    data_2018_std = add_country_id_column(database, data_2018_std)
    data_2018_std_with_player_id = add_player_id_column(database, data_2018_std)
    data_2018_std_with_team_id = add_team_id_column(database, data_2018_std_with_player_id)
    data_2018_std_with_team_id['year'] = 2018

    data_2018_players_seasons = data_2018_std_with_team_id.filter(['player_id', 'team_id', 'year'])
    # Insert player season
    # insert_dataframe_values_to_table(database, data_2018_players_seasons, 'players_seasons')

    data_2018_std['positions'] = data_2018_std_with_team_id['Pos'].map(positions_dict)

    # Insert positions
    # data_2018_std.apply(lambda x: insert_player_position(database, x['player_id'], x['team_id'], x['year'], x['positions']), axis=1)

    data_2018_std = data_2018_std.filter(['player_id', 'team_id', 'Min', 'G-PK', 'PK', 'PKatt', 'Ast', 'CrdY', 'CrdR'])
    data_2018_std.rename(columns={'Min': 'minutes_played',
                                'G-PK': 'non_penalty_goals',
                                'PK': 'penalty_goals',
                                'PKatt': 'penalty_attempts',
                                'Ast': 'assists',
                                'CrdY': 'yellow_cards',
                                'CrdR': 'red_cards'}, inplace=True)

    data_2018_def = pd.read_csv("./datasets/2018/2018-def.txt")
    data_2018_def = data_2018_def.filter(['Att 3rd', 'Mid 3rd', 'Def 3rd', 'Blocks', 'Int'])
    data_2018_def.rename(columns={'Att 3rd': 'offensive_tackles', 
                                'Mid 3rd': 'midfield_tackles',
                                'Def 3rd': 'defensive_tackles',
                                'Blocks': 'blocks',
                                'Int': 'interceptions'}, inplace=True)

    data_2018_gca = pd.read_csv("./datasets/2018/2018-gca.txt")
    data_2018_gca = data_2018_gca.filter(['SCA', 'GCA'])
    data_2018_gca.rename(columns={'SCA': 'shot_creating_actions', 
                                'GCA': 'goal_creating_actions'}, inplace=True)

    stats_2018 = pd.concat([data_2018_std, data_2018_def, data_2018_gca], axis = 1)

    data_2018_gk = pd.read_csv("./datasets/2018/2018-gk.txt")
    data_2018_gk = add_country_id_column(database, data_2018_gk)
    data_2018_gk_with_player_id = add_player_id_column(database, data_2018_gk)
    data_2018_gk_with_player_id = data_2018_gk_with_player_id.filter(['player_id', 'GA', 'SoTA', 'Saves', 'PKatt', 'PKsv'])
    data_2018_gk_with_player_id.rename(columns={'PKatt': 'def_penalties_attempts', 
                                'GA': 'goals_against', 
                                'Saves': 'saves', 
                                'SoTA': 'shots_on_target', 
                                'PKsv': 'penalties_saved'}, inplace=True)

    stats_2018_complete = pd.merge(stats_2018, data_2018_gk_with_player_id, on='player_id', how='left')
    stats_2018_complete['year'] = 2018
    stats_2018_complete.drop_duplicates(subset=['player_id', 'team_id', 'year'], inplace=True)
    stats_2018_complete = stats_2018_complete.fillna(0)

    insert_dataframe_values_to_table(database, stats_2018_complete, 'players_stats_in_season')

def populate_2019_stats(database):
    data_2019_std = pd.read_csv("./datasets/2019/2019-std.txt")
    data_2019_std = add_country_id_column(database, data_2019_std)
    data_2019_std_with_player_id = add_player_id_column(database, data_2019_std)
    data_2019_std_with_team_id = add_team_id_column(database, data_2019_std_with_player_id)
    data_2019_std_with_team_id['year'] = 2019

    data_2019_players_seasons = data_2019_std_with_team_id.filter(['player_id', 'team_id', 'year'])
    # Insert player season
    insert_dataframe_values_to_table(database, data_2019_players_seasons, 'players_seasons')

    data_2019_std['positions'] = data_2019_std_with_team_id['Pos'].map(positions_dict)

    # Insert positions
    data_2019_std.apply(lambda x: insert_player_position(database, x['player_id'], x['team_id'], x['year'], x['positions']), axis=1)

    data_2019_std = data_2019_std.filter(['player_id', 'team_id', 'Min', 'G-PK', 'PK', 'PKatt', 'Ast', 'CrdY', 'CrdR'])
    data_2019_std.rename(columns={'Min': 'minutes_played',
                                'G-PK': 'non_penalty_goals',
                                'PK': 'penalty_goals',
                                'PKatt': 'penalty_attempts',
                                'Ast': 'assists',
                                'CrdY': 'yellow_cards',
                                'CrdR': 'red_cards'}, inplace=True)

    data_2019_def = pd.read_csv("./datasets/2019/2019-def.txt")
    data_2019_def = data_2019_def.filter(['Att 3rd', 'Mid 3rd', 'Def 3rd', 'Blocks', 'Int'])
    data_2019_def.rename(columns={'Att 3rd': 'offensive_tackles', 
                                'Mid 3rd': 'midfield_tackles',
                                'Def 3rd': 'defensive_tackles',
                                'Blocks': 'blocks',
                                'Int': 'interceptions'}, inplace=True)

    data_2019_gca = pd.read_csv("./datasets/2019/2019-gca.txt")
    data_2019_gca = data_2019_gca.filter(['SCA', 'GCA'])
    data_2019_gca.rename(columns={'SCA': 'shot_creating_actions', 
                                'GCA': 'goal_creating_actions'}, inplace=True)

    stats_2019 = pd.concat([data_2019_std, data_2019_def, data_2019_gca], axis = 1)

    data_2019_gk = pd.read_csv("./datasets/2019/2019-gk.txt")
    data_2019_gk = add_country_id_column(database, data_2019_gk)
    data_2019_gk_with_player_id = add_player_id_column(database, data_2019_gk)
    data_2019_gk_with_player_id = data_2019_gk_with_player_id.filter(['player_id', 'GA', 'SoTA', 'Saves', 'PKatt', 'PKsv'])
    data_2019_gk_with_player_id.rename(columns={'PKatt': 'def_penalties_attempts', 
                                'GA': 'goals_against', 
                                'Saves': 'saves', 
                                'SoTA': 'shots_on_target', 
                                'PKsv': 'penalties_saved'}, inplace=True)

    stats_2019_complete = pd.merge(stats_2019, data_2019_gk_with_player_id, on='player_id', how='left')
    stats_2019_complete['year'] = 2019
    stats_2019_complete.drop_duplicates(subset=['player_id', 'team_id', 'year'], inplace=True)
    stats_2019_complete = stats_2019_complete.fillna(0)

    insert_dataframe_values_to_table(database, stats_2019_complete, 'players_stats_in_season')

def populate_2020_stats(database):
    data_2020_std = pd.read_csv("./datasets/2020/2020-std.txt")
    data_2020_std = add_country_id_column(database, data_2020_std)
    data_2020_std_with_player_id = add_player_id_column(database, data_2020_std)
    data_2020_std_with_team_id = add_team_id_column(database, data_2020_std_with_player_id)
    data_2020_std_with_team_id['year'] = 2020

    data_2020_players_seasons = data_2020_std_with_team_id.filter(['player_id', 'team_id', 'year'])
    # Insert player season
    insert_dataframe_values_to_table(database, data_2020_players_seasons, 'players_seasons')

    data_2020_std['positions'] = data_2020_std_with_team_id['Pos'].map(positions_dict)

    # Insert positions
    data_2020_std.apply(lambda x: insert_player_position(database, x['player_id'], x['team_id'], x['year'], x['positions']), axis=1)

    data_2020_std = data_2020_std.filter(['player_id', 'team_id', 'Min', 'G-PK', 'PK', 'PKatt', 'Ast', 'CrdY', 'CrdR'])
    data_2020_std.rename(columns={'Min': 'minutes_played',
                                'G-PK': 'non_penalty_goals',
                                'PK': 'penalty_goals',
                                'PKatt': 'penalty_attempts',
                                'Ast': 'assists',
                                'CrdY': 'yellow_cards',
                                'CrdR': 'red_cards'}, inplace=True)

    data_2020_def = pd.read_csv("./datasets/2020/2020-def.txt")
    data_2020_def = data_2020_def.filter(['Att 3rd', 'Mid 3rd', 'Def 3rd', 'Blocks', 'Int'])
    data_2020_def.rename(columns={'Att 3rd': 'offensive_tackles', 
                                'Mid 3rd': 'midfield_tackles',
                                'Def 3rd': 'defensive_tackles',
                                'Blocks': 'blocks',
                                'Int': 'interceptions'}, inplace=True)

    data_2020_gca = pd.read_csv("./datasets/2020/2020-gca.txt")
    data_2020_gca = data_2020_gca.filter(['SCA', 'GCA'])
    data_2020_gca.rename(columns={'SCA': 'shot_creating_actions', 
                                'GCA': 'goal_creating_actions'}, inplace=True)

    stats_2020 = pd.concat([data_2020_std, data_2020_def, data_2020_gca], axis = 1)

    data_2020_gk = pd.read_csv("./datasets/2020/2020-gk.txt")
    data_2020_gk = add_country_id_column(database, data_2020_gk)
    data_2020_gk_with_player_id = add_player_id_column(database, data_2020_gk)
    data_2020_gk_with_player_id = data_2020_gk_with_player_id.filter(['player_id', 'GA', 'SoTA', 'Saves', 'PKatt', 'PKsv'])
    data_2020_gk_with_player_id.rename(columns={'PKatt': 'def_penalties_attempts', 
                                'GA': 'goals_against', 
                                'Saves': 'saves', 
                                'SoTA': 'shots_on_target', 
                                'PKsv': 'penalties_saved'}, inplace=True)

    stats_2020_complete = pd.merge(stats_2020, data_2020_gk_with_player_id, on='player_id', how='left')
    stats_2020_complete['year'] = 2020
    stats_2020_complete.drop_duplicates(subset=['player_id', 'team_id', 'year'], inplace=True)
    stats_2020_complete = stats_2020_complete.fillna(0)

    insert_dataframe_values_to_table(database, stats_2020_complete, 'players_stats_in_season')

def populate_2021_stats(database):
    data_2021_std = pd.read_csv("./datasets/2021/2021-std.txt")
    data_2021_std = add_country_id_column(database, data_2021_std)
    data_2021_std_with_player_id = add_player_id_column(database, data_2021_std)
    data_2021_std_with_team_id = add_team_id_column(database, data_2021_std_with_player_id)
    data_2021_std_with_team_id['year'] = 2021

    data_2021_players_seasons = data_2021_std_with_team_id.filter(['player_id', 'team_id', 'year'])
    # Insert player season
    insert_dataframe_values_to_table(database, data_2021_players_seasons, 'players_seasons')

    data_2021_std['positions'] = data_2021_std_with_team_id['Pos'].map(positions_dict)

    # Insert positions
    data_2021_std.apply(lambda x: insert_player_position(database, x['player_id'], x['team_id'], x['year'], x['positions']), axis=1)

    data_2021_std = data_2021_std.filter(['player_id', 'team_id', 'Min', 'G-PK', 'PK', 'PKatt', 'Ast', 'CrdY', 'CrdR'])
    data_2021_std.rename(columns={'Min': 'minutes_played',
                                'G-PK': 'non_penalty_goals',
                                'PK': 'penalty_goals',
                                'PKatt': 'penalty_attempts',
                                'Ast': 'assists',
                                'CrdY': 'yellow_cards',
                                'CrdR': 'red_cards'}, inplace=True)

    data_2021_def = pd.read_csv("./datasets/2021/2021-def.txt")
    data_2021_def = data_2021_def.filter(['Att 3rd', 'Mid 3rd', 'Def 3rd', 'Blocks', 'Int'])
    data_2021_def.rename(columns={'Att 3rd': 'offensive_tackles', 
                                'Mid 3rd': 'midfield_tackles',
                                'Def 3rd': 'defensive_tackles',
                                'Blocks': 'blocks',
                                'Int': 'interceptions'}, inplace=True)

    data_2021_gca = pd.read_csv("./datasets/2021/2021-gca.txt")
    data_2021_gca = data_2021_gca.filter(['SCA', 'GCA'])
    data_2021_gca.rename(columns={'SCA': 'shot_creating_actions', 
                                'GCA': 'goal_creating_actions'}, inplace=True)

    stats_2021 = pd.concat([data_2021_std, data_2021_def, data_2021_gca], axis = 1)

    data_2021_gk = pd.read_csv("./datasets/2021/2021-gk.txt")
    data_2021_gk = add_country_id_column(database, data_2021_gk)
    data_2021_gk_with_player_id = add_player_id_column(database, data_2021_gk)
    data_2021_gk_with_player_id = data_2021_gk_with_player_id.filter(['player_id', 'GA', 'SoTA', 'Saves', 'PKatt', 'PKsv'])
    data_2021_gk_with_player_id.rename(columns={'PKatt': 'def_penalties_attempts', 
                                'GA': 'goals_against', 
                                'Saves': 'saves', 
                                'SoTA': 'shots_on_target', 
                                'PKsv': 'penalties_saved'}, inplace=True)

    stats_2021_complete = pd.merge(stats_2021, data_2021_gk_with_player_id, on='player_id', how='left')
    stats_2021_complete['year'] = 2021
    stats_2021_complete.drop_duplicates(subset=['player_id', 'team_id', 'year'], inplace=True)
    stats_2021_complete = stats_2021_complete.fillna(0)

    insert_dataframe_values_to_table(database, stats_2021_complete, 'players_stats_in_season')

def populate_2022_stats(database):
    data_2022_std = pd.read_csv("./datasets/2022/2022-std.txt")
    data_2022_std = add_country_id_column(database, data_2022_std)
    data_2022_std_with_player_id = add_player_id_column(database, data_2022_std)
    data_2022_std_with_team_id = add_team_id_column(database, data_2022_std_with_player_id)
    data_2022_std_with_team_id['year'] = 2022

    data_2022_players_seasons = data_2022_std_with_team_id.filter(['player_id', 'team_id', 'year'])
    # Insert player season
    insert_dataframe_values_to_table(database, data_2022_players_seasons, 'players_seasons')

    data_2022_std['positions'] = data_2022_std_with_team_id['Pos'].map(positions_dict)

    # Insert positions
    data_2022_std.apply(lambda x: insert_player_position(database, x['player_id'], x['team_id'], x['year'], x['positions']), axis=1)

    data_2022_std = data_2022_std.filter(['player_id', 'team_id', 'Min', 'G-PK', 'PK', 'PKatt', 'Ast', 'CrdY', 'CrdR'])
    data_2022_std.rename(columns={'Min': 'minutes_played',
                                'G-PK': 'non_penalty_goals',
                                'PK': 'penalty_goals',
                                'PKatt': 'penalty_attempts',
                                'Ast': 'assists',
                                'CrdY': 'yellow_cards',
                                'CrdR': 'red_cards'}, inplace=True)

    data_2022_def = pd.read_csv("./datasets/2022/2022-def.txt")
    data_2022_def = data_2022_def.filter(['Att 3rd', 'Mid 3rd', 'Def 3rd', 'Blocks', 'Int'])
    data_2022_def.rename(columns={'Att 3rd': 'offensive_tackles', 
                                'Mid 3rd': 'midfield_tackles',
                                'Def 3rd': 'defensive_tackles',
                                'Blocks': 'blocks',
                                'Int': 'interceptions'}, inplace=True)

    data_2022_gca = pd.read_csv("./datasets/2022/2022-gca.txt")
    data_2022_gca = data_2022_gca.filter(['SCA', 'GCA'])
    data_2022_gca.rename(columns={'SCA': 'shot_creating_actions', 
                                'GCA': 'goal_creating_actions'}, inplace=True)

    stats_2022 = pd.concat([data_2022_std, data_2022_def, data_2022_gca], axis = 1)

    data_2022_gk = pd.read_csv("./datasets/2022/2022-gk.txt")
    data_2022_gk = add_country_id_column(database, data_2022_gk)
    data_2022_gk_with_player_id = add_player_id_column(database, data_2022_gk)
    data_2022_gk_with_player_id = data_2022_gk_with_player_id.filter(['player_id', 'GA', 'SoTA', 'Saves', 'PKatt', 'PKsv'])
    data_2022_gk_with_player_id.rename(columns={'PKatt': 'def_penalties_attempts', 
                                'GA': 'goals_against', 
                                'Saves': 'saves', 
                                'SoTA': 'shots_on_target', 
                                'PKsv': 'penalties_saved'}, inplace=True)

    stats_2022_complete = pd.merge(stats_2022, data_2022_gk_with_player_id, on='player_id', how='left')
    stats_2022_complete['year'] = 2022
    stats_2022_complete.drop_duplicates(subset=['player_id', 'team_id', 'year'], inplace=True)
    stats_2022_complete = stats_2022_complete.fillna(0)

    insert_dataframe_values_to_table(database, stats_2022_complete, 'players_stats_in_season')

def populate_players_stats(database):
    populate_2018_stats(database)
    populate_2019_stats(database)
    populate_2020_stats(database)
    populate_2021_stats(database)
    populate_2022_stats(database)

