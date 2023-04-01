import os
import psycopg2

class Database:
    def __init__(self):
        self.connection = psycopg2.connect(
            host = os.environ.get("DATABASE_HOST"),
            port = os.environ.get("DATABASE_PORT"),
            user = os.environ.get("DATABASE_USER"),
            password = os.environ.get("DATABASE_PASSWORD"),
            database= os.environ.get("DATABASE")
        )

    def create_tables(self):
        commands = (
            """DROP TABLE IF EXISTS countries CASCADE""",
            """CREATE TABLE IF NOT EXISTS countries (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            alpha_3_code VARCHAR(3) NOT NULL,
            region VARCHAR(255) NOT NULL,
            sub_region VARCHAR(255),
            intermediate_region VARCHAR(255))""",
            """DROP TABLE IF EXISTS players CASCADE""",
            """CREATE TABLE IF NOT EXISTS players (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            birth_year integer,
            country_id integer REFERENCES countries ON DELETE CASCADE
            )""",      
            """DROP TABLE IF EXISTS leagues CASCADE""",
            """CREATE TABLE IF NOT EXISTS leagues (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            country_id integer REFERENCES countries ON DELETE CASCADE
            )""",
            """DROP TABLE IF EXISTS teams CASCADE""",
            """CREATE TABLE IF NOT EXISTS teams (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            league_id integer REFERENCES leagues
            ON DELETE CASCADE)""",
            """DROP TABLE IF EXISTS players_seasons CASCADE""",  
            """CREATE TABLE IF NOT EXISTS players_seasons (
                player_id integer,
                team_id integer,
                year integer,
                FOREIGN KEY (player_id) REFERENCES players ON DELETE CASCADE,
                FOREIGN KEY (team_id) REFERENCES teams ON DELETE CASCADE,
                PRIMARY KEY (player_id, team_id, year)
            )""",
            """DROP TABLE IF EXISTS positions CASCADE""",
            """CREATE TABLE IF NOT EXISTS positions(
                id SERIAL PRIMARY KEY,
                name VARCHAR(128) NOT NULL
            )""",
            """DROP TABLE IF EXISTS players_positions_in_season CASCADE""",
            """CREATE TABLE IF NOT EXISTS players_positions_in_season(
                player_id integer,
            team_id integer,
            year integer,
            position_id integer,
            FOREIGN KEY (player_id, team_id, year)
            REFERENCES players_seasons (player_id, team_id, year) ON DELETE CASCADE,
            FOREIGN KEY (position_id) REFERENCES positions ON DELETE CASCADE,
            PRIMARY KEY (player_id, team_id, year, position_id)
            )""",
            """DROP TABLE IF EXISTS players_stats_in_season""",
            """CREATE TABLE IF NOT EXISTS players_stats_in_season(
            player_id integer,
            team_id integer,
            year integer,
            minutes_played integer NOT NULL,
                non_penalty_goals integer NOT NULL,
            penalty_goals integer NOT NULL,
            penalty_attempts integer NOT NULL,
            assists integer NOT NULL,
            yellow_cards integer NOT NULL,
            red_cards integer NOT NULL,
            offensive_tackles integer NOT NULL,
            midfield_tackles integer NOT NULL,
            defensive_tackles integer NOT NULL,
            blocks integer NOT NULL,
            interceptions integer NOT NULL,
            shot_creating_actions integer NOT NULL,
            goal_creating_actions integer NOT NULL,
            goals_against integer NOT NULL,
            saves integer NOT NULL,
            shots_on_target integer NOT NULL,
            penalties_saved integer NOT NULL,	
            def_penalties_attempts integer NOT NULL,
            FOREIGN KEY (player_id, team_id, year)
            REFERENCES players_seasons (player_id, team_id, year) ON DELETE CASCADE,
            PRIMARY KEY (player_id, team_id, year)
        )"""
        )

        try:
            for command in commands:
                cursor = self.connection.cursor()
                cursor.execute(command)
                cursor.close()
                self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print('erro:', error)