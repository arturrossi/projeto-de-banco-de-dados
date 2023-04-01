import psycopg2.extras
from constants import alternative_alpha_3
import pandas.io.sql as psql
from psycopg2.extensions import AsIs

# Function to insert dataframe data into table
def insert_dataframe_values_to_table(database, df, table_name):
    if len(df) <= 0:
        return

    df_columns = list(df)
     # create (col1,col2,...)
    columns = ",".join(df_columns)
    # create VALUES('%s', '%s",...) one '%s' per column
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 

     #create INSERT INTO table (columns) VALUES('%s',...)
    insert_stmt = "INSERT INTO {} ({}) {}".format(table_name, columns, values)

    cursor = database.connection.cursor()
    psycopg2.extras.execute_batch(cursor, insert_stmt, df.values)
    database.connection.commit()
    cursor.close()

# Useful functions to clean players data
def get_country_id_by_alpha3(countries, name):
  try:
    country_id_row = countries[countries['alpha_3_code'] == name]['id'].iloc[0]
    return country_id_row
  except:
    # Trying one of the non-standard aplha-3 codes
    country_id_row = countries[countries['alpha_3_code'] == alternative_alpha_3.get(name)]['id'].iloc[0]
    return country_id_row

def add_country_id_column(database, players_df):
    countries = countries = psql.read_sql_query('select * from countries', database.connection)

    players_df['country_id'] = players_df.apply(lambda x: get_country_id_by_alpha3(countries, x['Nation'][3:].strip()), axis=1)
    return players_df

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)