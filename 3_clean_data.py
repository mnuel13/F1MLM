import sqlite3
import pandas as pd
from dateutil.relativedelta import *


database_name = 'races.db'
connection = sqlite3.connect(database_name)

table_names = ['a_races_table', 'b_results_table', 'c_driver_standings_table',
               'd_constructor_standings_table', 'e_qualifying_results_table', 'f_weather_info_table']


def get_table(table_name):
    query = f'SELECT * FROM "{table_name}"'
    df = pd.read_sql_query(query, connection)
    return df


races = get_table(table_names[0])
results = get_table(table_names[1])
driver_standings = get_table(table_names[2])
constructor_standings = get_table(table_names[3])
qualifying = get_table(table_names[4])
weather = get_table(table_names[5])

connection.close()

# merge df

df1 = pd.merge(races, weather, how='inner',
               on=['season', 'round', 'circuit_id']).drop(['lat', 'long', 'country', 'weather'],
                                                          axis=1)
df2 = pd.merge(df1, results, how='inner',
               on=['season', 'round', 'circuit_id']).drop(['url', 'points', 'status', 'time'],
                                                                 axis=1)
df3 = pd.merge(df2, driver_standings, how='left',
               on=['season', 'round', 'driver'])
df4 = pd.merge(df3, constructor_standings, how='left',
               on=['season', 'round', 'constructor'])  # from 1958

final_df = pd.merge(df4, qualifying, how='left',
                    on=['season', 'round', 'No']).drop(['driver_name', 'car'],
                                                         axis=1)  # from 1983

final_df['grid_x'] = final_df['grid_y']
final_df = final_df.drop(columns=['No', 'grid_y'])
final_df = final_df.rename(columns={'grid_x': 'grid'})

for col in ['qualifying_time']:
    final_df[col].fillna('0', inplace=True)

connection = sqlite3.connect(database_name)

# Append the 'results' DataFrame to the 'races.db' database as a new table
final_df.to_sql('g_final_table', connection, if_exists='replace', index=False)

# Close the database connection
connection.close()

# calculate age of drivers
final_df['date'] = pd.to_datetime(final_df.date)
final_df['date_of_birth'] = pd.to_datetime(final_df.date_of_birth)
final_df['driver_age'] = final_df.apply(lambda x:
                                        relativedelta(x['date'], x['date_of_birth']).years, axis=1)
final_df.drop(['date', 'date_of_birth'], axis=1, inplace=True)

# fill/drop nulls

for col in ['driver_points', 'driver_wins', 'driver_standings_pos', 'constructor_points',
            'constructor_wins', 'constructor_standings_pos']:
    final_df[col].fillna(0, inplace=True)
    final_df[col] = final_df[col].map(lambda x: int(x))

for col in ['qualifying_time']:
    final_df[col].fillna(00.000, inplace=True)

final_df.dropna(inplace=True)

# convert to boolean to save space

for col in ['weather_warm', 'weather_cold', 'weather_dry', 'weather_wet', 'weather_cloudy']:
    final_df[col] = final_df[col].map(lambda x: bool(x))

# calculate difference in qualifying times

final_df['qualifying_time'] = final_df['qualifying_time'].map(lambda x: 0 if str(x) == '00.000' else
                            (float(str(x).split(':')[1]) + (60 * float(str(x).split(':')[0]))) if isinstance(x, str) and ':' in x else
                            (float(x) if isinstance(x, (int, float)) else 0))
final_df = final_df[final_df['qualifying_time'] != 0]
final_df.sort_values(['season', 'round', 'grid'], inplace=True)
final_df['qualifying_time_diff'] = final_df.groupby(['season', 'round']).qualifying_time.diff()
final_df['qualifying_time'] = final_df.groupby(['season',
                                                'round']).qualifying_time_diff.cumsum().fillna(0)
final_df.drop('qualifying_time_diff', axis=1, inplace=True)
# final_df['drivers'] = final_df.driver

# get dummies


df_dum = pd.get_dummies(final_df, columns=['circuit_id', 'nationality', 'constructor'])

for col in df_dum.columns:
    if 'nationality' in col and df_dum[col].sum() < 140:
        df_dum.drop(col, axis=1, inplace=True)

    elif 'constructor' in col and df_dum[col].sum() < 140:
        df_dum.drop(col, axis=1, inplace=True)

    elif 'circuit_id' in col and df_dum[col].sum() < 70:
        df_dum.drop(col, axis=1, inplace=True)


    else:
        pass

connection = sqlite3.connect(database_name)
# Append the 'results' DataFrame to the 'races.db' database as a new table
final_df.to_sql('h_final_cleaned_table', connection, if_exists='replace', index=False)
# Close the database connection
connection.close()

connection = sqlite3.connect(database_name)
df_dum.to_sql('i_df_dum_table', connection, if_exists='replace', index=False)
connection.close()
