import pandas as pd
import numpy as np
import requests
import sqlite3
from tqdm import tqdm
from datetime import datetime
import time
from bs4 import BeautifulSoup


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', 12)

# Disable SSL certificate verification (not recommended for production)
requests.packages.urllib3.disable_warnings()
verify_ssl = False

startY, endY = 2024, 2025
# qualy started in 1983
years_to_retrieve = [y for y in range(startY, endY)]
until_round = 15
database_filename = 'races.db'

################################################################################### RACES

races = {'season': [],
         'round': [],
         'circuit_id': [],
         'lat': [],
         'long': [],
         'country': [],
         'date': [],
         'url': []
         }

# query API


not_connect = True
while not_connect:
    try:
        url = 'https://ergast.com/api/f1/{}.json'
        r = requests.get(url.format(years_to_retrieve[-1], verify=verify_ssl))
        json = r.json()
        dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print(f'{dt_string} - Api Available! Collecting Races Table Data!')
        print("")
        not_connect = False

    except:
        dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print(f'{dt_string} - Api not Available at the moment, trying again...')
        time.sleep(60)
        print("")




for year in years_to_retrieve:

    url = 'https://ergast.com/api/f1/{}.json'
    r = requests.get(url.format(year), verify=verify_ssl)
    json = r.json()
    for item in json['MRData']['RaceTable']['Races']:
        # if int(item['round']) == until_round + 1:
        #     break
        try:
            races['season'].append(int(item['season']))
        except:
            races['season'].append(None)

        try:
            races['round'].append(int(item['round']))
        except:
            races['round'].append(None)

        try:
            races['circuit_id'].append(item['Circuit']['circuitId'])
        except:
            races['circuit_id'].append(None)

        try:
            races['lat'].append(float(item['Circuit']['Location']['lat']))
        except:
            races['lat'].append(None)

        try:
            races['long'].append(float(item['Circuit']['Location']['long']))
        except:
            races['long'].append(None)

        try:
            races['country'].append(item['Circuit']['Location']['country'])
        except:
            races['country'].append(None)

        try:
            races['date'].append(item['date'])
        except:
            races['date'].append(None)

        try:
            races['url'].append(item['url'])
        except:
            races['url'].append(None)

races = pd.DataFrame(races)

# Connect to the SQLite database
connection = sqlite3.connect(database_filename)


existing_query = "SELECT * FROM a_races_table WHERE season IN ({}) AND round IN ({}) AND circuit_id IN ({})".format(
    ','.join(map(str, races['season'])),
    ','.join(map(str, races['round'])),
    ','.join(map(lambda x: "'" + x + "'", races['circuit_id']))
)

existing_race_data = pd.read_sql_query(existing_query, connection)
new_race_records = races[~races.apply(tuple, axis=1).isin(existing_race_data.apply(tuple, axis=1))]

# Append the new data to the existing races_table
new_race_records.to_sql('a_races_table', connection, if_exists='append', index=False)

# Close the database connection
connection.close()

################################################################################### RESULTS

rounds = []


for year in np.array(races.season.unique()):
    rounds.append([year, list(races[races.season == year]['round'])])

results = {'season': [],
           'round': [],
           'circuit_id': [],
           'driver': [],
           'date_of_birth': [],
           'nationality': [],
           'constructor': [],
           'grid': [],
           'time': [],
           'status': [],
           'points': [],
           'podium': [],
           'No': []}

# query API

not_connect = True
while not_connect:
    try:
        url = 'http://ergast.com/api/f1/{}/{}/results.json'
        r = requests.get(url.format(startY, until_round, verify=verify_ssl))
        json = r.json()
        dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print(f'{dt_string} - Api Available! Collecting Result Table Data!')
        print("")
        not_connect = False

    except:
        dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print(f'{dt_string} - Api not Available at the moment, trying again...')
        time.sleep(60)
        print("")


for n in list(range(len(rounds))):
    for i in rounds[n][1]:
        url = 'http://ergast.com/api/f1/{}/{}/results.json'
        print(rounds[n][0], i)
        r = requests.get(url.format(rounds[n][0], i, verify=verify_ssl))
        json = r.json()

        try:
            for item in json['MRData']['RaceTable']['Races'][0]['Results']:
                try:
                    results['season'].append(int(json['MRData']['RaceTable']['Races'][0]['season']))
                except:
                    results['season'].append(None)

                try:
                    results['round'].append(int(json['MRData']['RaceTable']['Races'][0]['round']))
                except:
                    results['round'].append(None)

                try:
                    results['circuit_id'].append(json['MRData']['RaceTable']['Races'][0]['Circuit']['circuitId'])
                except:
                    results['circuit_id'].append(None)

                try:
                    results['driver'].append(item['Driver']['driverId'])
                except:
                    results['driver'].append(None)

                try:
                    results['date_of_birth'].append(item['Driver']['dateOfBirth'])
                except:
                    results['date_of_birth'].append(None)

                try:
                    results['nationality'].append(item['Driver']['nationality'])
                except:
                    results['nationality'].append(None)

                try:
                    results['constructor'].append(item['Constructor']['constructorId'])
                except:
                    results['constructor'].append(None)

                try:
                    results['grid'].append(int(item['grid']))
                except:
                    results['grid'].append(None)

                try:
                    results['time'].append(int(item['Time']['millis']))
                except:
                    results['time'].append(None)

                try:
                    results['status'].append(item['status'])
                except:
                    results['status'].append(None)

                try:
                    results['points'].append(float(item['points']))
                except:
                    results['points'].append(None)

                try:
                    results['podium'].append(int(item['position']))
                except:
                    results['podium'].append(None)

                try:
                    results['No'].append(int(item['number']))
                except:
                    results['No'].append(None)

        except IndexError:
            pass

results = pd.DataFrame(results)

connection = sqlite3.connect(database_filename)

existing_query = "SELECT * FROM b_results_table WHERE season IN ({}) AND round IN ({}) AND circuit_id IN ({}) AND driver IN ({})".format(
    ','.join(map(str, results['season'])),
    ','.join(map(str, results['round'])),
    ','.join(map(lambda x: "'" + x + "'", results['circuit_id'])),
    ','.join(map(lambda x: "'" + x + "'", results['driver']))
)


existing_results_data = pd.read_sql_query(existing_query, connection)
new_results_records = results[~results.apply(tuple, axis=1).isin(existing_results_data.apply(tuple, axis=1))]

# Append the new data to the existing races_table
new_results_records.to_sql('b_results_table', connection, if_exists='append', index=False)

# Close the database connection
connection.close()

################################################################################### DRVER STANDING

driver_standings = {'season': [],
                    'round': [],
                    'driver': [],
                    'driver_points': [],
                    'driver_wins': [],
                    'driver_standings_pos': []}



# query API

for n in tqdm(list(range(len(rounds))), desc=f'Driver Standing Year'):
    inner = tqdm(total=len(rounds[n][1]), leave=False, desc=f'Season', ncols=90, colour='Green')
    for i in rounds[n][1]:
        inner.update()
        url = 'https://ergast.com/api/f1/{}/{}/driverStandings.json'
        r = requests.get(url.format(rounds[n][0], i, verify=verify_ssl))
        json = r.json()
        try:
            for item in json['MRData']['StandingsTable']['StandingsLists'][0]['DriverStandings']:
                try:
                    driver_standings['season'].append(
                        int(json['MRData']['StandingsTable']['StandingsLists'][0]['season']))
                except:
                    driver_standings['season'].append(None)

                try:
                    driver_standings['round'].append(
                        int(json['MRData']['StandingsTable']['StandingsLists'][0]['round']))
                except:
                    driver_standings['round'].append(None)

                try:
                    driver_standings['driver'].append(item['Driver']['driverId'])
                except:
                    driver_standings['driver'].append(None)

                try:
                    driver_standings['driver_points'].append(float(item['points']))
                except:
                    driver_standings['driver_points'].append(None)

                try:
                    driver_standings['driver_wins'].append(int(item['wins']))
                except:
                    driver_standings['driver_wins'].append(None)

                try:
                    driver_standings['driver_standings_pos'].append(int(item['position']))
                except:
                    driver_standings['driver_standings_pos'].append(None)
        except IndexError:
            pass
        inner.close()

driver_standings = pd.DataFrame(driver_standings)

# define lookup function to shift points and number of wins from previous rounds

def lookup(df, team, points):
    df['lookup1'] = df.season.astype(str) + df[team] + df['round'].astype(str)
    df['lookup2'] = df.season.astype(str) + df[team] + (df['round'] - 1).astype(str)
    new_df = df.merge(df[['lookup1', points]], how='left', left_on='lookup2', right_on='lookup1')
    new_df.drop(['lookup1_x', 'lookup2', 'lookup1_y'], axis=1, inplace=True)
    new_df.rename(columns={points + '_x': points + '_after_race', points + '_y': points}, inplace=True)
    new_df[points].fillna(0, inplace=True)
    return new_df


driver_standings = lookup(driver_standings, 'driver', 'driver_points')
driver_standings = lookup(driver_standings, 'driver', 'driver_wins')
driver_standings = lookup(driver_standings, 'driver', 'driver_standings_pos')

driver_standings.drop(['driver_points_after_race', 'driver_wins_after_race', 'driver_standings_pos_after_race'],
                      axis=1, inplace=True)

connection = sqlite3.connect(database_filename)

existing_query = "SELECT * FROM c_driver_standings_table WHERE season IN ({}) AND round IN ({}) AND driver IN ({})".format(
    ','.join(map(str, driver_standings['season'])),
    ','.join(map(str, driver_standings['round'])),
    ','.join(map(lambda x: "'" + x + "'", driver_standings['driver']))
)


existing_driver_standings_data = pd.read_sql_query(existing_query, connection)
new_driver_standings_records = driver_standings[~driver_standings.apply(tuple, axis=1).isin(existing_driver_standings_data.apply(tuple, axis=1))]

# Append the 'results' DataFrame to the 'races.db' database as a new table
new_driver_standings_records.to_sql('c_driver_standings_table', connection, if_exists='append', index=False)

# Close the database connection
connection.close()

#################################################################################### Constructor Standing

# start from year 1958 and do rounds[8:] if starting from 1950
if startY < 1958:
    constructor_rounds = rounds[1958-startY:]
else:
    constructor_rounds = rounds

constructor_standings = {'season': [],
                         'round': [],
                         'constructor': [],
                         'constructor_points': [],
                         'constructor_wins': [],
                         'constructor_standings_pos': []}
# query API


for n in tqdm(list(range(len(constructor_rounds))), desc=f'Constructors Year'):
    inner = tqdm(total=len(constructor_rounds[n][1]), leave=False, desc=f'Season', ncols=90, colour='Green')
    for i in constructor_rounds[n][1]:
        inner.update()
        url = 'https://ergast.com/api/f1/{}/{}/constructorStandings.json'
        r = requests.get(url.format(constructor_rounds[n][0], i, verify=verify_ssl))
        json = r.json()
        try:
            for item in json['MRData']['StandingsTable']['StandingsLists'][0]['ConstructorStandings']:
                try:
                    constructor_standings['season'].append(
                        int(json['MRData']['StandingsTable']['StandingsLists'][0]['season']))
                except:
                    constructor_standings['season'].append(None)

                try:
                    constructor_standings['round'].append(
                        int(json['MRData']['StandingsTable']['StandingsLists'][0]['round']))
                except:
                    constructor_standings['round'].append(None)

                try:
                    constructor_standings['constructor'].append(item['Constructor']['constructorId'])
                except:
                    constructor_standings['constructor'].append(None)

                try:
                    constructor_standings['constructor_points'].append(int(item['points']))
                except:
                    constructor_standings['constructor_points'].append(None)

                try:
                    constructor_standings['constructor_wins'].append(int(item['wins']))
                except:
                    constructor_standings['constructor_wins'].append(None)

                try:
                    constructor_standings['constructor_standings_pos'].append(int(item['position']))
                except:
                    constructor_standings['constructor_standings_pos'].append(None)
        except IndexError:
            pass
        inner.close()

constructor_standings = pd.DataFrame(constructor_standings)

constructor_standings = lookup(constructor_standings, 'constructor', 'constructor_points')
constructor_standings = lookup(constructor_standings, 'constructor', 'constructor_wins')
constructor_standings = lookup(constructor_standings, 'constructor', 'constructor_standings_pos')

constructor_standings.drop(
    ['constructor_points_after_race', 'constructor_wins_after_race', 'constructor_standings_pos_after_race'],
    axis=1, inplace=True)

connection = sqlite3.connect(database_filename)

existing_query = "SELECT * FROM d_constructor_standings_table WHERE season IN ({}) AND round IN ({}) AND constructor IN ({})".format(
    ','.join(map(str, constructor_standings['season'])),
    ','.join(map(str, constructor_standings['round'])),
    ','.join(map(lambda x: "'" + x + "'", constructor_standings['constructor']))
)


existing_constructor_standings_data = pd.read_sql_query(existing_query, connection)
new_constructor_standings_records = constructor_standings[~constructor_standings.apply(tuple, axis=1).isin(existing_constructor_standings_data.apply(tuple, axis=1))]

# Append the 'results' DataFrame to the 'races.db' database as a new table
new_constructor_standings_records.to_sql('d_constructor_standings_table', connection, if_exists='append', index=False)

# Close the database connection
connection.close()

#################################################################################### Qualifying

from bs4 import BeautifulSoup

qualifying_results = pd.DataFrame()


def get_time(row):
    if not pd.isna(row['Q3']) and row['Q3'] != 'DNF' and row['Q3'] != 'DNS':
        return row['Q3']
    elif not pd.isna(row['Q2']) and row['Q2'] != 'DNF' and row['Q2'] != 'DNS':
        return row['Q2']
    elif not pd.isna(row['Q1']) and row['Q1'] != 'DNF' and row['Q1'] != 'DNS':
        return row['Q1']
    else:
        return '3:30.00'


# Qualifying times are only available from 1983
if startY < 1983:
    startY = 1983
for year in tqdm(list(range(startY, endY)), desc=f'Qualifying Year'):
    url = 'https://www.formula1.com/en/results.html/{}/races.html'
    r = requests.get(url.format(year), verify=verify_ssl)
    soup = BeautifulSoup(r.text, 'html.parser')

    # find links to all circuits for a certain year
    page_races_link = soup.find_all('a', attrs={'class': "underline underline-offset-normal decoration-1 decoration-greyLight hover:decoration-brand-primary"})
    year_links = []

    for page in page_races_link:
        link = page.get('href')

        if 'races/' in link:
            year_links.append(link)
    # for each circuit, switch to the starting grid page and read table
    #year_links.append('races/1243/netherlands/qualifying')

    year_df = pd.DataFrame()
    new_url = 'https://www.formula1.com/en/results/{}/{}'
    if year == 2023:
        del year_links[5]

    for n, link in list(enumerate(year_links)):
        try:
            if year >= datetime.now().year:
                if n + 1 > until_round:
                    break

            print(year, n+1)
            print(link)

            link = link.replace('race-result', 'qualifying')

            df = pd.read_html(new_url.format(year, link))
            df = df[0]


            if 'Q1' not in df.columns:
                print(f'no Q1 - 3 in {year} round {n+1}')
                link = link.replace('qualifying.html', 'starting-grid.html')
                df = pd.read_html(new_url.format(link))
                df = df[0]
                df['season'] = year
                df['round'] = n + 1
                for col in df:
                    if 'Unnamed' in col:
                        df.drop(col, axis=1, inplace=True)
                merged_df = df
                print(merged_df)

            else:
                df['Time'] = df.apply(get_time, axis=1)
                df = df.drop(columns=['Q1', 'Q2', 'Q3', 'Laps'])
                df['season'] = year
                df['round'] = n + 1

                for col in df:
                    if 'Unnamed' in col:
                        df.drop(col, axis=1, inplace=True)

                link = link.replace('qualifying.html', 'starting-grid.html')
                df1 = pd.read_html(new_url.format(year, link))
                df1 = df1[0]
                try:
                    df1 = df1.drop(columns='Time')
                except KeyError:
                    pass
                df1['season'] = int(year)
                df1['round'] = int(n + 1)
                for col in df1:
                    if 'Unnamed' in col:
                        df1.drop(col, axis=1, inplace=True)

                # merged_df = pd.merge(df, df1, on='Driver', how='outer').sort_values(by='Pos_y')
                merged_df = pd.merge(df, df1, on=['Driver', 'No', 'Car', 'season', 'round'], how='outer')

                merged_df = merged_df.rename(columns={'Pos_x': 'Pos', 'Pos_y': 'Pos_df1'})
                merged_df = merged_df.reset_index(drop=True)
                merged_df['Pos'] = merged_df['Pos_df1']
                merged_df = merged_df.drop(columns=['Pos_df1'])
                merged_df.sort_values(by='Pos', inplace=True)
                merged_df['Pos'] = range(1, len(merged_df) + 1)
                merged_df['Time'].fillna('3:30.00', inplace=True)
                merged_df['Driver'] = merged_df['Driver'].str.replace("'", "")
                if 'Q1' in merged_df.columns:
                    merged_df = merged_df.drop(columns=['Q1', 'Q2', 'Q3', 'Laps'])
                print(merged_df)

        except ValueError:
            print(f"no quali for year {year} round {n+1}")
            pass

        year_df = pd.concat([year_df, merged_df])

    # concatenate all tables from all years

    qualifying_results = pd.concat([qualifying_results, year_df])

# rename columns

qualifying_results.rename(columns={'Pos': 'grid', 'Driver': 'driver_name', 'Car': 'car',
                                   'Time': 'qualifying_time'}, inplace=True)

try:
    qualifying_results = qualifying_results.drop(columns=['Gap', 'Laps'])
    print('\n\nERROR! USING PRACTICE TIMINGS INSTEAD OF QUALI!\n\n')
except:
    print('---')

# Connect to the database
connection = sqlite3.connect(database_filename)
print('printing quali df')
print(qualifying_results)
# Query to retrieve existing qualifying results data
existing_query = "SELECT * FROM e_qualifying_results_table WHERE season IN ({}) AND round IN ({}) AND driver_name IN ({})".format(
    ','.join(map(str, qualifying_results['season'])),
    ','.join(map(str, qualifying_results['round'])),
    ','.join(map(lambda x: f"'{x}'", qualifying_results['driver_name']))
)

# Read the existing data from the database and select the relevant columns for comparison
existing_qualifying_results_data = pd.read_sql_query(existing_query, connection)
existing_qualifying_results_data = existing_qualifying_results_data[['season', 'round', 'driver_name']]

# Identify new qualifying results records by checking for rows not present in the existing data
new_qualifying_results_records = qualifying_results[~qualifying_results[['season', 'round', 'driver_name']].apply(tuple, axis=1).isin(existing_qualifying_results_data.apply(tuple, axis=1))]

# Append the new qualifying results records to the database
new_qualifying_results_records.to_sql('e_qualifying_results_table', connection, if_exists='append', index=False)

# Close the database connection
connection.close()

#################################################################################### Weather

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

chrome_driver_path = 'D:\\Dropbox\\PycharmProjects\\LiveDashboard\\chromedriver.exe'
chrome_driver_path = '/Users/lele/Dropbox/PycharmProjects/rc_reports v2/chromedriver'

chrome_options = Options()
# chrome_options.add_argument('--headless')
# chrome_options.add_argument('--disable-gpu')

weather = races.iloc[:, [0, 1, 2]]

info = []

# read wikipedia tables

for link in races.url:
    try:
        df = pd.read_html(link)[0]
        if 'Weather' in list(df.iloc[:, 0]):
            n = list(df.iloc[:, 0]).index('Weather')
            info.append(df.iloc[n, 1])
        else:
            df = pd.read_html(link)[1]
            if 'Weather' in list(df.iloc[:, 0]):
                n = list(df.iloc[:, 0]).index('Weather')
                info.append(df.iloc[n, 1])
            else:
                df = pd.read_html(link)[2]
                if 'Weather' in list(df.iloc[:, 0]):
                    n = list(df.iloc[:, 0]).index('Weather')
                    info.append(df.iloc[n, 1])
                else:
                    df = pd.read_html(link)[3]
                    if 'Weather' in list(df.iloc[:, 0]):
                        n = list(df.iloc[:, 0]).index('Weather')
                        info.append(df.iloc[n, 1])
                    else:
                        driver = webdriver.Chrome(executable_path=chrome_driver_path, options=chrome_options)
                        driver.get(link)

                        # click language button
                        button = driver.find_element_by_link_text('Italiano')
                        button.click()

                        # find weather in italian with selenium

                        clima = driver.find_element_by_xpath(
                            '//*[@id="mw-content-text"]/div/table[1]/tbody/tr[9]/td').text
                        info.append(clima)

    except:
        info.append('not found')

# append column with weather information to dataframe

weather['weather'] = info

# set up a dictionary to convert weather information into keywords

weather_dict = {'weather_warm': ['soleggiato', 'clear', 'warm', 'hot', 'sunny', 'fine', 'mild', 'sereno'],
                'weather_cold': ['cold', 'fresh', 'chilly', 'cool'],
                'weather_dry': ['dry', 'asciutto'],
                'weather_wet': ['showers', 'wet', 'rain', 'pioggia', 'damp', 'thunderstorms', 'rainy'],
                'weather_cloudy': ['overcast', 'nuvoloso', 'clouds', 'cloudy', 'grey', 'coperto']}

# map new df according to weather dictionary

weather_df = pd.DataFrame(columns=weather_dict.keys())
for col in weather_df:
    weather_df[col] = weather['weather'].map(
        lambda x: 1 if any(i in weather_dict[col] for i in x.lower().split()) else 0)

weather_info = pd.concat([weather, weather_df], axis=1)

connection = sqlite3.connect(database_filename)

existing_query = "SELECT * FROM f_weather_info_table WHERE season IN ({}) AND round IN ({}) AND circuit_id IN ({})".format(
    ','.join(map(str, weather_info['season'])),
    ','.join(map(str, weather_info['round'])),
    ','.join(map(lambda x: "'" + x + "'", weather_info['circuit_id']))
)


existing_weather_info_data = pd.read_sql_query(existing_query, connection)
new_weather_info_records = weather_info[~weather_info.apply(tuple, axis=1).isin(existing_weather_info_data.apply(tuple, axis=1))]

# Append the 'results' DataFrame to the 'races.db' database as a new table
new_weather_info_records.to_sql('f_weather_info_table', connection, if_exists='append', index=False)

# Close the database connection
connection.close()