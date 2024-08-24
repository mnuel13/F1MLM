import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import folium


connection = sqlite3.connect('races.db')
# Replace 'your_table_name' with the correct table name in your SQLite database
table_name = 'a_races_table'
# SQL query to select all rows from the table with quotes around the table name
query = f'SELECT * FROM "{table_name}"'
# Use pd.read_sql_query() to read the table into a DataFrame
df = pd.read_sql_query(query, connection)
connection.close()

######################################################################## CHART 1
# Calculate the frequency of each circuit
circuit_counts = df['circuit_id'].value_counts().reset_index()
circuit_counts.columns = ['circuit_id', 'count']

# Get the top 28 circuits by frequency
top_circuits = circuit_counts.head(28)

# Filter the original DataFrame to contain only rows where the circuit is in the top 28
df_filtered = df[df['circuit_id'].isin(top_circuits['circuit_id'])]

# Sort the DataFrame by circuit frequency
df_filtered = df_filtered.merge(top_circuits, on='circuit_id').sort_values(by='count', ascending=False)

# Create the Seaborn stripplot
plt.figure(figsize=(12, 6))
sns.stripplot(x="season", y="circuit_id", data=df_filtered, jitter=False, palette="deep", hue='circuit_id', legend=False)

plt.xlabel('Season')
plt.ylabel('Circuit ID')
plt.title('F1 Circuit Presence Over the Years (Top 28 Circuits)')

plt.savefig('circuit_popularity.png')  # Change the file extension as needed

######################################################################## CHART 2
# Create a base map
# Convert the 'lat' and 'long' columns to float data type
# df = df[df['season'] == 2022]
df['lat'] = pd.to_numeric(df['lat'])
df['long'] = pd.to_numeric(df['long'])

# Create a map centered at a specific location
m = folium.Map(location=[0, 0], zoom_start=2)

# Iterate through the DataFrame rows
for index, row in df.iterrows():
    lat = row['lat']
    long = row['long']
    circuit_name = row['circuit_id']  # Replace with your actual column name

    # Add a marker for each circuit
    folium.Marker([lat, long], tooltip=circuit_name).add_to(m)

# Add lines connecting the circuits in order
# for i in range(1, len(df)):
#     lat1 = df.iloc[i - 1]['lat']
#     long1 = df.iloc[i - 1]['long']
#     lat2 = df.iloc[i]['lat']
#     long2 = df.iloc[i]['long']
#
#     folium.PolyLine([(lat1, long1), (lat2, long2)], color='blue').add_to(m)


# Display the map
m.save('circuit_map.html')


######################################################################## CHART 3
connection = sqlite3.connect('races.db')
# Replace 'your_table_name' with the correct table name in your SQLite database
table_name = 'b_results_table'
# SQL query to select all rows from the table with quotes around the table name
query = f'SELECT * FROM "{table_name}"'
# Use pd.read_sql_query() to read the table into a DataFrame
results = pd.read_sql_query(query, connection)
results = results[results['season'] == 2022]
connection.close()

# Filter the DataFrame to include only rows where Grid 1 equals Podium 1
filtered_results = results[(results['grid'] == 1) & (results['podium'] == 1)]

# Group by 'circuit_id' and calculate the percentage
correlation_data = filtered_results.groupby('circuit_id').size().reset_index(name='success_count')
total_races = results.groupby('circuit_id').size().reset_index(name='total_races')
correlation_data = correlation_data.merge(total_races, on='circuit_id', how='left')
correlation_data['success_percentage'] = (correlation_data['success_count'] / correlation_data['total_races']) * 100

# Sort by success_percentage in descending order and keep the top 30
correlation_data = correlation_data.sort_values(by='success_percentage', ascending=False).head(30)

# Create a bar graph
plt.figure(figsize=(12, 6))
plt.bar(correlation_data['circuit_id'], correlation_data['success_percentage'], color='skyblue')
plt.title('Top 30 Circuits: Percentage of Successful Pole Position (Grid 1) to Winning (Podium 1)')
plt.xlabel('Circuit ID')
plt.ylabel('Success Percentage (%)')
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.savefig('correlation_quali_podium.png')  # Change the file extension as needed


