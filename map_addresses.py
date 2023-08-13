import csv
import requests
import pandas as pd

api_key = "AIzaSyAuGNXnlxcre8CWidwQK6vC2mDdwL9nWwM"
search_query = "highways in California"

url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={search_query}&key={api_key}"

response = requests.get(url)
data = response.json()

results = data['results']

# Initialize an empty list to store the tabular data
table_data = []

# Iterate through each place in 'results'
for place in results:
    name = place.get('name', None)
    address = place.get('formatted_address', None)
    lat = place['geometry']['location'].get('lat', None)
    lng = place['geometry']['location'].get('lng', None)
    rating = place.get('rating', None)
    user_ratings_total = place.get('user_ratings_total', None)
    business_status = place.get('business_status', None)
    types = ', '.join(place.get('types', []))

    # Append the extracted data to the 'table_data' list
    table_data.append([name, address, lat, lng, rating, user_ratings_total, business_status, types])

# Create a DataFrame from the 'table_data' list
df = pd.DataFrame(table_data, columns=['Name', 'Address', 'Latitude', 'Longitude', 'Rating', 'User Ratings Total', 'Business Status', 'Types'])

print(df)
# Load dataframe into a csv file
csv_file = '/home/oseloka/pyprojects/airflow/tolldata-etl/staging/highway-patrol.csv'

df.to_csv(csv_file, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)