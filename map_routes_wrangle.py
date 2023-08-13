import csv
import pandas as pd
import geopandas as gpd
from shapely.wkt import loads
from shapely.geometry import LineString

# Read the CSV file into a DataFrame
csv_file = '/home/oseloka/pyprojects/airflow/tolldata-etl/staging/interstate-highways.csv'
df = pd.read_csv(csv_file)

# Eliminating columns except required columns 
required_columns = ['name', 'maxspeed', 'length', 'geometry']
df = df[required_columns]

# Perform data validation - Check for missing values in the 'geometry' column
missing_geometry = df[df['geometry'].isnull()]
if not missing_geometry.empty:
    print("Warning: There are rows with missing geometry ddata.")
    print(missing_geometry)

# Convert the 'geometry' column from WKT to LineString objects
def wkt_to_linestring(wkt_str):
    # Remove spaces between coordinates in the WKT string
    #wkt_str = wkt_str.replace(" ", ",")
    #wkt_str = wkt_str.replace(",,", ",")
    try:
        return loads(wkt_str)
    except Exception as e:
        print(e)

df['geometry'] = df['geometry'].apply(wkt_to_linestring)

# Create a new DataFrame to hold the exploded LineString
new_rows = []
for index, row in df.iterrows():
    line_string = row['geometry']
    coords = list(line_string.coords)
    for i in range(1, len(coords)):
        new_row = row.copy()
        new_row['geometry'] = LineString([coords[i - 1], coords[i]])
        new_rows.append(new_row)

# Create a new DataFrame from the list of new rows
gdf_points = gpd.GeoDataFrame(new_rows, geometry='geometry')

# Reset the index
gdf_points.reset_index(drop=True, inplace=True)

# Extract latitude and longitude points into separate columns
gdf_points['Latitude'] = gdf_points['geometry'].apply(lambda point: point.coords[0][1])
gdf_points['longitude'] = gdf_points['geometry'].apply(lambda point: point.coords[0][0])

# Drop the original geometry column as it is no longer needed. 
gdf_points.drop(columns=['geometry'], inplace=True)

# Add a new column to hold ID's for each segment
segment_ids = {}
current_id = 4000
def assign_segment_id(segment_name):
    global current_id
    if segment_name not in segment_ids:
        segment_ids[segment_name] = current_id
        current_id += 1
    return segment_ids[segment_name]

gdf_points['segment_ID'] = gdf_points['name'].apply(assign_segment_id)

# Display first few roows of the DataFrame to ensure data is as expected
print(gdf_points.head())

# Save the cleaned data into a new csv file
cleaned_csv = '/home/oseloka/pyprojects/airflow/tolldata-etl/staging/cleaned-interstate-highways.csv'
gdf_points.to_csv(cleaned_csv, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)