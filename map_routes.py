
import osmnx as ox
import networkx as nx
import geopandas as gpd

# Define the location (e.g., California, USA) for downloading the road network
location = "California, USA"
G = ox.graph_from_place(location, network_type="drive", custom_filter='["highway"="motorway"]')

# Convert the Networkx graph to a GeoDataFrame
gdf = ox.graph_to_gdfs(G, nodes=False, edges=True)

# Save the GeoDataFrame to a CSV file
csv_file = '/home/oseloka/pyprojects/airflow/tolldata-etl/staging/interstate-highways.csv'
gdf.to_csv(csv_file, index=False, encoding='utf-8')