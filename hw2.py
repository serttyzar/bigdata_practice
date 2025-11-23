from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, coalesce
import geopandas as gpd
import folium
import pandas as pd
import webbrowser
import os

spark = SparkSession.builder.appName("BikeTripAnalysis").getOrCreate()

bike_df = spark.read.csv("201902-citibike-tripdata.csv", header=True, inferSchema=True)
zones_gdf = gpd.read_file("NYC Taxi Zones.geojson").to_crs(epsg=4326)

start_counts = bike_df.groupBy(
    "start station name", 
    "start station latitude", 
    "start station longitude") \
    .agg(count("*").alias("start_count")
)

end_counts = bike_df.groupBy(
    "end station name", 
    "end station latitude", 
    "end station longitude") \
    .agg(count("*").alias("end_count")
)

from pyspark.sql.functions import lit

station_counts = start_counts.join(
    end_counts,
    (col("start station name") == col("end station name")) &
    (col("start station latitude") == col("end station latitude")) &
    (col("start station longitude") == col("end station longitude")),
    "outer"
).select(
    coalesce(col("start station name"), col("end station name")).alias("station_name"),
    coalesce(col("start station latitude"), col("end station latitude")).alias("lat"),
    coalesce(col("start station longitude"), col("end station longitude")).alias("lon"),
    coalesce(col("start_count"), lit(0)).alias("start_count"),
    coalesce(col("end_count"), lit(0)).alias("end_count")
)

stations_pd = station_counts.toPandas()
stations_pd['total'] = stations_pd['start_count'] + stations_pd['end_count']
stations_gdf = gpd.GeoDataFrame(
    stations_pd,
    geometry=gpd.points_from_xy(stations_pd.lon, stations_pd.lat),
    crs="EPSG:4326"
)

stations_with_zones = gpd.sjoin(stations_gdf, zones_gdf, how="left", predicate="within")
zone_stats = stations_with_zones.groupby('objectid')[['start_count', 'end_count', 'total']].sum().reset_index()
zone_stats['objectid'] = zone_stats['objectid'].astype(int)
zones_gdf['objectid'] = zones_gdf['objectid'].astype(int)
zones_gdf = zones_gdf.merge(zone_stats, on='objectid', how='left')
zones_gdf['start_count'] = zones_gdf['start_count'].fillna(0).astype(float)
zones_gdf['end_count'] = zones_gdf['end_count'].fillna(0).astype(float)
zones_gdf['total'] = zones_gdf['total'].fillna(0).astype(float)

map_file = 'bike_trips_map.html'
m = folium.Map(location=[40.7128, -74.0060], zoom_start=12, tiles='CartoDB positron')

choropleth = folium.Choropleth(
    geo_data=zones_gdf,
    data=zones_gdf,
    columns=['objectid', 'total'],
    key_on='feature.properties.objectid',
    fill_color='YlOrRd',
    fill_opacity=0.7,
    line_opacity=0.8,
    line_color='white',
    legend_name='Общее количество поездок',
    highlight=True,
    bins=7,
    name='Зоны'
).add_to(m)

folium.features.GeoJson(
    zones_gdf,
    name='Информация о зонах',
    style_function=lambda x: {
        'fillColor': 'transparent',
        'color': 'transparent',
        'weight': 0
    },
    tooltip=folium.features.GeoJsonTooltip(
        fields=['objectid', 'zone', 'start_count', 'end_count', 'total'],
        aliases=['ID зоны:', 'Название:', 'Начало поездок:', 'Конец поездок:', 'Всего:'],
        localize=True
    )
).add_to(m)

stations_group = folium.FeatureGroup(name='Станции', show=False)

for idx, row in stations_pd.iterrows():
    folium.CircleMarker(
        location=[row['lat'], row['lon']],
        radius=3,
        fill=True,
        fill_opacity=0.4,
        weight=1,
        popup=f"<b>{row['station_name']}</b><br>Начало: {row['start_count']}<br>Конец: {row['end_count']}<br>Всего: {row['total']}"
    ).add_to(stations_group)

stations_group.add_to(m)

folium.LayerControl(collapsed=False).add_to(m)

m.save(map_file)
webbrowser.open('file://' + os.path.abspath(map_file))

print("\nТоп 10 станций по общему количеству поездок):")
stations_sorted = stations_pd.nlargest(10, 'total')[['station_name', 'start_count', 'end_count', 'total']]
print(stations_sorted.to_string(index=False))

print("\nЗоны по убыванию количества поездок:")
zones_sorted = zones_gdf[zones_gdf['total'] > 0].sort_values('total', ascending=False)[['objectid', 'zone', 'borough', 'start_count', 'end_count', 'total']]
print(zones_sorted.to_string(index=False))

print(f"\nВсего зон с поездками: {len(zones_sorted)}")
print(f"Всего станций: {len(stations_pd)}")
print(f"Общее количество поездок: {int(stations_pd['total'].sum() / 2)}")

spark.stop()