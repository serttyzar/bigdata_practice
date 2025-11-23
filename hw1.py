import csv
from math import radians, sin, cos, sqrt, atan2
from pyspark import SparkContext, SparkConf
import pandas as pd


def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0

    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    
    distance = R * c
    return distance


def parse_csv_line(line):
    reader = csv.reader([line])
    row = next(reader)
    
    if len(row) < 14:
        return None
    
    try:
        place_id = row[0]
        name = row[1]
        latitude = float(row[13])  # Latitude_WGS84
        longitude = float(row[12])  # Longitude_WGS84
        return (place_id, name, latitude, longitude)
    except:
        return None


def main():
    conf = SparkConf().setAppName("PlacesDistance").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    
    target_lat = 55.751244
    target_lon = 37.618423
    
    lines = sc.textFile("places.csv")
    
    places_rdd = lines.map(parse_csv_line) \
                      .filter(lambda x: x is not None)
    
    places_rdd.cache()
    
    distances_from_point = places_rdd.map(
        lambda x: (x[0], x[1], haversine(target_lat, target_lon, x[2], x[3]))
    )

    data = distances_from_point.take(10)
    df = pd.DataFrame(data, columns=['ID', 'Название', 'Расстояние_км'])
    df['Расстояние_км'] = df['Расстояние_км'].round(3)
    print("\nПервые 10 заведений с расстоянием от точки (55.751244, 37.618423)")
    print(df)

    all_pairs = places_rdd.cartesian(places_rdd)
    
    distances_between_all = all_pairs.filter(lambda x: x[0][0] < x[1][0]) \
        .map(lambda x: (
            (x[0][0], x[0][1]),
            (x[1][0], x[1][1]),
            haversine(x[0][2], x[0][3], x[1][2], x[1][3])  # расстояние
        ))
    

    flat_data = [(p1[0], p1[1], p2[0], p2[1], dist) for p1, p2, dist in distances_between_all.take(10)]
    df= pd.DataFrame(
        flat_data,
        columns=['ID_1', 'Название_1', 'ID_2', 'Название_2', 'Расстояние_км']
    )
    df['Расстояние_км'] = df['Расстояние_км'].round(3)
    print("\nПервые 10 пар заведений с расстоянием между ними:")
    print(df)
    
    closest_10 = distances_between_all.takeOrdered(10, key=lambda x: x[2])
    
    flat_data = [(p1[0], p1[1], p2[0], p2[1], dist) for p1, p2, dist in closest_10]
    df= pd.DataFrame(
        flat_data,
        columns=['ID_1', 'Название_1', 'ID_2', 'Название_2', 'Расстояние_км']
    )
    df['Расстояние_км'] = df['Расстояние_км'].round(3)
    print("\nПервые 10 пар заведений с расстоянием между ними:")
    print(df)

    farthest_10 = distances_between_all.takeOrdered(10, key=lambda x: -x[2])
    
    flat_data = [(p1[0], p1[1], p2[0], p2[1], dist) for p1, p2, dist in farthest_10]
    df= pd.DataFrame(
        flat_data,
        columns=['ID_1', 'Название_1', 'ID_2', 'Название_2', 'Расстояние_км']
    )
    df['Расстояние_км'] = df['Расстояние_км'].round(3)
    print("\nПервые 10 пар заведений с расстоянием между ними:")
    print(df)
    
    sc.stop()


if __name__ == "__main__":
    main()
