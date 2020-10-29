from pyspark.sql import SparkSession, Row, Window
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, StringType
from pyspark.sql.functions import UserDefinedFunction, collect_list, explode, col, arrays_zip, \
    monotonically_increasing_id

from utils import compute_diff


stations = ({
    "internal_bus_station_id": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    "public_bus_station": ["BAutogara", "BVAutogara", "SBAutogara", "CJAutogara", "MMAutogara", "ISAutogara",
                           "CTAutogara", "TMAutogara", "BCAutogara", "MSAutogara"]
})

trips = ({
    'ORIGIN': ['B', 'BV', 'TM', 'CJ'],
    'TRIPTIMES': [
        ['2020-03-01 10:10:00', '2020-03-01 12:20:10', '2020-03-01 15:10:00'],
        ['2020-03-01 10:10:00', '2020-03-01 12:20:10', '2020-03-01 15:10:00'],
        ['2020-04-01 10:10:00', '2020-04-01 12:20:10', '2020-04-01 15:10:00'],
        ['2020-05-01 10:10:00', '2020-03-01 12:20:10', '2020-05-01 15:10:00']],
    'DESTINATION': ['MM', 'IS', 'CT', 'BC'],
    'INTERNAL_BUS_STATION_IDS': [
        [0, 2, 4],
        [1, 8, 5],
        [7, 2, 6],
        [3, 9, 8]
    ]
})


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .appName("Deloiite challenge") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sc = spark._sc

    schema_stations = StructType([
        StructField('internal_bus_station_id', ArrayType(IntegerType())),
        StructField('public_bus_station', ArrayType(StringType()))
    ])

    schema_trips = StructType([
        StructField('ORIGIN', ArrayType(StringType())),
        StructField('DESTINATION', ArrayType(StringType())),
        StructField('INTERNAL_BUS_STATION_IDS', ArrayType(ArrayType(IntegerType()))),
        StructField('TRIPTIMES', ArrayType(ArrayType(StringType())))
    ])

    rdd_stations = sc.parallelize([stations])
    stations = spark.createDataFrame(rdd_stations, schema_stations)
    # stations.show()

    rdd_trips = sc.parallelize([trips])
    trips = spark.createDataFrame(rdd_trips, schema_trips)
    # trips.show()

    ft_stations = stations.withColumn("tmp", arrays_zip("internal_bus_station_id", "public_bus_station"))\
        .withColumn("tmp", explode("tmp"))\
        .select(col("tmp.internal_bus_station_id"), col("tmp.public_bus_station"))

    ft_trips = trips.withColumn("tmp", arrays_zip("DESTINATION", "INTERNAL_BUS_STATION_IDS", "ORIGIN", "TRIPTIMES"))\
        .withColumn("tmp", explode("tmp"))\
        .select(col("tmp.ORIGIN"), col("tmp.DESTINATION"), col("tmp.INTERNAL_BUS_STATION_IDS"), col("tmp.triptimes"))

    tmp = ft_trips.withColumn("INTERNAL_BUS_STATION_IDS", explode("INTERNAL_BUS_STATION_IDS"))\
        .withColumn("idx", monotonically_increasing_id())

    tmp = tmp.join(ft_stations, tmp.INTERNAL_BUS_STATION_IDS == ft_stations.internal_bus_station_id).sort("idx")
    tmp = tmp.select("ORIGIN", "DESTINATION", "PUBLIC_BUS_STATION", "TRIPTIMES")

    window = Window.partitionBy("ORIGIN")
    tmp = tmp.withColumn("PUBLIC_BUS_STATION", collect_list("PUBLIC_BUS_STATION").over(window)).distinct()

    compute_time = UserDefinedFunction(compute_diff, StringType())

    final = tmp.withColumn("duration", compute_time(col("triptimes"))) \
        .select(col("origin"), col("destination"), col("public_bus_station"), col("duration"))

    final.show()