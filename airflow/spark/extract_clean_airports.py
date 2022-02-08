from pyspark.sql.functions import *
from pyspark.sql.types import *
from udf import *
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()

    dfAirports = spark.read.option('header', True).csv(
        's3://93df6/immigration/airport_codes_csv.csv')

    dfAirports = dfAirports.filter(
        col('type') == 'large_airport').filter(col('iso_country') == 'US')
    dfAirports = dfAirports.withColumn(
        'coord', udfExtractLatLongFromCoords(col('coordinates')))
    dfAirports = dfAirports.withColumn(
        'state', udfExtractStateFromRegion(col('iso_region')))
    dfAirports = dfAirports.withColumn(
        'elevation', (col('elevation_ft') * 0.3048).cast('integer'))
    dfAirportsFinal = dfAirports.select(lower(col('name')).alias('name'), lower(col('municipality')).alias(
        'city'), lower(col('state')).alias('state'), 'coord.latitude', 'coord.longitude', 'elevation')
    dfAirportsFinal.show(3)

    dfAirportsFinal.write.mode("overwrite").parquet(
        "s3://93df6/immigration_load/airports")


if __name__ == '__main__':
    main()
