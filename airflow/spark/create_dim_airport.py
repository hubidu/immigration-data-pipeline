from pyspark.sql.functions import *
from pyspark.sql.types import *
from udf import *
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()

    dfImmigrationFinal = spark.read.parquet(
        's3://93df6/immigration_load/immigration')
    dfAirportsFinal = spark.read.parquet(
        's3://93df6/immigration_load/airports')

    dfAirport = dfImmigrationFinal \
        .select(col('destination_city').alias('city'), col('destination_state').alias('state')) \
        .drop_duplicates()

    dfAirport = dfAirport.withColumn('airport_id', sha2(concat(*(col(c).cast("string") for c in dfAirport.columns)), 256)) \
        .join(dfAirportsFinal, ['city', 'state']) \
        .drop_duplicates(['city', 'state']) \
        .drop('city', 'state')

    dfAirport.write.mode("overwrite").parquet(
        "s3://93df6/immigration_analytics/airport")


if __name__ == '__main__':
    main()
