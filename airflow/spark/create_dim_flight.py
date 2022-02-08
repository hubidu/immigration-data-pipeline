from pyspark.sql.functions import *
from pyspark.sql.types import *
from udf import *
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()

    dfImmigrationFinal = spark.read.parquet(
        's3://93df6/immigration_load/immigration')

    dfFlight = dfImmigrationFinal \
        .select('airline', 'flightnumber') \
        .drop_duplicates() \
        .filter(~col('airline').isNull())

    dfFlight = dfFlight.withColumn('flight_id', sha2(
        concat(*(col(c).cast("string") for c in dfFlight.columns)), 256))

    dfFlight.write.mode("overwrite").parquet(
        "s3://93df6/immigration_analytics/flight")


if __name__ == '__main__':
    main()
