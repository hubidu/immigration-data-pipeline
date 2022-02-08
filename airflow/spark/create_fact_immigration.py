from pyspark.sql.functions import *
from pyspark.sql.types import *
from udf import *
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()

    dfImmigrationFinal = spark.read.parquet(
        's3://93df6/immigration_load/immigration')

    dfFactImmigration = dfImmigrationFinal \
        .withColumn('age', when(col('age') <= 0, None).otherwise(col('age'))) \
        .withColumn('destination_id', sha2(concat(col('destination_city'), col('destination_state')), 256)) \
        .withColumn('airport_id', sha2(concat(col('destination_city'), col('destination_state')), 256)) \
        .withColumn('flight_id', sha2(concat(col('airline'), col('flightnumber')), 256)) \
        .withColumn('person_id', sha2(concat(col('person_birth'), col('person_gender'), col('person_country')), 256)) \
        .drop('destination_city', 'destination_state', 'person_birth', 'person_gender', 'person_country', 'airline', 'flightnumber')

    dfFactImmigration.write.mode("overwrite").parquet(
        "s3://93df6/immigration_analytics/immigration")


if __name__ == '__main__':
    main()
