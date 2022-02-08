from pyspark.sql.functions import *
from pyspark.sql.types import *
from udf import *
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()

    dfImmigrationFinal = spark.read.parquet(
        's3://93df6/immigration_load/immigration')
    dfCitiesFinal = spark.read.parquet('s3://93df6/immigration_load/cities')

    dfDestination = dfImmigrationFinal \
        .select(col('destination_city').alias('city'), col('destination_state').alias('state')) \
        .drop_duplicates()

    dfDestination = dfDestination.withColumn('destination_id', sha2(concat(*(col(c).cast("string") for c in dfDestination.columns)), 256)) \
        .join(dfCitiesFinal, ['city', 'state'], 'left')

    dfDestination.write.mode("overwrite").parquet(
        "s3://93df6/immigration_analytics/destination")


if __name__ == '__main__':
    main()
