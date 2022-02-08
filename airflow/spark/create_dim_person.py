from pyspark.sql.functions import *
from pyspark.sql.types import *
from udf import *
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()

    dfImmigrationFinal = spark.read.parquet(
        's3://93df6/immigration_load/immigration')

    dfPerson = dfImmigrationFinal \
        .select(col('person_birth').alias('birth_year'), col('person_gender').alias('gender'), col('person_country').alias('country')) \
        .drop_duplicates()

    dfPerson = dfPerson.withColumn('person_id', sha2(concat(*(col(c).cast("string") for c in dfPerson.columns)), 256)) \

    dfPerson.write.mode("overwrite").parquet(
        "s3://93df6/immigration_analytics/person")


if __name__ == '__main__':
    main()
