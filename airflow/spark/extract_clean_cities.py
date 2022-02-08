from pyspark.sql.functions import *
from pyspark.sql.types import *
from udf import *
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()

    dfCities = spark.read.option('delimiter', ';').option('header', True).csv(
        's3://93df6/immigration/us-cities-demographics.csv', sep=';')

    dfCitiesFinal = dfCities.filter(col('Race') == 'White').select(
        lower(col('City')).alias('city'), col(
            'Median Age').alias('median_age'),
        lower(col('State Code')).alias('state'), col(
            'Total Population').alias('total_population'),
        (round(col('Male Population')/col('Total Population'), 2)
         ).alias('male_pct'), col('Average Household Size').alias('household_size'),
        (round(col('Count')/col('Total Population'), 2)).alias('white_pct')
    ).drop_duplicates()

    dfCitiesFinal.show(3)
    dfCitiesFinal.write.mode("overwrite").parquet(
        "s3://93df6/immigration_load/cities")


if __name__ == '__main__':
    main()
