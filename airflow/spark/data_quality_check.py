from pyspark.sql.functions import *
from pyspark.sql.types import *
from udf import *
from pyspark.sql import SparkSession


def main():
    """
    Run quality checks on the final analytics schema
    """
    spark = SparkSession.builder.getOrCreate()

    dfImmigration = spark.read.parquet(
        f"s3://93df6/immigration_analytics/immigration")
    dfImmigration.createOrReplaceTempView('immigration')
    dfAirport = spark.read.parquet(f"s3://93df6/immigration_analytics/airport")
    dfAirport.createOrReplaceTempView('airport')
    dfDestination = spark.read.parquet(
        f"s3://93df6/immigration_analytics/destination")
    dfDestination.createOrReplaceTempView('destination')
    dfFlight = spark.read.parquet(f"s3://93df6/immigration_analytics/flight")
    dfFlight.createOrReplaceTempView('flight')
    dfPerson = spark.read.parquet(f"s3://93df6/immigration_analytics/person")
    dfPerson.createOrReplaceTempView('person')

    def check_airports_are_unique():
        result = dfAirport.groupBy('name').count().orderBy(desc('count'))
        result.show()
        result = result.first()
        if not result[1] == 1:
            raise Exception("Airport name must be unique")

    def check_age_is_not_lte_0():
        result = dfImmigration.where(col('age') <= 0)
        result.show()
        result = result.first()
        if not result is None:
            raise Exception("Age must not be <= 0")

    def check_results_for_age_gt_30():
        result = dfImmigration.where(col('age') > 30).count()
        assert result > 30, "Expected results for age > 30"

    def check_airline_flightnumber_are_unique():
        result = dfFlight.groupBy(
            'airline', 'flightnumber').count().orderBy(desc('count'))
        result.show()
        result = result.first()
        if not result[2] == 1:
            raise Exception("Flight airline and flightnumber must be unique")

    def check_uk_in_top_origin_countries():
        result = spark.sql("""
        select country, count(*) as total
        from immigration i
        join person p on (i.person_id = p.person_id)
        where arrival_date = '2016-04-20'
        group by country
        order by total desc
        """)
        result.show()

        first10 = result.take(10)
        if not "united kingdom" in map(
                lambda x: x[0], first10):
            raise Exception(
                "Expected united kingdom in top 10 origin countries")

    check_airports_are_unique()
    check_age_is_not_lte_0()
    check_results_for_age_gt_30()
    check_airline_flightnumber_are_unique()
    check_uk_in_top_origin_countries()


if __name__ == '__main__':
    main()
