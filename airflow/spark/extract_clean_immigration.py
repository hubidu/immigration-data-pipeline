from pyspark.sql.functions import *
from pyspark.sql.types import *
from udf import *
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()

    dfImmigrationRaw = spark.read.parquet('s3://93df6/immigration/sas_data/')

    I94SelectedColumns = ['cicid', 'dtadfile', 'biryear', 'i94bir', 'gender', 'I94CIT',
                          'I94RES', 'i94port', 'i94mode', 'I94VISA', 'dtaddto', 'airline', 'fltno', 'i94addr']
    I94IntegerColumns = ['cicid', 'biryear', 'i94bir',
                         'I94CIT', 'I94RES', 'i94mode', 'i94visa']

    dfImmigration = dfImmigrationRaw.select(*I94SelectedColumns)

    dfImmigration = correct_to_integer(dfImmigration, I94IntegerColumns)

    dfImmigration = dfImmigration.withColumn(
        'arrival_date', to_date(col('dtadfile'), 'yyyyMMdd'))

    dfImmigration = dfImmigration \
        .withColumn('mode', udfI94Mode(col('i94mode'))) \
        .withColumn('reason', udfI94VISA(col('I94VISA')))

    dfImmigration = dfImmigration \
        .withColumn('resident', udfCityOrResident(col('I94RES')))

    dfImmigration = dfImmigration \
        .withColumn('destination_city', udfDestCity(col('i94port'))) \
        .withColumn('destination_state', udfDestState(col('i94port')))

    dfImmigrationFinal = dfImmigration \
        .select(
            col('cicid').alias('immigration_id'), 'arrival_date', 'mode', 'reason', col(
                'i94bir').alias('age'),
            trim(col('destination_city')).alias('destination_city'), trim(
                col('destination_state')).alias('destination_state'),
            col('biryear').alias('person_birth'), lower(
                col('gender')).alias('person_gender'),
            col('resident').alias('person_country'),
            lower(col('airline')).alias('airline'), col(
                'fltno').alias('flightnumber')
        )
    dfImmigrationFinal.show(1)

    dfImmigrationFinal.write.mode("overwrite").parquet(
        "s3://93df6/immigration_load/immigration")


if __name__ == '__main__':
    main()
