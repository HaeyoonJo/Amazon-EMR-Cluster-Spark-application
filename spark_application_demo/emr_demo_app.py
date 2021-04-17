from pyspark.sql import SparkSession
from pyspark.sql import Column
from pyspark.sql.functions import upper
from datetime import datetime, date
import argparse

def pre_processing(output_uri):

    spark =  SparkSession.builder.appName("process sample data").getOrCreate()
    rdd = spark.sparkContext.parallelize([
        (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
        (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
        (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
    ])
    df = spark.createDataFrame(rdd, schema=['a', 'b', 'c', 'd', 'e'])
    spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
    if type(df.c) == type(upper(df.c)) == type(df.c.isNull()):
        df_new = df.withColumn('upper_c', upper(df.c))
    df_new
    df_new.repartition(1).write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_uri', help="The URI where output is saved")
    args = parser.parse_args()
    pre_processing(args.output_uri)