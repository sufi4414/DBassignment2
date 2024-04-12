import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
def average_rating(input_path):
  try:
    conf = SparkConf().setAppName("Part 1 Question 3")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    df = spark.read.option("header", True).csv(input_path)
    df = df.withColumn("Rating", df["Rating"].cast('float'))
    avg_df = (
        df
        .groupBy("City")
        .agg(F.avg("Rating").alias("AverageRating"))
        .orderBy("City")  # Sort by 'City' in ascending order
    )
    # avg_df.show()

    top3_df = (
        avg_df
        .orderBy(F.desc("AverageRating"))
        .limit(3)

    )
    # top3_df.show()

    bot3_df = (
        avg_df
        .orderBy("AverageRating")
        .limit(3)

    )
    # bot3_df.show()

    union_df = top3_df.union(bot3_df)
    union_df.show()

  finally:
    sc.stop()

average_rating(input_path)
