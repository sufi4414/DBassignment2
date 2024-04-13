import sys
from pyspark.sql import SparkSession,functions as F
from pyspark import SparkContext, SparkConf
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
input_path = "hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn)
output_path = "hdfs://%s:9000/assignment2/output/question3/" % (hdfs_nn)
def average_rating(input_path,output_path):
  try:
    # conf = SparkConf().setAppName("Part 1 Question 3")
    # sc = SparkContext(conf=conf)
    # spark = SparkSession(sc)
    # df = spark.read.option("header", True).csv(input_path)
    df = (
          spark.read.option("header", True)
          .option("inferSchema", True)
          .option("delimiter", ",")
          .option("quotes", '"')
          .csv(input_path)
        )  
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
    top3_df = top3_df.limit(3).withColumn("RatingGroup", F.lit("Top"))
    # top3_df.show()

    bot3_df = (
        avg_df
        .orderBy("AverageRating")
        .limit(3)

    )
    bot3_df = bot3_df.limit(3).withColumn("RatingGroup", F.lit("Bottom"))
    # bot3_df.show()

    union_df = top3_df.union(bot3_df)
    union_df.show()
    union_df.write.csv(output_path, header=True)

  finally:
    spark.stop()

average_rating(input_path,output_path)
