import sys
from pyspark.sql import SparkSession, functions as F
from pyspark import SparkContext, SparkConf
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
input_path = "hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn)
output_path = "hdfs://%s:9000/assignment2/output/question4/" % (hdfs_nn)
def count_restaurant(input_path,output_path):
  try:
    # conf = SparkConf().setAppName("Part 1 Question 4")
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
    df = df.withColumn("Cuisine Style", F.regexp_replace("Cuisine Style", "^\\[|\\]$", ""))
    df = df.withColumn("Cuisine Style", F.split(F.col("Cuisine Style"), ",\s*"))


    df_exploded = df.withColumn("Cuisine", F.explode("Cuisine Style"))
    df_exploded = df_exploded.withColumn("Cuisine", F.regexp_replace("Cuisine", "'", ""))
    df_exploded = df_exploded.withColumn("Cuisine", F.trim("Cuisine"))


    result_df = (
        df_exploded.groupBy("City", "Cuisine")
        .count()
        .orderBy("City", F.col("count").desc())
    )


    result_df = result_df.select(
        F.col("City").alias("City"),
        F.col("Cuisine").alias("Cuisine"),
        F.col("count")
    )

    result_df.show()
    result_df.write.csv(output_path, header=True)

  finally:
    spark.stop()

count_restaurant(input_path,output_path)
