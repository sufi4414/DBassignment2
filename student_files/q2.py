import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
def worst_and_best_restaurant(input_path):

  try:

    conf = SparkConf().setAppName("Part 1 Question 2")
    sc = SparkContext(conf=conf)

    spark = SparkSession(sc)

    df = spark.read.option("header", True).csv(input_path) # Replace with hdfs input_path

    df = df.filter(df['Price Range'].isNotNull())
    df = df.withColumn("Rating", df["Rating"].cast('float'))

    # best_df = df.groupBy("City", "Price Range").agg(F.max("Rating").alias("Best_Rating"))
    # worst_df = df.groupBy("City", "Price Range").agg(F.min("Rating").alias("Worst_Rating"))
    best_df = (
      df
      .groupBy("City", "Price Range")
      .agg(F.max("Rating"))
      .withColumn("Rating", F.col("max(Rating)"))
      .orderBy("City")  # Sort by 'City' in ascending order
    )

    worst_df = (
        df
        .groupBy("City", "Price Range")
        .agg(F.min("Rating"))
        .withColumn("Rating", F.col("min(Rating)"))
        .orderBy("City")  # Sort by 'City' in ascending order
    )
    # print("Best restaurants....")
    # best_df.show()
    # print("Worst restaurants....")
    # worst_df.show()

    union_df = best_df.union(worst_df)
    # print("Union df...")
    # union_df.show()

    combined_df = union_df.join(df, on=["City", "Price Range", "Rating"], how="inner")
    combined_df = (
    combined_df.dropDuplicates(["Price Range", "City", "Rating"])
    .select(
        "_c0",
        "Name",
        "City",
        "Cuisine Style",
        "Ranking",
        "Rating",
        "Price Range",
        "Number of Reviews",
        "Reviews",
        "URL_TA",
        "ID_TA",
    )
    .sort(F.col("City").asc(), F.col("Price Range").asc(), F.col("Rating").desc())
  )

    combined_df.show()

  finally:
    sc.stop()



worst_and_best_restaurant(input_path)

