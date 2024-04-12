import sys 
from pyspark.sql import SparkSession,functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.functions import from_json, col, explode, array, array_sort, count
import itertools

# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
input_path = "hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn)
output_path = "hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn)
def find_pairs(input_path, output_path):
  try:
    # conf = SparkConf().setAppName("Part 1 Question 4")
    # sc = SparkContext(conf=conf)
    # spark = SparkSession(sc)
    # df = spark.read.option("header", True).parquet(input_path)
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("delimiter", ",")
        .option("quotes", '"')
        .parquet(input_path)
      )  
    json_schema = ArrayType(StructType([StructField("name", StringType(), nullable=False)]))

    df = df.drop("crew")
    actor1_df = df.withColumn("actor1", F.explode(F.from_json(F.col("cast"), json_schema)))
    actor2_df = df.withColumn("actor2", F.explode(F.from_json(F.col("cast"), json_schema)))
    actor1_df = actor1_df.select("movie_id", "title", F.col("actor1.name").alias("actor1"))
    actor2_df = actor2_df.select("movie_id", "title", F.col("actor2.name").alias("actor2"))
    # actor1_df.show()
    # actor2_df.show()

    paired_actors_df = actor1_df.alias("df1") \
    .join(
        actor2_df.alias("df2"),
        (col("df1.movie_id") == col("df2.movie_id")) &
        (col("df1.actor1") != col("df2.actor2"))
    ) \
    .select(
        col("df1.movie_id"),
        col("df1.title"),
        col("df1.actor1").alias("actor1"),
        col("df2.actor2").alias("actor2")
    ) \
    .distinct() \
    .orderBy("movie_id", "actor1", "actor2")

    # paired_actors_df.show()
    df_counts = paired_actors_df.groupBy("actor1", "actor2") \
    .agg(count("*").alias("pair_count")) \
    .filter(col("pair_count") >= 2) \
    .orderBy("actor1", "actor2")

    valid_pairs = df_counts.select("actor1", "actor2")

    # Join back to get all occurrences of these pairs along with movie details
    final_df = paired_actors_df.join(valid_pairs, ["actor1", "actor2"], "inner")
    final_df = final_df.select("movie_id", "title", "actor1", "actor2")
    final_df.show()
    final_df.write.option("header", True).mode("overwrite").parquet(output_path)

  finally:
    spark.stop()

find_pairs(input_path,output_path)
