import sys
from pyspark.sql import SparkSession, functions as F
# you may add more import if you need to
from pyspark import SparkContext, SparkConf


# don't change this line
hdfs_nn = sys.argv[1]


spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW
input_path = "hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn)
output_path = "hdfs://%s:9000/assignment2/output/question1/" % (hdfs_nn)
def cleanup_csv(input_path,output_path):
  # The commented was used in collab.
  try:
    # conf = SparkConf().setAppName("Part 1 Question 1")
    # sc = SparkContext(conf=conf)
    # spark = SparkSession(sc)
    df = (
          spark.read.option("header", True)
          .option("inferSchema", True)
          .option("delimiter", ",")
          .option("quotes", '"')
          .csv(input_path)
        )   

    # df = spark.read.option("header", True).csv(input_path)
    # print(f"Original DataFrame count: {df.count()}")

    # Show the count of "Reviews" groups and their frequencies, then sort by count in descending order
    df.groupBy(F.col("Reviews")).agg(F.count("Reviews").alias("count_Reviews")).sort(F.desc("count_Reviews")).show()

    df_filtered = df.filter(
          (df['Rating'].cast('float') >= 1.0) &
          # (df['Reviews'] != "[ [ ], [ ] ]") & # do we consider this as empty or not empty?
          (df['Reviews'].isNotNull())
      )
    print(f"Filtered DataFrame count: {df_filtered.count()}")
    # df_filtered.show()
    df_filtered.write.csv(output_path, header=True)
  finally:
    spark.stop()

cleanup_csv(input_path,output_path)

