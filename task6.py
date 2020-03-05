from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

conf = SparkConf().setAppName("YelpReviews").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

folder_name = "./data/"

input_file_name_1 = "yelp_businesses.csv"
input_file_name_2 = "yelp_top_reviewers_with_reviews.csv"
input_file_name_3 = "yelp_top_users_friendship_graph.csv"

businesses_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '\t').load(folder_name + input_file_name_1)
top_reviewers_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '\t').load(folder_name + input_file_name_2)
friendships_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ',').load(folder_name + input_file_name_3)

#joined_df = top_reviewers_df.join(businesses_df, col("top_reviewers_df.business_id") == col("businesses_df.business_id"), "inner")
joined_df = top_reviewers_df.join(businesses_df, on=['business_id'], how='inner')

joined_df.show()