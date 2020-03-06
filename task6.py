from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, TimestampType

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

# Casting to specified types from the data description
businesses_df = businesses_df.withColumn("latitude", businesses_df["latitude"].cast(FloatType()))
businesses_df = businesses_df.withColumn("longitude", businesses_df["longitude"].cast(FloatType()))
businesses_df = businesses_df.withColumn("stars", businesses_df["stars"].cast(FloatType()))
top_reviewers_df = top_reviewers_df.withColumn("review_date", top_reviewers_df["review_date"].cast(TimestampType()))

joined_df = top_reviewers_df.join(businesses_df, on=['business_id'], how='inner')

joined_df.show() #6a
joined_df.createOrReplaceTempView('reviews') #6b
sqlContext.sql('select user_id, count(distinct(review_id)) as NumberOfReviews from reviews group by user_id order by NumberOfReviews desc').show() #6c