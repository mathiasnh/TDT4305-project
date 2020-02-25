from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("YelpReviews").setMaster("local")
sc = SparkContext(conf=conf)

folder_name = "./data/"

input_file_name_1 = "yelp_business.csv"
input_file_name_2 = "yelp_top_reviewers_with_reviews.csv"
input_file_name_3 = "yelp_top_users_friendship_graph.csv"

output_file_name = "result_1.csv"

textFile_1 = sc.textFile(folder_name + input_file_name_1)
textFile_2 = sc.textFile(folder_name + input_file_name_2)
textFile_3 = sc.textFile(folder_name + input_file_name_3)
