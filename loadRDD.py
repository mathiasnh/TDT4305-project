from pyspark import SparkContext, SparkConf
import csv

def remove_header(csv):
    csv_header = csv.first()
    header = sc.parallelize([csv_header])
    return csv.subtract(header)


conf = SparkConf().setAppName("YelpReviews").setMaster("local")
sc = SparkContext(conf=conf)

folder_name = "./data/"

input_file_name_1 = "yelp_businesses.csv"
input_file_name_2 = "yelp_top_reviewers_with_reviews.csv"
input_file_name_3 = "yelp_top_users_friendship_graph.csv"

output_file_name = "result_1.csv"

yelp_businesses = remove_header(sc.textFile(folder_name + input_file_name_1))
yelp_top_reviewers_with_reviews = remove_header(sc.textFile(folder_name + input_file_name_2))
yelp_top_users_friendship_graph = remove_header(sc.textFile(folder_name + input_file_name_3))


yelp_businesses_rdd = yelp_businesses.map(lambda line: line.split('\t'))
yelp_top_reviewers_with_reviews = yelp_top_reviewers_with_reviews.map(lambda line: line.split('\t'))
yelp_top_users_friendship_graph = yelp_top_users_friendship_graph.map(lambda line: line.split(','))

yelp_businesses_rdd.cache()
distinct_business_IDs_rdd = yelp_businesses_rdd.map(lambda fields: fields[0])
count_business_rdd = sc.parallelize([distinct_business_IDs_rdd.count()])
yelp_businesses_rdd.unpersist()

yelp_top_reviewers_with_reviews.cache()
distinct_top_reviewers_rdd = yelp_top_reviewers_with_reviews.map(lambda fields: fields[0])
count2_top_rdd = sc.parallelize([distinct_top_reviewers_rdd.count()])
yelp_top_reviewers_with_reviews.unpersist()

yelp_top_users_friendship_graph.cache()
distinct_top_users_friendships_rdd = yelp_top_users_friendship_graph.map(lambda fields: fields[0])
count3_friends_rdd = sc.parallelize([distinct_top_users_friendships_rdd.count()])
yelp_top_users_friendship_graph.unpersist()


#count_rdd.collect().saveAsTextFile(folder_name)
print(count_business_rdd.collect()[0])
print(count2_top_rdd.collect()[0])
print(count3_friends_rdd.collect()[0])