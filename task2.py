from pyspark import SparkContext, SparkConf
from datetime import datetime
#from task1 import remove_header
import csv
import base64

def remove_header(csv):
    csv_header = csv.first()
    header = sc.parallelize([csv_header])
    return csv.subtract(header)

def time_stamp(val):
    return datetime.utcfromtimestamp(float(val)).strftime('%Y-%m-%d %H:%M:%S')

conf = SparkConf().setAppName("YelpReviews").setMaster("local")
sc = SparkContext(conf=conf)

folder_name = "./data/"

input_file_name = "yelp_top_reviewers_with_reviews.csv"

output_file_name = "result_1.csv"

yelp_top_reviewers_with_reviews = remove_header(sc.textFile(folder_name + input_file_name))

yelp_top_reviewers_with_reviews = yelp_top_reviewers_with_reviews.map(lambda line: line.split('\t'))

yelp_top_reviewers_with_reviews.cache()
""" a """
#distinct_top_reviewers_rdd = yelp_top_reviewers_with_reviews.map(lambda fields: fields[1]).distinct() 
#count_top_rdd = sc.parallelize([distinct_top_reviewers_rdd.count()])

""" b """
#reviews = yelp_top_reviewers_with_reviews.map(lambda fields: base64.b64decode(fields[3]))
#number_of_chars_in_review = yelp_top_reviewers_with_reviews.map(lambda fields: len(base64.b64decode(fields[3]))).collect()
#length = len(number_of_chars_in_review)

""" c """
#businesses = yelp_top_reviewers_with_reviews.map(lambda fields: fields[2])
#result_c = businesses.map(lambda k: (k, 1)).reduceByKey(lambda count1, count2: count1 + count2)

""" d """
#timestamps = yelp_top_reviewers_with_reviews.map(lambda fields: fields[4])
#result_d = timestamps.map(lambda k: (datetime.utcfromtimestamp(float(k)).strftime('%Y'), 1)).reduceByKey(lambda count1, count2: count1 + count2)

""" e """
timestamps = yelp_top_reviewers_with_reviews.map(lambda fields: fields[4])
sorted_timestamps = sorted(timestamps.collect())

""" f """


yelp_top_reviewers_with_reviews.unpersist()


#count_rdd.collect().saveAsTextFile(folder_name)
#print("a) Number of distinct users: {}".format(count_top_rdd.collect()[0]))
#print("b) Average number of characters in each review: {}".format(sum(number_of_chars_in_review)/length))
#print("c) 10 most reviewed businesses: {}".format(result_c.takeOrdered(10, key=lambda x: -x[1])))
#print("d) Number of reviews per year: {}".format(sorted(result_d.collect())))
print(time_stamp(sorted_timestamps[0]), time_stamp(sorted_timestamps[-1]))
