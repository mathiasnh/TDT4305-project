from pyspark import SparkContext, SparkConf
from datetime import datetime
import csv
import base64
from math import sqrt

def remove_header(csv):
    csv_header = csv.first()
    header = sc.parallelize([csv_header])
    return csv.subtract(header)

conf = SparkConf().setAppName("YelpReviews").setMaster("local")
sc = SparkContext(conf=conf)

folder_name = "./data/"

input_file_name = "yelp_top_reviewers_with_reviews.csv"

output_file_name = "result_31.csv"

yelp_top_reviewers = remove_header(sc.textFile(folder_name + input_file_name)).map(lambda line: line.split('\t'))

yelp_top_reviewers.cache()

yelp_top_reviewers_rdd = yelp_top_reviewers.map(lambda fields: (fields[2], base64.b64decode(fields[3])))

yelp_top_reviewers.unpersist()

print(yelp_top_reviewers_rdd.collect())