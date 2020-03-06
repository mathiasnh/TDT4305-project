from pyspark import SparkContext, SparkConf
from datetime import datetime
import numpy as np
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

input_file_name = "yelp_top_users_friendship_graph.csv"

output_file_name = "result_1.csv"

yelp_top_users_friendship_graph = remove_header(sc.textFile(folder_name + input_file_name)).map(lambda line: line.split(','))

yelp_top_users_friendship_graph.cache()

yelp_top_users_friendship_graph_rdd = yelp_top_users_friendship_graph.map(lambda fields: (fields[0], fields[1]))

nodes_out_degrees = yelp_top_users_friendship_graph_rdd.map(lambda k: (k[0], 1)).reduceByKey(lambda x, y: x + y)

nodes_in_degrees = yelp_top_users_friendship_graph_rdd.map(lambda k: (k[1], 1)).reduceByKey(lambda x, y: x + y)


""" b """
total_connections = nodes_in_degrees.values().sum()
count_out = nodes_out_degrees.distinct().count()
count_in = nodes_in_degrees.distinct().count()

mean_out = total_connections / count_out
mean_in = total_connections / count_in

median_out = np.median(nodes_out_degrees.values().collect())
median_in = np.median(nodes_in_degrees.values().collect())

yelp_top_users_friendship_graph.unpersist()

print("a1) Top 10 nodes (out degrees): {}".format(nodes_out_degrees.takeOrdered(10, key=lambda x: -x[1])))
print("a2) Top 10 nodes (in degrees): {}".format(nodes_in_degrees.takeOrdered(10, key=lambda x: -x[1])))
print("b1) Mean of in degrees in friendships graph: {}".format(mean_in))
print("b2) Mean of out degrees in friendships graph: {}".format(mean_out))
print("b3) Median of in degrees in friendships graph: {}".format(median_in))
print("b4) Median of out degrees in friendships graph: {}".format(median_out))