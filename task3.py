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

output_file_name_3a = "result_3a.csv"

yelp_businesses = remove_header(sc.textFile(folder_name + input_file_name_1))

yelp_businesses_rdd = yelp_businesses.map(lambda line: line.split('\t'))

yelp_businesses_rdd.cache()
""" a """
cities = yelp_businesses_rdd.map(lambda fields: (fields[3], float(fields[8])))
tot_number_of_stars_per_city = cities.reduceByKey(lambda x, y: x + y)
reviews_per_city = cities.map(lambda k: (k[0], 1)).reduceByKey(lambda x, y: x + y)

result_a = tot_number_of_stars_per_city.join(reviews_per_city).map(lambda t: (t[0], t[1][0] / t[1][1]))

#ratings_per_city = users.map(lambda k: (k, 1)).reduceByKey(lambda x, y: x + y)

""" b """
#categories = yelp_businesses_rdd.flatMap(lambda fields: fields[10].replace(" ", "").split(',')).map(lambda k: (k, 1)).reduceByKey(lambda x, y: x + y)

""" c """
"""
postal_codes = yelp_businesses_rdd.map(lambda fields: fields[5])
num_of_businesses_in_pc = postal_codes.map(lambda k: (k, 1)).reduceByKey(lambda x, y: x + y)

longs = yelp_businesses_rdd.map(lambda fields: (fields[5], float(fields[7])))
longs_sum = longs.reduceByKey(lambda x, y: x + y)

lats = yelp_businesses_rdd.map(lambda fields: (fields[5], float(fields[6])))
lats_sum = lats.reduceByKey(lambda x, y: x + y)

joined = num_of_businesses_in_pc.join(longs).join(lats)

yelp_businesses_rdd.unpersist()
"""

#count_rdd.collect().saveAsTextFile(folder_name)
result_a.saveAsTextFile(folder_name + output_file_name_3a)
print("a) Average number of stars per review for each city: {}".format(result_a.collect()))
#print("b) Top 10 categories: {}".format(categories.takeOrdered(10, key=lambda x: -x[1])))