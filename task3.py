from pyspark import SparkContext, SparkConf
import csv

def remove_header(csv):
    csv_header = csv.first()
    header = sc.parallelize([csv_header])
    return csv.subtract(header)


conf = SparkConf().setAppName("YelpReviews").setMaster("local")
sc = SparkContext(conf=conf)

folder_name = "./results/"

input_file_name_1 = "yelp_businesses.csv"

output_file_name_3a = "result_3a.csv"
output_file_name_3c = "result_3c.csv"


yelp_businesses = remove_header(sc.textFile(folder_name + input_file_name_1))

yelp_businesses_rdd = yelp_businesses.map(lambda line: line.split('\t'))

yelp_businesses_rdd.cache()

""" a """
cities = yelp_businesses_rdd.map(lambda fields: (fields[3], float(fields[8])))
tot_number_of_stars_per_city = cities.reduceByKey(lambda x, y: x + y)
reviews_per_city = cities.map(lambda k: (k[0], 1)).reduceByKey(lambda x, y: x + y)

result_a = tot_number_of_stars_per_city.join(reviews_per_city).map(lambda t: (t[0], t[1][0] / t[1][1]))

""" b """
categories = yelp_businesses_rdd.flatMap(lambda fields: fields[10].replace(" ", "").split(',')).map(lambda k: (k, 1)).reduceByKey(lambda x, y: x + y)

""" c """
sum_geo_per_postal_code = yelp_businesses_rdd.map(lambda fields: (fields[5], [(fields[6], fields[7])])).reduceByKey(lambda x, y: x + y)

avg_geo = sum_geo_per_postal_code.mapValues(lambda x: (sum(float(data[0]) for data in x) / len(x), sum(float(data[1]) for data in x) / len(x)))

geo_cen = avg_geo.map(lambda postal_lat_lon: (postal_lat_lon[0], postal_lat_lon[1][0], postal_lat_lon[1][1]))

yelp_businesses_rdd.unpersist()


#result_a.saveAsTextFile(folder_name + output_file_name_3a)
#geo_cen.saveAsTextFile(folder_name + output_file_name_3c)
print("a) Average number of stars per review for each city: {}".format(result_a.collect()))
print("b) Top 10 categories: {}".format(categories.takeOrdered(10, key=lambda x: -x[1])))
aprint("c) Geographic center for each postal code: {}".format(geo_cen.collect()))