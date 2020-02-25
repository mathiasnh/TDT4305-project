from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("YelpReviews").setMaster("local")
sc = SparkContext(conf=conf)

dataFile = sc.textFile("yelp_business.csv")

print(dataFile)

