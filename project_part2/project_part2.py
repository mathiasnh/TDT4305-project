from pyspark import SparkContext, SparkConf
import csv
import base64
import re

def remove_header(csv):
    csv_header = csv.first()
    header = sc.parallelize([csv_header])
    return csv.subtract(header)

conf = SparkConf().setAppName("YelpReviews").setMaster("local")
sc = SparkContext(conf=conf)

folder_name = "./data/"
result_folder_name = "./results/"

input_file_name = "yelp_top_reviewers_with_reviews.csv"
afinn111 = "AFINN-111.txt"
stopwordstxt = "stopwords.txt"

output_file_name = "result_part2.csv"

k = int(input("How many businesses do you want to view? "))

afinn_dct = {}

for word in open(folder_name + afinn111):
    key, val = word.split("\t")
    afinn_dct[key] = val

# Use set for faster lookup
stopwords = set(open(folder_name + stopwordstxt).read().split("\n"))

yelp_top_reviewers_with_reviews = remove_header(sc.textFile(folder_name + input_file_name))

yelp_top_reviewers_with_reviews = yelp_top_reviewers_with_reviews.map(lambda line: line.split('\t'))

def polarity(s):
    # Tokenize each review into individual lower cased words
    split = re.sub(r'[,.?!]','',s).lower().split()

    # Remove all stopwords
    removed = filter(lambda x: x not in stopwords, split)

    # Find and return polarty for review 
    return sum(map(lambda x: int(afinn_dct.get(x, 0)), removed))

# Mapping businessid to polarity of decoded review text
top_reviewers_rdd = yelp_top_reviewers_with_reviews.map(lambda fields: (fields[2], polarity(base64.b64decode(fields[3]))))

# Reducing, sorting by ascending order and taking k elements
reduced = top_reviewers_rdd.reduceByKey(lambda x, y: x + y).sortBy(lambda x: -x[1])

# Add indexes to all RDD elements and use filter to only return a specified amount k
filtered_reduced = reduced.zipWithIndex().filter(lambda x: x[1] < k).map(lambda x: x[0])

filtered_reduced.repartition(1).saveAsTextFile(result_folder_name + output_file_name)