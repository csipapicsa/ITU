import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf

conf=SparkConf()
conf.set("spark.executor.memory", "2G")
conf.set("spark.executor.instances", "4")
# You can add other configuration options here.

spark = SparkSession.builder \
                    .appName('template-application') \
                    .config(conf=conf) \
                    .getOrCreate()


# Insert your own code here

##########################

# Example: Say we're only interested in reviews of good mexican restaurants in Arizona. You can delete this when you do your own thing. 

# Read in the business and review files
bs = spark.read.json("/datasets/yelp/business.json")
rs = spark.read.json("/datasets/yelp/review.json")

# Filter to only Arizona businesses with "Mexican" as part of their categories
az_mex = bs.filter(bs.state == "AZ").filter(bs.categories.rlike("Mexican")).select("business_id", "name")

# Join with the reviews
az_mex_rs = rs.join(az_mex, on="business_id", how="inner")

# Filter to only 5 star reviews
good_az_mex_rs = az_mex_rs.filter(az_mex_rs.stars == 5).select("name","text")

# Print the top 20 rows of the DataFrame
good_az_mex_rs.show()

#Convert to pandas (local object) and save to local file system (ambari0)
good_az_mex_rs.toPandas().to_csv("good_az_reviews.csv", header=True, index=False, encoding='utf-8')
