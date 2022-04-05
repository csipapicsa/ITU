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
                    
##
bdf = spark.read.json("/datasets/yelp/business.json")
print("3.1.1")
bdf.agg({'review_count': 'sum'}).show()

##

bdft = bdf.filter('stars == 5 and review_count >= 1000')

from pyspark.sql.functions import col
print("3.1.2")
bdf.filter('stars == 5 and review_count >= 1000').select(col("name"),col("stars"),col("review_count")).show()

##

udf = spark.read.json("/datasets/yelp/user.json")
print("3.1.3")
udf.filter('review_count > 1000').select(col("user_id")).show()
udfres = udf.filter('review_count > 1000').select(col("user_id"))

##

rdf = spark.read.json("/datasets/yelp/review.json") # contains the reviews 

df_bus_merge = rdf.join(bdf,['business_id'],how='inner')

df_merged = df_bus_merge.join(udfres,['user_id'],how='inner')

df_merged_count = df_merged.groupBy('business_id').count()
print("3.1.4")
df_merged_count.filter('count > 5').show()

##

u_temp = udf.sort(col("average_stars").desc())
# I made a new df since if you print it it looks like a mess
print("3.1.5")
u_temp.select(col("average_stars"),col("user_id")).show()

#############################


rdf = spark.read.json("/datasets/yelp/review.json")
rdf.createOrReplaceTempView("reviews")
bdf = spark.read.json("/datasets/yelp/business.json")
bdf.createOrReplaceTempView("business")

df1 = spark.sql("SELECT * FROM reviews")

df2 = spark.sql("SELECT * FROM reviews WHERE text LIKE '%authentic%'")
# its super sloooooooowwwww, how can I add these to variables ? 
print("3.2.1 a")
print("Ratio of reviews which contains word 'authetnic' is ", (df2.count() / df1.count())*100)

##

#b
rews = spark.sql("SELECT * FROM reviews WHERE text LIKE '%legitimate%'")
from pyspark.sql.functions import split, explode
bdf2 = bdf.withColumn('cat_new',explode(split('categories',', ')))
merge = rews.join(bdf2,['business_id'],how='inner')
# collect all categories
categories = merge.select("cat_new").rdd.flatMap(lambda x: x).collect()

from collections import Counter
c_l = []
for i in categories:
    c_l.append(i)

c = Counter(c_l)
e = 40
print("first ",e," most common categories in order")
print(c.most_common(40))

# define cousie types manually

cousine_types = [
'American (New)',
'American (Traditional)',
'Italian',
'Mexican',
'Japanese',
'Chinese',
'Thai',
'Indian',
'French',
'Korean',
'Mediterranean',
'Soul']

m = merge.filter(merge.cat_new.isin(cousine_types))
m = m.groupBy("cat_new").count()
print("3.2.1.b")
m.sort(col("count").desc()).show(truncate=False)

## 

#rdf_a = spark.sql("SELECT * FROM reviews WHERE text LIKE '%authentic%' OR text LIKE '%legitimate%' OR text LIKE '%accent%'")
rdf_a = spark.sql("SELECT * FROM reviews WHERE text LIKE '%authentic%' OR text LIKE '%accent%'")
bdf = spark.sql("SELECT * FROM business WHERE state IS NOT NULL OR city IS NOT NULL")

merge_auth = rdf_a.join(bdf,['business_id'],how='inner')

auth_df = merge_auth.cube("state").count().orderBy("state") # it can have multipe columns, I just skipped it
auth_df = auth_df.withColumnRenamed("count","auth_lan_count")
auth_df = auth_df.withColumnRenamed("state","state_auth")

# sec part
rdf = spark.sql("SELECT * FROM reviews")

merge_all = rdf.join(bdf,['business_id'],how='inner')
all_r = merge_all.cube("state").count().orderBy("state")
all_r = all_r.withColumnRenamed("count","all_lan_count")


join = all_r.join(auth_df,all_r.state ==  auth_df.state_auth,"fullouter")
join = join.fillna(0, subset=['all_lan_count'])\
       .fillna(0, subset=['auth_lan_count'])

from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
from pyspark.sql.functions import lit

join = join.withColumn('Ratio', (coalesce(col('auth_lan_count'), lit(0)) / coalesce(col('all_lan_count'), lit(1))*100))
print("3.2.3")
print("Result of ratio of using authenticity language compared to all review in state level")
join.sort(join.Ratio.desc()).show(truncate=False)

##
print("3.2.2 Hypothesis Testing")
# read in two different dataframe, which are cointains "authentic" and "legitimate"
rdf = spark.read.json("/datasets/yelp/review.json")
rdf.createOrReplaceTempView("reviews")

bdf = spark.read.json("/datasets/yelp/business.json")
bdf.createOrReplaceTempView("business")

############ BADS 
rdf = spark.sql("SELECT * FROM reviews WHERE text LIKE '%authentic%' OR text LIKE '%legitimate%' \
                    AND (text LIKE '%dirty%' \
                    OR text LIKE '%cheap%' OR text LIKE '%kitsch%' \
                    OR text LIKE '%rude%' OR text LIKE '%simple%' OR text LIKE '%bad%')")

bdf = spark.sql("SELECT * FROM business")

rdf.toPandas().to_csv("rdf_hip.csv", header=True, index=False, encoding='utf-8')
bdf.toPandas().to_csv("bdf.csv", header=True, index=False, encoding='utf-8')
import pandas as pd
rdf = pd.read_csv('rdf_hip.csv')
bdf = pd.read_csv('bdf.csv')

merge_a = pd.merge(bdf, rdf, on="business_id", how="inner")
merge_a = merge_a[merge_a["categories"].str.contains("restaurant", case=False)==True]



merge_a = merge_a.astype({"categories": str})

merge_a['cats'] = merge_a.categories.apply(lambda x: x.split(', '))

s = merge_a.apply(lambda x: pd.Series(x['cats']), axis=1).stack().reset_index(level=1, drop=True)
#s = merge_a.apply(lambda x: pd.Series(x['cats']), axis=1).stack(level=list).reset_index(level=1, drop=True)

s.name = 'cat'
df2 = merge_a.drop('cats', axis=1).join(s)
df2['cat'] = pd.Series(df2['cat'], dtype=object)
# delete restaurants 
df2 = df2[df2.cat != "Restaurants"]
df2.to_csv("bads.csv", header=True, index=False, encoding='utf-8')



list_of_categories =  df2['cat'].tolist()
from collections import Counter

bads = Counter(list_of_categories)
e = 40
print("first ",e," most common categories in order for negative words")
print(bads.most_common(e))


###### GOODS 

rdf = spark.sql("SELECT * FROM reviews WHERE text LIKE '%authentic%' OR text LIKE '%legitimate%' \
                    AND (text NOT LIKE '%dirty%' \
                    OR text NOT LIKE '%cheap%' OR text NOT LIKE '%kitsch%' \
                    OR text NOT LIKE '%rude%' OR text NOT LIKE '%simple%' OR text NOT LIKE '%bad%')")

rdf.toPandas().to_csv("rdf_hip.csv", header=True, index=False, encoding='utf-8')

import pandas as pd
rdf = pd.read_csv('rdf_hip.csv')
bdf = pd.read_csv('bdf.csv')

merge_a = pd.merge(bdf, rdf, on="business_id", how="inner")

merge_a = merge_a.astype({"categories": str})
merge_a = merge_a[merge_a["categories"].str.contains("restaurant", case=False)==True]

merge_a['cats'] = merge_a.categories.apply(lambda x: x.split(', '))

s = merge_a.apply(lambda x: pd.Series(x['cats']), axis=1).stack().reset_index(level=1, drop=True)
#s = merge_a.apply(lambda x: pd.Series(x['cats']), axis=1).stack(level=list).reset_index(level=1, drop=True)

s.name = 'cat'
df2 = merge_a.drop('cats', axis=1).join(s)
df2['cat'] = pd.Series(df2['cat'], dtype=object)
df2 = df2[df2.cat != "Restaurants"]
df2.to_csv("not_bads.csv", header=True, index=False, encoding='utf-8')



list_of_categories =  df2['cat'].tolist()
from collections import Counter

goods = Counter(list_of_categories)
e = 40
print("first ",e," most common categories in order without negative words")
print(goods.most_common(e))

print("Generate lists manually by cuisine type, grouped by the origin of the continents")
s_am_list = ["Mexican","New Mexican Cuisine","Peruvian","Cuban"]
s_asian_list = ["Thai", "Indian","Pakistani"]
f_l = ["Mexican","New Mexican Cuisine","Peruvian","Cuban","Thai", "Indian","Pakistani"]
eu = ["Italian","French","Greek","German","Spanish","Irish"]

g = 0
b = 0
for i in f_l:
    g += goods[i]
    b += bads[i]

print("Ratio of negative reviews compared to non negative reviews \n where the origin of the cusine is South America or South Asia: ", b/g)

g = 0
b = 0
for i in eu:
    g += goods[i]
    b += bads[i]
    

print("Ratio of negative reviews compared to non negative reviews \n where the origin of the cusine is Europe: ", b/g)










