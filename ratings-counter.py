from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
# =============================================================================
#RDD(Resillient Distributed Dataset) is created using sparkcontext object given 
# Spark itself and After creating RDD, we use various methods like map, flatmap, sample
#,filter, intersection, union to trasform our dataset(RDD), I called here RDD as dataset 
#since resilient and distributed are managed by spark and only thing we need to care 
#about is performing operations of this RDD as dataset.
    
# =============================================================================
# Functional programming is foudation of Spark
# Many RDD methods accept a function as a parameter then typical params like int, float.
# rdd.map(lambda x: x*x) is the same thing as:
#    def squareIt(x):
#        return x*x;
#    rdd.map(squareIt)
# =============================================================================
