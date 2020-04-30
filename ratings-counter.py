#importing SparkContecxt to create RDD but without SparkConf, 
#which allows you to configure whether  
#and tell it things like, do I want to run on just one computer? 
#Or do you want to run it on a cluster,
from pyspark import SparkConf, SparkContext
import collections

#create sparkcontext which has master node as local(single thread and single process) machine
# there is an extension where we can divide the RDD processing in different CPUs/core and clusters
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
#sparkcontext object
sc = SparkContext(conf = conf)

#each line of the textfile is a value(whose type is string) of RDD object ie lines here
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
#this map function is taking only rating value from u.data dataset, lines RDD remains untouched
#new RDD is ratings where our output is
ratings = lines.map(lambda x: x.split()[2])

#using action(countByValue) on RDD(ratings)
#here we are using action method on RDD hence result here is plain python object and result, no longer is a RDD
result = ratings.countByValue()

#plain python syntax
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
# rdd.map(lambda x: x*x) this is compact form of the below, is the same thing as:
#    def squareIt(x):
#        return x*x;
#    rdd.map(squareIt)
# =======================RDD MORE ACTIONS============================================
# collect,count, countByValue: breakdown by value, reduce:
#nothing actually happens in your driver program until an action is called : Lazy evaluation, 
# after you call an action Directed Acyclic Graph is create and optimized the workflow to execute our
#query kinda lazy loading    
# =============================================================================
