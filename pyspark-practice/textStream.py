from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[1]", "textStream")
ssc = StreamingContext(sc, 10)

lines = ssc.textFileStream("/home/xx/tmp") 
words = lines.flatMap(lambda line: line.split(" ")).filter(lambda x:x)
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.pprint()

ssc.start()             
ssc.awaitTermination() 
