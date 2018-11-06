from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import re

def fileName(data):
    pattern = r"hdfs:"
    debug = data.toDebugString()
    repater = re.compile(pattern)
    matchOB = repater.match(debug)
    print(matchOB)
    print(debug)

sc = SparkContext("local[1]", "textStream")
ssc = StreamingContext(sc, 10)

lines = ssc.textFileStream("hdfs:///user/")

lines.foreachRDD(fileName)

replaced = lines.map(lambda line: line.replace('[', '').replace(']', '').replace(',',''))
docids = replaced.map(lambda line: line.split(" ")).filter(lambda x:x)
docids.count()
docids.pprint()

#words = lines.flatMap(lambda line: line.split(" ")).filter(lambda x:x)
#pairs = words.map(lambda word: (word, 1))
#wordCounts = pairs.reduceByKey(lambda x, y: x + y)
#wordCounts.pprint()

ssc.start()             
ssc.awaitTermination() 
