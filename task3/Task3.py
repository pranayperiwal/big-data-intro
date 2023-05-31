from pyspark import SparkContext

if __name__ == "__main__":
	
	# create Spark context with necessary configuration
    sc = SparkContext("local","PySpark Word Count Exmaple")
	
	# read data from text file and split each line by newline
    lines = sc.textFile("hdfs://localhost:9000/user/pranay/output/part-00000").flatMap(lambda line: line.split('\n'))

	# split the line by whitespace to get the URLs
    timestamp = lines.map(lambda line: line.split()[1])
	
    # groupedRDD = timestamp.map(lambda line: [line[:4], line[5:]])
    minutes = timestamp.flatMap(lambda line: [line[:4]]).map(lambda timestamp: (timestamp, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda x: x[1], False)

    val = minutes.collect()
    for row in val:
        print(row)

