from pyspark import SparkContext
 
if __name__ == "__main__":
	
	# create Spark context with necessary configuration
	sc = SparkContext("local","PySpark Word Count Exmaple")
	
	# read data from text file and split each line by newline
	lines = sc.textFile("hdfs://localhost:9000/user/pranay/output/part-00000").flatMap(lambda line: line.split('\n'))

	# split the line by whitespace to get the URLs
	urls = lines.map(lambda line: line.split()[3])

	#split the URLs by '.', '?' and '=' to get the tokens 
	tokens = urls.flatMap(lambda url: url.split('.')).flatMap(lambda urlSplit: urlSplit.split('?')).flatMap(lambda urlSplit: urlSplit.split('=')).filter(lambda word: word != '')
	
	# count the occurrence of each token
	wordCounts = tokens.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda x: x[1], False)
	
	val = wordCounts.collect()[:10]
	for row in val:
		print(row)
        
