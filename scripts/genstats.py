from __future__ import print_function
import sys
import json
from pyspark import SparkContext, SparkConf

from elasticsearch import Elasticsearch

# this method updates the task status and results in the elasticsearch
def updateESIndex(taskid, current, status, output):
	#update task status in ES
	es.update(index="spark-data-statistics", doc_type="data-statistics-job", id=taskid, body={
		'doc':{
			'current': current,
			'status': status,
			'result': output
		}
	})


if __name__ == "__main__":
	if len(sys.argv) < 2:
		print("Usage: statistics.py <file> <taskid>")

	#get data url from the command line params 
	dataurl = sys.argv[1]
	#task id is genarated by celery and is alwyas the second argment when submitting the spark task
	task_id = sys.argv[2]
	
	#setup elasticsearch host
	ES_HOST = {
	    "host": 'localhost',
	    "port": 9200
	}

	es = Elasticsearch(hosts=[ES_HOST])

	try:

		#Configure SPARK
		conf = SparkConf().setAppName("Data statistics using Spark")
		conf = conf.setMaster("local[*]")
		#conf.set("fs.s3n.awsAccessKeyId", "")
		#conf.set("fs.s3n.awsSecretAccessKey", "")
		sparkContext = SparkContext(conf=conf)

		#update task status in ES
		updateESIndex(task_id, 1, "Started", "")

		#1)Load devices.csv
		dataRDD = sparkContext.textFile(dataurl)
		#get header row with column names
		header = dataRDD.take(1)
		header_list = header[0].split(',')
		#rows = df.filter(lambda line: line!=header)
		device_fields = dataRDD.map(lambda line: line.split(","))
		# persist the fields rdd as it is needed again
		device_fields.persist()

		result = { } 

		total_records = device_fields.map(lambda fields: fields[0]).count()
		result['total_records'] = total_records

	 	#use accumulator (spark shared variable) to keep trackof column indices
		index = sparkContext.accumulator(0)
		#generate staitistics for each column
		for col in header_list:
			i = index.value # accumulator value cannot be accessed directly inside tasks i.e. map function here
			num_uniq_values = device_fields.map(lambda fields:fields[i]).distinct().count()
			result[col] = num_uniq_values
			index.add(1)


		out = json.dumps(result)
		print (out)
		#update task status with result in ES
		updateESIndex(task_id, 100, "Finished", out)

	except:
		ex_type, ex, tb = sys.exc_info()
		print("$$$$$$$$$$$$$$$$$$$$$$$$$$$")
		print (ex_type, ex, tb)
		updateESIndex(task_id, 0, "Failed", "")


'''
For issues related to accessing file from S3, refer this
stackoverflow.com/questions/28029134/how-can-i-access-s3-s3n-from-a-local-hadoop-2-6-installation
1) Set credentials in hdfs-site.xml
2) Copy hdfs-site.xml to spark/conf
3) Use classpath with spark-submit
4)  Copy the following jars from hadoop/share/hadoop/tools/lib to spark/lib
	[hadoop-aws.jar, aws-java-sdk.jar, google-collections.jar, gauva-18.jar]
'''

'''
spark-submit --driver-class-path /usr/local/spark/lib/aws-java-sdk-1.7.4.jar:/usr/local/spark/lib/hadoop-aws-2.6.0.jar:/usr/local/spark/lib/gauva-18.0.jar:/usr/local/spark/lib/google-collections-0.8.jar /home/ashish/Documents/Flask_SparkApp/scripts/statistics.py s3n://cmpe295b-sjsu-bigdatasecurity/devices.csv
'''
