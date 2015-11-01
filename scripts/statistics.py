from __future__ import print_function
import sys
import json
from pyspark import SparkContext, SparkConf

from elasticsearch import Elasticsearch

ES_HOST = {
    "host": 'localhost',
    "port": 9200
}

es = Elasticsearch(hosts=[ES_HOST])


def configureSpark(app_name, master):
	
	#Configure SPARK
	conf = SparkConf().setAppName(app_name)
	conf = conf.setMaster(master)
	conf.set("fs.s3n.awsAccessKeyId", "AccessId")
	conf.set("fs.s3n.awsSecretAccessKey", "secret")
	spark_context = SparkContext(conf=conf)
	return spark_context


def getDeviceFields(sc, inputFile):
	
	#task id is alwyas the second argment when submitting the spark task
	task_id = sys.argv[2]
	#update task status in ES
	es.update(index="spark-data-statistics", doc_type="data-statistics-job", id=task_id, body={
		'doc': {
			'current': 1,
			'status': 'Spark job started...'
		}
	})

	#1)Load devices.csv
	df = sc.textFile(inputFile)
	#get header row with column names
	header = df.take(1)
	#rows = df.filter(lambda line: line!=header)
	device_fields = df.map(lambda line: line.split(","))

	# persist the fields rdd as it is needed again
	device_fields.persist()
	
	# print num of partitions of RDD
	#print "Number of partitions for device_fields RDD : %d" % device_fields.getNumPartitions()		
	
	return device_fields	


def deviceStatistics(device_fields):

	#task id is alwyas the second argment when submitting the spark task
	task_id = sys.argv[2]

	total_records = device_fields.map(lambda fields: fields[0]).count()

	num_uniq_handles = device_fields.filter(lambda x: "-1" not in x[0]).count() 

	num_unique_deviceid = device_fields.map(lambda fields: fields[1]).distinct().count()
	
	num_unique_devicetypes = device_fields.map(lambda fields: fields[2]).distinct().count()

	num_unique_deviceos = device_fields.map(lambda fields: fields[3]).distinct().count()

	num_unique_dev_country = device_fields.map(lambda fields: fields[4]).distinct().count()

	num_missing_handles = device_fields.filter(lambda h: "-1" in h[0]).count()

	percentage_missing_handles = round(( int(num_missing_handles) * 100 / int(total_records) ), 2)
	 
	#print "Statistics from devices.csv: \n  total_records :%d, \n  num_uniq_handles :%d, \n num_unique_deviceid :%d, \n num_unique_devicetypes :%d, \n num_unique_deviceos :%d, \n num_unique_dev_country: %d, \n num_missing_handles: %d  \n percentage_missing_handles: %d \n " % (total_records, num_uniq_handles, num_unique_deviceid, num_unique_devicetypes, num_unique_deviceos, num_unique_dev_country, num_missing_handles, percentage_missing_handles )

	result = {
		"total_records" : total_records,
		"num_uniq_handles" : num_uniq_handles,
		"num_unique_deviceid": num_unique_deviceid,
		"num_unique_devicetypes": num_unique_devicetypes,
		"num_unique_deviceos": num_unique_deviceos,
		"num_unique_dev_country": num_unique_dev_country
	}

	out = json.dumps(result)
	#update task status with result in ES
	es.update(index='spark-data-statistics', doc_type='data-statistics-job', id=task_id, body={
		'doc':{
			'current': 100,
			'status': 'Spark job finished...',
			'result': out
		}
	})


def getStatistics(dataurl):
	
	#local[#] - # is nnumber of cores to be allocated (Max =max cores on your system)
	sparkContext = configureSpark("StatisticAnalysis DrawbridgeData", "local[*]")
	
	deviceFields = getDeviceFields(sparkContext, dataurl)
	deviceStatistics(deviceFields)


# parameter is the data url provided by celery task
#getStatistics(sys.argv[1])

if __name__ == "__main__":
	if len(sys.argv) < 2:
		print("Usage: statistics.py <file> <taskid>")

	#provide file url as the parameter
	getStatistics(sys.argv[1])

'''
For issues related to accessing file from S3, refer this
stackoverflow.com/questions/28029134/how-can-i-access-s3-s3n-from-a-local-hadoop-2-6-installation
1) Set credentials in hdfs-site.xml
2) Copy hdfs-site.xml to spark/conf
3) Use classpath with spark-submit
'''


'''
spark-submit --driver-class-path /usr/local/spark/lib/aws-java-sdk-1.7.4.jar:/usr/local/spark/lib/hadoop-aws-2.6.0.jar:/usr/local/spark/lib/gauva-18.0.jar:/usr/local/spark/lib/google-collections-0.8.jar /home/ashish/Documents/Flask_SparkApp/scripts/statistics.py s3n://cmpe295b-sjsu-bigdatasecurity/devices.csv
'''
