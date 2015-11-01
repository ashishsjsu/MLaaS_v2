from __future__ import absolute_import
from bootstrap import app, celery
import os

@celery.task(bind=True)
def spark_wordcount_task(self):
	
	'''
		self - the task itself
		request represents the current request object and conatins information and state of the task
	'''
	task_id = self.request.id
	print ("# task_id ",task_id)

	master_path = 'local[2]'
	project_dir  = "scripts/"
	spark_code_path = project_dir + "wordcount.py"
	input_path = project_dir + "input.txt"
	
	result = os.system("spark-submit --master %s %s %s %s" % (master_path, spark_code_path, input_path, self.request.id))

	return {'current' : 100, 'total' : 100, 'status' : 'Task Completed!', 'result': 10}


@celery.task(bind=True)
def spark_data_statistics(self, fileinput):
	
	print "**************************"
	print fileinput
	'''
		self - the task itself
		request represents the current request object and conatins information and state of the task
	'''
	task_id = self.request.id
	dataurl = fileinput
	master_path = 'local[*]'
	project_dir = "scripts/"
	spark_code_path = project_dir + "statistics.py"

	driver_classpath = "/usr/local/spark/lib/aws-java-sdk-1.7.4.jar:/usr/local/spark/lib/hadoop-aws-2.6.0.jar:/usr/local/spark/lib/gauva-18.0.jar:/usr/local/spark/lib/google-collections-0.8.jar"

	result = os.system("spark-submit --driver-class-path %s --master %s %s %s %s" % (driver_classpath, master_path, spark_code_path, dataurl, self.request.id))

	return {'current': 100, 'total': 100, 'status': 'Task Completed!', 'result': result}