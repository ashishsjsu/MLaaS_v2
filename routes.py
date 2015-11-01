from flask import Blueprint, jsonify, url_for, request
import time, os, urllib, base64, hmac
import json
from hashlib import sha1
from datetime import datetime
from elasticsearch import Elasticsearch

import pickle
import importlib

ES_HOST = {
	"host": "localhost",
	"port": 9200
}
es = Elasticsearch(hosts = [ES_HOST])

#create a blueprint object
sparktask_page = Blueprint('sparktask_page', __name__)

@sparktask_page.route('/')
def sayHello():
	try:
		resp = jsonify({"message": "Hello!"})
		resp.status_code = 200
		return resp	
	except:
		abort(404)

#this api is meant to return the signature with which client can upload the file to AWS S3
@sparktask_page.route('/upload/sign_s3', methods=["GET"])
def upload_file():

	# get AWS parameters needed to sign the request
	AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
	AWS_SECRET_KEY = os.environ.get('AWS_SECRET_KEY')
	S3_BUCKET = os.environ.get('S3_BUCKET')

	#object name to be uploaded, get file name from the req url params
	object_name = urllib.quote_plus(request.args.get('file_name'))
	# get the type of file
	mime_type = request.args.get('file_type')

	# set expiry time of the signature to one day, this is a temporary request
	expires = int(time.time()+60*60*24)
	# this header indicates that the file is publically available for download
	amz_headers = "x-amz-acl:public-read"
	# construct the PUT request sing above data
	string_to_sign = "PUT\n\n%s\n%d\n%s\n/%s/%s" % (mime_type, expires, amz_headers, S3_BUCKET, object_name)
	# generate the signature as SHA hash of compiled AWS swcret key and PUT request
	signature = base64.encodestring(hmac.new(AWS_SECRET_KEY.encode(), string_to_sign.encode('utf8'), sha1).digest())
	# strip the whitespaces and escape the special characters for safer HTTP transmission
	signature = urllib.quote_plus(signature.strip())
	# url of the object to be uploaded
	url = "https://%s.s3.amazonaws.com/%s" % (S3_BUCKET, object_name)
	# return the signed request and the prospective url as json response
	response = json.dumps({
		'signed_request': "%s?AWSAccessKeyId=%s&Expires=%s&Signature=%s" % (url, AWS_ACCESS_KEY, expires, signature),
		'url': url
	})

	return response


# this api generated the data statistics 
@sparktask_page.route('/statistics', methods=["POST"])
def getStatistics():
	print ""
	from tasks import spark_data_statistics
	#Todo: pass argumetns to the task from the req body
	task = spark_data_statistics.apply_async(args=["s3n://cmpe295b-sjsu-bigdatasecurity/devices.csv"])

	#dump the task metadata in pickle object to use for status tracking
	task_meta = {task.id: "spark_data_statistics"}
	pickle.dump(task_meta, open("taskmetadata/taskmeta.p", "wb"))

	#create a new elastic search index for this task
	if not es.indices.exists('spark-data-statistics'):
		print ("Creating '%s' index..." % ('spark-data-statistics'))
		res = es.indices.create(index='spark-data-statistics', body={
			"settings": {
				'number_of_shards': 1,
				'number_of_replicas': 0
			}
		})
		print res

	es.index(index='spark-data-statistics', doc_type="data-statistics-job", id=task.id, body={
		'current': 0,
		'total': 100,
		'status': 'Spark job pending',
		'start_time': datetime.utcnow()
	})

	# return the task id and the url to track the status
	return jsonify({'task_id': task.id}), 202, {'Location': url_for('.taskstatus', task_id=task.id)}



#route to submit a spark job
@sparktask_page.route('/spark_task', methods=["POST"])
def sparktask():

	#import the task/job to be submitted
	from tasks import spark_wordcount_task 	
	task = spark_wordcount_task.apply_async()

	#dump the task metadata in pickle object to use for status tracking
	task_meta = {task.id: "spark_wordcount_task"}
	pickle.dump(task_meta, open("taskmetadata/taskmeta.p", "wb"))

	#create a new elasticsearch index for the task
	if not es.indices.exists('spark-jobs'):
		print ("Creating '%s' index..." % ('spark-jobs'))
		res = es.indices.create(index='spark-jobs', body={
			"settings": {
				'number_of_shards': 1,
				'number_of_replicas': 0
			}
		})
		print (res)

	es.index(index='spark-jobs', doc_type='wordcount-job', id=task.id, body={
		'current': 0,
		'total': 100,
		'status': 'Spark job pending...',
		'start_time': datetime.utcnow()
	})

	return jsonify({'task_id': task.id}), 202, {'Location': url_for('.taskstatus', task_id=task.id)}


#route to check spark job status
@sparktask_page.route('/wordcount/<task_id>/status', methods=["GET"])
def taskstatus(task_id):

	print "###############################"
	print taskmeta[task_id]
	from tasks import spark_wordcount_task
	task = spark_job_task.AsyncResult(task_id)

	if task.state == 'FAILURE':
		#something went wrong in the job submitted
		response = {
			'state': task.state,
			'current': 1,
			'total': 1,
			'status': str(task.info)
		}
	else:
		# or else get the task from the ES
		es_task_info = es.get(index='spark-jobs', doc_type='wordcount-job', id=task_id)
		response = es_task_info['_source']
		response['state'] = task.state

	return jsonify(response)


@sparktask_page.route('/statistics/<task_id>/status', methods=["GET"])
def taskstatus(task_id):

	print "###############################"
	print taskmeta[task_id]
	from tasks import spark_data_statistics
	task = spark_job_task.AsyncResult(task_id)

	if task.state == 'FAILURE':
		#something went wrong in the job submitted
		response = {
			'state': task.state,
			'current': 1,
			'total': 1,
			'status': str(task.info)
		}
	else:
		# or else get the task from the ES
		es_task_info = es.get(index='spark-data-statistics', doc_type='data-statistics-job', id=task_id)
		response = es_task_info['_source']
		response['state'] = task.state

	return jsonify(response)
