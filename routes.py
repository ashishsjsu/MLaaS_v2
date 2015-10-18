from flask import Blueprint, jsonify, url_for
import time
from datetime import datetime
from elasticsearch import Elasticsearch

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
		return jsonify({"message": "Hello!"})
	except:
		abort(404)

#route to submit a spark job
@sparktask_page.route('/spark_task', methods=["POST"])
def sparktask():

	#import the task/job to be submitted
	from tasks import spark_job_task 	
	task = spark_job_task.apply_async()

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

	es.index(index='spark-jobs', doc_type='job', id=task.id, body={
		'current': 0,
		'total': 100,
		'status': 'Spark job pending...',
		'start_time': datetime.utcnow()
	})

	return jsonify({'task_id': task.id}), 202, {'Location': url_for('.taskstatus', task_id=task.id)}


#route to check spark job status
@sparktask_page.route('/status/<task_id>', methods=["GET"])
def taskstatus(task_id):
	    
	from tasks import spark_job_task
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
		es_task_info = es.get(index='spark-jobs', doc_type='job', id=task_id)
		response = es_task_info['_source']
		response['state'] = task.state

	return jsonify(response)
