from __future__ import print_function
import sys
import json
from pyspark import SparkContext, SparkConf
from operator import add

from elasticsearch import Elasticsearch

ES_HOST = {
    "host": 'localhost',
    "port": 9200
}

es = Elasticsearch(hosts=[ES_HOST])

if __name__ == "__main__":
  
    if len(sys.argv) < 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    
    task_id = sys.argv[2]
  
    #update task status in ES
    es.update(index="spark-jobs", doc_type="job", id=task_id, body={
        'doc': {
            'current': 1,
            'status': 'Spark job started...'
        }
    })

    #============================================================
    #create spark context
    conf = SparkConf().setAppName('PythonWordCount')
    sc = SparkContext(conf=conf)
  
    lines = sc.textFile(sys.argv[1], 1)
  
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()

    output_dict = {}

    for (word, count) in output:
        print("%s: %i" % (word, count))
        output_dict[str(word)] = count
    #============================================================

    out = json.dumps(output_dict)
    #update task status with result in ES
    es.update(index='spark-jobs', doc_type='job', id=task_id, body={
        'doc':{
            'current': 100,
            'status': 'Spark job finished...',
            'result': out
        }
    })

    sc.stop()
