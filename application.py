from __future__ import absolute_import
from flask import Flask, request, session, flash, redirect, url_for, jsonify
from celery import Celery
import os
from routes import sparktask_page



class Application(object):

	# define app constructor
	def __init__(self, debug=True):
		self.flask_app = Flask(__name__)
		self.debug = debug
		self._register_routes()
		self._configure_cors()

	#create a new celery object and configure it, this is standard code
	def celery(self):
		app = self.flask_app
		celery = Celery(app.import_name)
		celery.config_from_object('celeryconfig')

		TaskBase = celery.Task

		class ContextTask(TaskBase):
			abstract = True

			def __call__(self, *args, **kwargs):
				with app.app_context():
					return TaskBase.__call__(self, *args, **kwargs)
		celery.Task = ContextTask

		return celery

	def _configure_cors(self):
		@self.flask_app.after_request
		def add_cors(response):
			response.headers.add('Access-Control-Allow-Origin', '*')
			response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
			response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
			return response

	def _register_routes(self):
		#register the routes using blueprint
		self.flask_app.register_blueprint(sparktask_page)

	def start_app(self):
		self.flask_app.run(debug=self.debug)