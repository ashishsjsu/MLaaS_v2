#this file is where he application is bootstrapped 

from application import Application
import os

#instantiate the application class
app = Application(debug=True)
# instantiate the celery object
celery = app.celery()

import tasks