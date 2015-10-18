#!/usr/bin/env bash

source venv/bin/activate
celery -A bootstrap.celery worker --loglevel=info --concurrency=1
