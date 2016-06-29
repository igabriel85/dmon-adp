"""

Copyright 2015, Institute e-Austria, Timisoara, Romania
    http://www.ieat.ro/
Developers:
 * Gabriel Iuhasz, iuhasz.gabriel@info.uvt.ro

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at:
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from flask import send_file
from flask import request
from flask.ext.restplus import Resource, fields
import os
import jinja2
import sys
import subprocess
import platform
import logging
from logging.handlers import RotatingFileHandler

from app import *


#directory locations
tmpDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')


@adp.route('/v1/datasets')
class ListDatasets(Resource):
    def get(self):
        return "List of all Datasets"


@adp.route('/v1/datasets')
class DatasetList(Resource):
    def get(self):
        return "List of Datasets"

@adp.route('/v1/datasets/<datasetname>')
class DatasetInfo(Resource):
    def get(self, datasetname):
        return "List status on " + datasetname

    def put(self, datasetname):
        return "Add description of dataset" + datasetname

    def post(self, datasetname):
        return "Execute query and dataset generation based on description"


@adp.route('/v1/jobs')
class AdpJobController(Resource):
    def get(self):
        return "List job status based on payload"

    def put(self):
        return "Describe Job"

    def post(self):
        return "Execute job based on descriptor"


@adp.route('/v1/configure')
class AdpConf(Resource):
    def get(self):
        return "Current configuration of ADP"

    def put(self):
        return "Change current configuration for ADP"

    def post(self):
        return "Enact  configuration"