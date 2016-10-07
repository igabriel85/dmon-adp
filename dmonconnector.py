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

from datetime import datetime
from elasticsearch import Elasticsearch
import csv
import unicodedata
import requests
import os
import sys, getopt


class Connector():

    def __init__(self, esEndpoint):
        self.esInstance = Elasticsearch(esEndpoint)

    def query(self, queryBody, allm=True, dMetrics=[], debug=False, myIndex="logstash-*"):
        res = self.esInstance.search(index=myIndex, body=queryBody)
        if debug == True:
            print "%---------------------------------------------------------%"
            print "Raw JSON Ouput"
            print res
            print("%d documents found" % res['hits']['total'])
            print "%---------------------------------------------------------%"
        termsList = []
        termValues = []
        ListMetrics = []
        for doc in res['hits']['hits']:
            if allm == False:
                if not dMetrics:
                    sys.exit("dMetrics argument not set. Please supply valid list of metrics!")
                for met in dMetrics:
                    # prints the values of the metrics defined in the metrics list
                    if debug == True:
                        print "%---------------------------------------------------------%"
                        print "Parsed Output -> ES doc id, metrics, metrics values."
                        print("doc id %s) metric %s -> value %s" % (doc['_id'], met, doc['_source'][met]))
                        print "%---------------------------------------------------------%"
                    termsList.append(met)
                    termValues.append(doc['_source'][met])
                dictValues = dict(zip(termsList, termValues))
            else:
                for terms in doc['_source']:
                    # prints the values of the metrics defined in the metrics list
                    if debug == True:
                        print "%---------------------------------------------------------%"
                        print "Parsed Output -> ES doc id, metrics, metrics values."
                        print("doc id %s) metric %s -> value %s" % (doc['_id'], terms, doc['_source'][terms]))
                        print "%---------------------------------------------------------%"
                    termsList.append(terms)
                    termValues.append(doc['_source'][terms])
                    dictValues = dict(zip(termsList, termValues))
            ListMetrics.append(dictValues)
        return ListMetrics, res

    def info(self):
        try:
            res = self.esInstance.info()
        except Exception as inst:
            return "An exception has occured with type %s at arguments %s" %(type(inst), inst.args)
        return res

    def pushAnomaly(self):
        return "push andomaly"

    def getModel(self):
        return "getModel"

    def pushModel(self):
        return "push model"

    def localData(self):
        return "use local data"


if __name__ == '__main__':
    testConnector = Connector('85.120.206.27')
    print testConnector.info()