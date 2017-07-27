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
from adplogger import logger
import time
from dataformatter import DataFormatter
from pyQueryConstructor import QueryConstructor


class Connector:
    def __init__(self, esEndpoint, dmonPort=5001, esInstanceEndpoint=9200, index="logstash-*"):
        self.esInstance = Elasticsearch(esEndpoint)
        self.esEndpoint = esEndpoint
        self.dmonPort = dmonPort
        self.esInstanceEndpoint = esInstanceEndpoint
        self.myIndex = index

    def query(self, queryBody, allm=True, dMetrics=[], debug=False):
        res = self.esInstance.search(index=self.myIndex, body=queryBody, request_timeout=230)
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
            logger.error('[%s] : [ERROR] Exception has occured while connecting to ES dmon with type %s at arguments %s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            return "An exception has occured with type %s at arguments %s" %(type(inst), inst.args)
            sys.exit(2)
        return res

    def roles(self):
        nUrl = "http://%s:%s/dmon/v1/overlord/nodes/roles" % (self.esEndpoint, self.dmonPort)
        logger.info('[%s] : [INFO] dmon get roles url -> %s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), nUrl)
        try:
            rRoles = requests.get(nUrl)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception has occured while connecting to dmon with type %s at arguments %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            print "Can't connect to dmon at %s port %s" % (self.esEndpoint, self.dmonPort)
            sys.exit(2)
        rData = rRoles.json()
        return rData

    def createIndex(self, indexName):
        try:
            self.esInstance.create(index=indexName, ignore=400)
            logger.info('[%s] : [INFO] Created index %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), indexName)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Failed to created index %s with %s and %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), indexName, type(inst), inst.args)

    def closeIndex(self, indexName):
        try:
            self.esInstance.close(index=indexName)
            logger.info('[%s] : [INFO] Closed index %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), indexName)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Failed to close index %s with %s and %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), indexName, type(inst),
                         inst.args)

    def deleteIndex(self, indexName):
        try:
            res = self.esInstance.indices.delete(index=indexName, ignore=[400, 404])
            logger.info('[%s] : [INFO] Deleted index %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), indexName)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Failed to delete index %s with %s and %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), indexName, type(inst),
                         inst.args)
            return 0
        return res

    def openIndex(self, indexName):
        res = self.esInstance.indices.open(index=indexName)
        logger.info('[%s] : [INFO] Open index %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), indexName)
        return res

    def getIndex(self, indexName):
        res = self.esInstance.indices.get(index=indexName, human=True)
        return res

    def getIndexSettings(self, indexName):
        res = self.esInstance.indices.get_settings(index=indexName, human=True)
        return res

    def clusterHealth(self):
        res = self.esInstance.cluster.health(request_timeout=15)
        return res

    def clusterSettings(self):
        res = self.esInstance.cluster.get_settings(request_timeout=15)
        return res

    def clusterState(self):
        res = self.esInstance.cluster.stats(human=True, request_timeout=15)
        return res

    def nodeInfo(self):
        res = self.esInstance.nodes.info(request_timeout=15)
        return res

    def nodeState(self):
        res = self.esInstance.nodes.stats(request_timeout=15)
        return res

    def getStormTopology(self):
        nUrl = "http://%s:%s/dmon/v1/overlord/detect/storm" % (self.esEndpoint, self.dmonPort)
        logger.info('[%s] : [INFO] dmon get storm topology url -> %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), nUrl)
        try:
            rStormTopology = requests.get(nUrl)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception has occured while connecting to dmon with type %s at arguments %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            print "Can't connect to dmon at %s port %s" % (self.esEndpoint, self.dmonPort)
            sys.exit(2)
        rData = rStormTopology.json()
        return rData

    def pushAnomaly(self, anomalyIndex, doc_type, body):
        try:
            res = self.esInstance.index(index=anomalyIndex, doc_type=doc_type, body=body)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception has occured while pushing anomaly with type %s at arguments %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            print "Can't push anomaly to dmon!"
            sys.exit(2)
        return res

    def getModel(self):
        return "getModel"

    def pushModel(self):
        return "push model"

    def localData(self):
        return "use local data"

    def getInterval(self):
        nUrl = "http://%s:%s/dmon/v1/overlord/aux/interval" % (self.esEndpoint, self.dmonPort)
        logger.info('[%s] : [INFO] dmon get interval url -> %s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), nUrl)
        try:
            rInterval = requests.get(nUrl)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception has occured while connecting to dmon with type %s at arguments %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            print "Can't connect to dmon at %s port %s" % (self.esEndpoint, self.dmonPort)
            sys.exit(2)
        rData = rInterval.json()
        return rData

    def aggQuery(self, queryBody):
        adt_timeout = os.environ['ADP_TIMEOUT'] = os.getenv('ADP_TIMEOUT', str(60)) # Set timeout as env variable ADT_TIMEOUT, if not set use default 60
        try:
            res = self.esInstance.search(index=self.myIndex, body=queryBody, request_timeout=float(adt_timeout))
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception while executing ES query with %s and %s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            sys.exit(2)
        return res

    def getNodeList(self):
        '''
        :return: -> returns the list of registered nodes from dmon
        '''
        nUrl = "http://%s:%s/dmon/v1/observer/nodes" % (self.esEndpoint, self.dmonPort)
        logger.info('[%s] : [INFO] dmon get node url -> %s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), nUrl)
        try:
            rdmonNode = requests.get(nUrl)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception has occured while connecting to dmon with type %s at arguments %s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            print "Can't connect to dmon at %s port %s" % (self.esEndpoint, self.dmonPort)
            sys.exit(2)
        rdata = rdmonNode.json()
        nodes = []
        for e in rdata['Nodes']:
            for k in e:
                nodes.append(k)
        return nodes

    def getDmonStatus(self):
        nUrl = "http://%s:%s/dmon/v1/overlord/core/status" % (self.esEndpoint, self.dmonPort)
        logger.info('[%s] : [INFO] dmon get core status url -> %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), nUrl)
        try:
            rdmonStatus = requests.get(nUrl)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception has occured while connecting to dmon with type %s at arguments %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            print "Can't connect to dmon at %s port %s" % (self.esEndpoint, self.dmonPort)
            sys.exit(2)
        return rdmonStatus.json()


if __name__ == '__main__':
    dataDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    #Standard query values
    # qte = 1475842980000
    # qlte = 1475845200000
    qgte = 1477561800000
    qlte = 1477562100000
    qsize = 0
    qinterval = "10s"

    dmonConnector = Connector('85.120.206.27')
    qConstructor = QueryConstructor()
    dformat = DataFormatter(dataDir)

    test = dmonConnector.clusterHealth()
    test2 = dmonConnector.clusterSettings()
    test3 = dmonConnector.clusterState()
    test4 = dmonConnector.nodeInfo()
    test5 = dmonConnector.nodeState()
    test6 = dmonConnector.getIndex('logstash-*')
    test7 = dmonConnector.getIndexSettings('logstash-*')

    body = {
            'timestamp': datetime.utcnow(),
            'anomaly': 'complex',
            'host': '10.0.0.0'
        }

    test8 = dmonConnector.pushAnomaly('testme', doc_type='d', body=body)

    print test
    print test2
    print test3
    print test4
    print test5
    print test6
    print test7
    print test8