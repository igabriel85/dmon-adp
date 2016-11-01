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


class Connector():
    def __init__(self, esEndpoint, dmonPort=5001, esInstanceEndpoint=9200, index="logstash-*"):
        self.esInstance = Elasticsearch(esEndpoint)
        self.esEndpoint = esEndpoint
        self.dmonPort = dmonPort
        self.esInstanceEndpoint = esInstanceEndpoint
        self.myIndex = index

    def query(self, queryBody, allm=True, dMetrics=[], debug=False):
        res = self.esInstance.search(index=self.myIndex, body=queryBody)
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

    def pushAnomaly(self):
        return "push andomaly"

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
        try:
            res = self.esInstance.search(index=self.myIndex, body=queryBody)
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

    # nodeList = dmonConnector.getNodeList()
    #
    # nodeProcess = {}
    # nodeProcessM = {}
    # for node in nodeList:
    #     testS = qConstructor.jvmRedProcessString(node)
    #     testS2 = qConstructor.jvmMapProcessingString(node)
    #     qtest = qConstructor.queryByProcess(testS, qgte, qlte, 500, qinterval, wildCard=True, qtformat="epoch_millis",
    #                         qmin_doc_count=1)
    #     qtest2 = qConstructor.queryByProcess(testS2, qgte, qlte, 500, qinterval, wildCard=True, qtformat="epoch_millis",
    #                                         qmin_doc_count=1)
    #     test = dmonConnector.aggQuery(qtest)
    #     test2 = dmonConnector.aggQuery(qtest2)
    #
    #     #print test['hits']['hits']
    #     unique = set()
    #     for i in test['hits']['hits']:
    #         #print i['_source']['ProcessName']
    #         unique.add(i['_source']['ProcessName'])
    #     nodeProcess[node] = list(unique)
    #
    #     unique2 = set()
    #     for i in test2['hits']['hits']:
    #         #print i['_source']['ProcessName']
    #         unique2.add(i['_source']['ProcessName'])
    #     nodeProcessM[node] = list(unique2)
    #
    #
    # print nodeProcess
    # for k, v in nodeProcess.iteritems():
    #     if v:
    #         for e in v:
    #             test22, test22_file = qConstructor.jvmRedProcessbyNameString(k, e)
    #             qtest = qConstructor.jvmNNquery(test22, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
    #                                         qmin_doc_count=1)
    #             test = dmonConnector.aggQuery(qtest)
    #             print test
    #             dformat.dict2csv(test, qtest, test22_file)
    #     else:
    #         pass
    #
    # print nodeProcessM
    # for k, v in nodeProcessM.iteritems():
    #     if v:
    #         for e in v:
    #             test21, test21_file = qConstructor.jvmMapProcessbyNameString(k, e)
    #             qtest = qConstructor.jvmNNquery(test21, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
    #                                         qmin_doc_count=1)
    #             test = dmonConnector.aggQuery(qtest)
    #             print test
    #             dformat.dict2csv(test, qtest, test21_file)
    #     else:
    #         pass

    #testquery = {'query': {'filtered': {'filter': {'bool': {'must_not': [], 'must': [{'range': {'@timestamp': {'gte': 1477561800000, 'lte': 1477562100000, 'format': 'epoch_millis'}}}]}}, 'query': {'query_string': {'query': 'serviceMetrics:\"ShuffleMetrics\" AND serviceType:\"mapred\" AND hostname:\"dice.cdh.master\"', 'analyze_wildcard': True}}}}, 'aggs': {'2': {'date_histogram': {'field': '@timestamp', 'interval': '10s', 'time_zone': 'Europe/Helsinki', 'min_doc_count': 1, 'extended_bounds': {'max': 1477562100000, 'min': 1477561800000}}, 'aggs': {'1': {'avg': {'field': 'ShuffleConnections'}}, '3': {'avg': {'field': 'ShuffleOutputBytes'}}, '5': {'avg': {'field': 'ShuffleOutputsOK'}}, '4': {'avg': {'field': 'ShuffleOutputsFailed'}}}}}, 'size': 0}
    sshuffle , t = qConstructor.shuffleString('dice.cdh.slave1')
    qshuffle = qConstructor.shuffleQuery(sshuffle, qgte, qlte, qsize, qinterval)

    testshuffle = dmonConnector.aggQuery(qshuffle)

    print testshuffle