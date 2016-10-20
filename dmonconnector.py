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

dataDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


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

    def pushAnomaly(self):
        return "push andomaly"

    def getModel(self):
        return "getModel"

    def pushModel(self):
        return "push model"

    def localData(self):
        return "use local data"

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

    #Standard query values

    qgte = 1475842980000
    qlte = 1475845200000
    qsize = 0
    qinterval = "1s"



    dmonConnector = Connector('85.120.206.27')
    qConstructor = QueryConstructor()
    dformat = DataFormatter(dataDir)

    nodeList = dmonConnector.getNodeList()
    print nodeList

    # Get host based metrics
    for node in nodeList:
        load, load_file = qConstructor.loadString(node)
        memory, memory_file = qConstructor.memoryString(node)
        interface, interface_file = qConstructor.interfaceString(node)
        packet, packet_file = qConstructor.packetString(node)
        nodeManager, nodeManager_file = qConstructor.nodeManagerString(node)
        jvmNodeManager, jvmNodeManager_file = qConstructor.jvmnodeManagerString(node)



        qload = qConstructor.systemLoadQuery(load, qgte, qlte, qsize, qinterval)
        qmemory = qConstructor.systemMemoryQuery(memory, qgte, qlte, qsize, qinterval)
        qinterface = qConstructor.systemInterfaceQuery(interface, qgte, qlte, qsize, qinterval)
        qpacket = qConstructor.systemInterfaceQuery(packet, qgte, qlte, qsize, qinterval)
        qnodeManager = qConstructor.yarnNodeManager(nodeManager, qgte, qlte, qsize, qinterval)
        qjvmNodeManager = qConstructor.jvmNNquery(jvmNodeManager, qgte, qlte, qsize, qinterval)


        # Execute query and convert response to csv
        qloadResponse = dmonConnector.aggQuery(qload)
        dformat.dict2csv(qloadResponse, qload, load_file)

        gmemoryResponse = dmonConnector.aggQuery(qmemory)
        #print gmemoryResponse
        dformat.dict2csv(gmemoryResponse, qmemory, memory_file)

        ginterfaceResponse = dmonConnector.aggQuery(qinterface)
        dformat.dict2csv(ginterfaceResponse, qinterface, interface_file)

        gpacketResponse = dmonConnector.aggQuery(qpacket)
        dformat.dict2csv(gpacketResponse, qpacket, packet_file)

        gnodeManagerResponse = dmonConnector.aggQuery(qnodeManager)
        dformat.dict2csv(gnodeManagerResponse, qnodeManager, nodeManager_file)

        gjvmNodeManagerResponse = dmonConnector.aggQuery(qjvmNodeManager)
        if gjvmNodeManagerResponse['aggregations'].values()[0].values()[0]:
            dformat.dict2csv(gjvmNodeManagerResponse, qjvmNodeManager, jvmNodeManager_file)
        else:
            logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)


    # Get non host based metrics
    dfs, dfs_file = qConstructor.dfsFString()
    dfsFs, dfsFs_file = qConstructor.dfsFString()
    jvmNameNodeString, jvmNameNode_file = qConstructor.jvmNameNodeString()

    qdfs = qConstructor.dfsQuery(dfs, qgte, qlte, qsize, qinterval)
    qdfsFs = qConstructor.dfsFSQuery(dfsFs, qgte, qlte, qsize, qinterval)
    qjvmNameNode = qConstructor.jvmNNquery(jvmNameNodeString, qgte, qlte, qsize, qinterval)

    gdfs = dmonConnector.aggQuery(qdfs)
    dformat.dict2csv(gdfs, qdfs, dfs_file)

    gdfsFs = dmonConnector.aggQuery(qdfsFs)
    dformat.dict2csv(gdfsFs, qdfsFs, dfsFs_file)

    gjvmNameNode = dmonConnector.aggQuery(qjvmNameNode)
    dformat.dict2csv(gjvmNameNode, qjvmNameNode, jvmNameNode_file)






    #print testConnector.info()
    #response = testConnector.aggQuery(query)
    # logger.info('This is a test')
    #response2 = testConnector.aggQuery(query2)
    # dformat = DataFormatter(dataDir)
    #
    # dformat.dict2csv(response, query, 'test2.csv')
    # dformat.dict2csv(response2, query2, 'test22.csv')
    #
    # dformat.dict2arff('test2.csv', 'test2.arff')

    #responseSystem = testConnector.aggQuery(systemRequest)
    #print responseSystem



    #print type(response['aggregations'])
    #print len(response)
    #print response2
    #print len(response2)



