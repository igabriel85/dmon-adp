from dataformatter import DataFormatter
from pyQueryConstructor import QueryConstructor
import os
from dmonconnector import Connector
from adplogger import logger
from datetime import datetime
import time

if __name__ == '__main__':
    dataDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    #Standard query values
    # qte = 1475842980000
    # qlte = 1475845200000
    qgte = 1475842980000
    qlte = 1475845200000
    qsize = 0
    qinterval = "10s"



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
        if gnodeManagerResponse['aggregations'].values()[0].values()[0]:
            dformat.dict2csv(gnodeManagerResponse, qnodeManager, nodeManager_file)
        else:
            logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)



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