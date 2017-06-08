from dataformatter import DataFormatter
from pyQueryConstructor import QueryConstructor
import os
from dmonconnector import Connector
from adplogger import logger
from datetime import datetime
import time

if __name__ == '__main__':
    dataDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    modelDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')

    #Standard query values
    # qte = 1475842980000
    # qlte = 1475845200000
    qgte = 1477911300000
    qlte = 1477914720000

    qsize = 0
    qinterval = "10s"
    dmonEndpoint = '85.120.206.27'

    dmonConnector = Connector(dmonEndpoint)
    qConstructor = QueryConstructor()
    dformat = DataFormatter(dataDir)

    nodeList = dmonConnector.getNodeList()
    interval = dmonConnector.getInterval()

    if int(qinterval[:-1]) < interval['System']:
        logger.warning('[%s] : [WARN] System Interval smaller than set interval!',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))


    # per slave unique process name list
    nodeProcessReduce = {}
    nodeProcessMap = {}

    # Get host based metrics
    for node in nodeList:
        # Query and file string
        load, load_file = qConstructor.loadString(node)
        memory, memory_file = qConstructor.memoryString(node)
        interface, interface_file = qConstructor.interfaceString(node)
        packet, packet_file = qConstructor.packetString(node)
        nodeManager, nodeManager_file = qConstructor.nodeManagerString(node)
        jvmNodeManager, jvmNodeManager_file = qConstructor.jvmnodeManagerString(node)
        #jvmMapTask, jvmMapTask_file = qConstructor.jvmMapTask(node)
        datanode, datanode_file = qConstructor.datanodeString(node)
        shuffle, shuffle_file = qConstructor.shuffleString(node)


        # Queries
        qload = qConstructor.systemLoadQuery(load, qgte, qlte, qsize, qinterval)
        qmemory = qConstructor.systemMemoryQuery(memory, qgte, qlte, qsize, qinterval)
        qinterface = qConstructor.systemInterfaceQuery(interface, qgte, qlte, qsize, qinterval)
        qpacket = qConstructor.systemInterfaceQuery(packet, qgte, qlte, qsize, qinterval)
        qnodeManager = qConstructor.yarnNodeManager(nodeManager, qgte, qlte, qsize, qinterval)
        qjvmNodeManager = qConstructor.jvmNNquery(jvmNodeManager, qgte, qlte, qsize, qinterval)
        #qjvmMapTask = qConstructor.jvmNNquery(jvmMapTask, qgte, qlte, qsize, qinterval)
        qdatanode = qConstructor.datanodeMetricsQuery(datanode, qgte, qlte, qsize, qinterval)
        qshuffle = qConstructor.shuffleQuery(shuffle, qgte, qlte, qsize, qinterval)


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

        gshuffleResponse = dmonConnector.aggQuery(qshuffle)
        if gshuffleResponse['aggregations'].values()[0].values()[0]:
            dformat.dict2csv(gshuffleResponse, qshuffle, shuffle_file)
        else:
            logger.info('[%s] : [INFO] Empty response from  %s no shuffle metrics!',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)

        gdatanode = dmonConnector.aggQuery(qdatanode)
        if gdatanode['aggregations'].values()[0].values()[0]:
            dformat.dict2csv(gdatanode, qdatanode, datanode_file)
        else:
            logger.info('[%s] : [INFO] Empty response from  %s no datanode metrics!',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)


        # gjvmMapTaskResponse = dmonConnector.aggQuery(qjvmMapTask)
        # if gjvmMapTaskResponse['aggregations'].values()[0].values()[0]:
        #     dformat.dict2csv(gjvmMapTaskResponse, qjvmMapTask, jvmMapTask_file)
        # else:
        #     logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!',
        #                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)

        reduce = qConstructor.jvmRedProcessString(node)
        map = qConstructor.jvmMapProcessingString(node)

        qreduce = qConstructor.queryByProcess(reduce, qgte, qlte, 500, qinterval)
        qmap = qConstructor.queryByProcess(map, qgte, qlte, 500, qinterval)

        greduce = dmonConnector.aggQuery(qreduce)
        gmap = dmonConnector.aggQuery(qmap)


        uniqueReduce = set()
        for i in greduce['hits']['hits']:
            # print i['_source']['ProcessName']
            uniqueReduce.add(i['_source']['ProcessName'])
        nodeProcessReduce[node] = list(uniqueReduce)

        uniqueMap = set()
        for i in gmap['hits']['hits']:
            # print i['_source']['ProcessName']
            uniqueMap.add(i['_source']['ProcessName'])
        nodeProcessMap[node] = list(uniqueMap)

    # Get Process info by host and name
    for host, processes in nodeProcessReduce.iteritems():
        if processes:
            for process in processes:
                logger.info('[%s] : [INFO] Reduce process %s for host  %s found',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), process, host)
                hreduce, hreduce_file = qConstructor.jvmRedProcessbyNameString(host, process)
                qhreduce = qConstructor.jvmNNquery(hreduce, qgte, qlte, qsize, qinterval)
                ghreduce = dmonConnector.aggQuery(qhreduce)
                dformat.dict2csv(ghreduce, qhreduce, hreduce_file)
        else:
            logger.info('[%s] : [INFO] No reduce process for host  %s found',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), host)
            pass

    for host, processes in nodeProcessMap.iteritems():
        if processes:
            for process in processes:
                logger.info('[%s] : [INFO] Map process %s for host  %s found',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), process, host)
                hmap, hmap_file = qConstructor.jvmMapProcessbyNameString(host, process)
                qhmap = qConstructor.jvmNNquery(hmap, qgte, qlte, qsize, qinterval)
                ghmap = dmonConnector.aggQuery(qhmap)
                dformat.dict2csv(ghmap, qhmap, hmap_file)
        else:
            logger.info('[%s] : [INFO] No map process for host  %s found',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), host)
            pass

    # Get non host based metrics queries and file strings
    dfs, dfs_file = qConstructor.dfsString()
    dfsFs, dfsFs_file = qConstructor.dfsFString()
    jvmNameNodeString, jvmNameNode_file = qConstructor.jvmNameNodeString()
    queue, queue_file = qConstructor.queueResourceString()
    cluster, cluster_file = qConstructor.clusterMetricsSring()
    jvmResMng, jvmResMng_file = qConstructor.jvmResourceManagerString()
    mrapp, mrapp_file = qConstructor.mrappmasterString()
    jvmMrapp, jvmMrapp_file = qConstructor.jvmMrappmasterString()
    fsop, fsop_file = qConstructor.fsopDurationsString()

    # Queries
    qdfs = qConstructor.dfsQuery(dfs, qgte, qlte, qsize, qinterval)
    qdfsFs = qConstructor.dfsFSQuery(dfsFs, qgte, qlte, qsize, qinterval)
    qjvmNameNode = qConstructor.jvmNNquery(jvmNameNodeString, qgte, qlte, qsize, qinterval)
    qqueue = qConstructor.resourceQueueQuery(queue, qgte, qlte, qsize, qinterval)
    qcluster = qConstructor.clusterMetricsQuery(cluster, qgte, qlte, qsize, qinterval)
    qjvmResMng = qConstructor.jvmNNquery(jvmResMng, qgte, qlte, qsize, qinterval)
    qjvmMrapp = qConstructor.jvmNNquery(jvmMrapp, qgte, qlte, qsize, qinterval)
    qfsop = qConstructor.fsopDurationsQuery(fsop, qgte, qlte, qsize, qinterval)

    # Responses and convert to csv
    gdfs = dmonConnector.aggQuery(qdfs)
    dformat.dict2csv(gdfs, qdfs, dfs_file)

    gdfsFs = dmonConnector.aggQuery(qdfsFs)
    dformat.dict2csv(gdfsFs, qdfsFs, dfsFs_file)

    gjvmNameNode = dmonConnector.aggQuery(qjvmNameNode)
    dformat.dict2csv(gjvmNameNode, qjvmNameNode, jvmNameNode_file)

    gqueue = dmonConnector.aggQuery(qqueue)
    dformat.dict2csv(gqueue, qqueue, queue_file)

    gcluster = dmonConnector.aggQuery(qcluster)
    dformat.dict2csv(gcluster, qcluster, cluster_file)

    gjvmResourceManager = dmonConnector.aggQuery(qjvmResMng)
    dformat.dict2csv(gjvmResourceManager, qjvmResMng, jvmResMng_file)

    gjvmMrapp = dmonConnector.aggQuery(qjvmMrapp)
    dformat.dict2csv(gjvmMrapp, qjvmMrapp, jvmMrapp_file)

    gfsop = dmonConnector.aggQuery(qfsop)
    dformat.dict2csv(gfsop, qfsop, fsop_file)


    # Merge and rename by node system Files
    dformat.chainMergeSystem()

    #Merge system metricsall
    merged_df = dformat.chainMergeNR()
    dformat.df2csv(merged_df, os.path.join(dataDir, "System.csv"))

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

