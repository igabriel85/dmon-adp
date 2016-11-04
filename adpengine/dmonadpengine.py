import os
from dmonconnector import *
from util import queryParser, nodesParse
settings = {'load': '<model_name>', 'qsize': 'qs', 'export': '<model_name>', 'file': None, 'query': 'yarn:resourcemanager, clustre, jvm_NM;system', 'index': 'logstash-*', 'detect': False, 'from': '1444444444', 'to': '1455555555', 'sload': 'load_threashold', 'nodes': 0, 'method': 'method_name', 'snetwork': 'net_threashold', 'train': True, 'esInstanceEndpoint': 9200, 'validate': False, 'model': '<model_name>', 'qinterval': '10s', 'dmonPort': 5001, 'esendpoint': '85.120.206.27', 'smemory': 'mem_threashold', 'MethodSettings': {'set1': 'none', 'set2': 'none', 'set3': 'none'}}


class AdpEngine:
    def __init__(self, settingsDict, dataDir, modelsDir):
        self.esendpoint = settingsDict['esendpoint']
        self.esInstanceEndpoint = settingsDict['esInstanceEndpoint']
        self.dmonPort = settingsDict['dmonPort']
        self.index = settingsDict['index']
        self.tfrom = int(settingsDict['from'])
        self.to = int(settingsDict['to'])
        self.query = settingsDict['query']
        self.qsize = settingsDict['qsize']
        self.nodes = nodesParse(settingsDict['nodes'])
        self.qinterval = settingsDict['qinterval']
        self.train = settingsDict['train']
        self.model = settingsDict['model']
        self.load = settingsDict['load']
        self.method = settingsDict['method']
        self.validate = settingsDict['validate']
        self.export = settingsDict['export']
        self.detect = settingsDict['detect']
        self.sload = settingsDict['sload']
        self.smemory = settingsDict['smemory']
        self.snetwork = settingsDict['snetwork']
        self.methodSettings = settingsDict['MethodSettings']
        self.dataDir = dataDir
        self.modelsDir = modelsDir
        self.anomalyIndex = "anomalies"
        self.dmonConnector = Connector(self.esendpoint)
        self.qConstructor = QueryConstructor()
        self.dformat = DataFormatter(self.dataDir)
        self.regnodeList = []

    def initConnector(self):
        print "Establishing connection with dmon ....."
        resdmonInfo = self.dmonConnector.getDmonStatus()
        print "Connection established, status %s" %resdmonInfo
        resInfo = self.dmonConnector.info()
        print "General es dmon info -> %s" %resInfo

        interval = self.dmonConnector.getInterval()

        if int(self.qinterval[:-1]) < interval['System']:
            logger.warning('[%s] : [WARN] System Interval smaller than set interval!',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            print "Warning query interval difference detected, dmon interval is %s while adp is %s!" %(self.qinterval, interval['System'])
        else:
            print "Query interval check passed."
            logger.info('[%s] : [INFO] Query interval check passed!',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        resClusterState = self.dmonConnector.clusterHealth()
        print "ES cluster health -> %s" %resClusterState

        # print "Checking index %s state ...." %self.index
        # resGetIndex = self.dmonConnector.getIndex(self.index)
        # print "Index %s state -> %s" %(self.index, resGetIndex)

        print "Checking dmon registered nodes...."
        self.regnodeList = self.dmonConnector.getNodeList()
        print "Nodes found -> %s" %self.regnodeList

    def getData(self):
        queryd = queryParser(self.query)
        logger.info('[%s] : [INFO] Checking node list',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        desNodes = []
        if not self.nodes:
            desNodes = self.dmonConnector.getNodeList()
            logger.info('[%s] : [INFO] Metrics from all nodes will be collected ',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        else:
            if set(self.nodes).issubset(set(self.regnodeList)):
                desNodes = self.nodes
                logger.info('[%s] : [INFO] Metrics from %s nodes will be collected ',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(desNodes))
            else:
                logger.error('[%s] : [ERROR] Registred nodes %s do not contain desired nodes %s ',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(self.regnodeList), str(desNodes))
                sys.exit(1)

        if 'system' in queryd:
            if queryd['system'] == 0:
                print "Starting query for system metrics ...."
                for node in desNodes:
                    load, load_file = self.qConstructor.loadString(node)
                    memory, memory_file = self.qConstructor.memoryString(node)
                    interface, interface_file = self.qConstructor.interfaceString(node)
                    packet, packet_file = self.qConstructor.packetString(node)

                    # Queries
                    qload = self.qConstructor.systemLoadQuery(load, self.tfrom, self.to, self.qsize, self.qinterval)
                    qmemory = self.qConstructor.systemMemoryQuery(memory, self.tfrom, self.to, self.qsize, self.qinterval)
                    qinterface = self.qConstructor.systemInterfaceQuery(interface, self.tfrom, self.to, self.qsize, self.qinterval)
                    qpacket = self.qConstructor.systemInterfaceQuery(packet, self.tfrom, self.to, self.qsize, self.qinterval)

                    # Execute query and convert response to csv
                    qloadResponse = self.dmonConnector.aggQuery(qload)
                    self.dformat.dict2csv(qloadResponse, qload, load_file)

                    gmemoryResponse = self.dmonConnector.aggQuery(qmemory)
                    # print gmemoryResponse
                    self.dformat.dict2csv(gmemoryResponse, qmemory, memory_file)

                    ginterfaceResponse = self.dmonConnector.aggQuery(qinterface)
                    self.dformat.dict2csv(ginterfaceResponse, qinterface, interface_file)

                    gpacketResponse = self.dmonConnector.aggQuery(qpacket)
                    self.dformat.dict2csv(gpacketResponse, qpacket, packet_file)

                # Merge and rename by node system Files
                print "Query complete startin merge ..."
                self.dformat.chainMergeSystem()

                # Merge system metricsall
                merged_df = self.dformat.chainMergeNR()
                self.dformat.df2csv(merged_df, os.path.join(self.dataDir, "System.csv"))
                print "System Metrics merge complete"
            else:
                print "Only for all system metrics available" #todo for metrics types
        if 'yarn' in queryd:
            print "Starting query for yarn metrics"
            if queryd['yarn'] == 0:
                # per slave unique process name list
                nodeProcessReduce = {}
                nodeProcessMap = {}
                for node in desNodes:
                    nodeManager, nodeManager_file = self.qConstructor.nodeManagerString(node)
                    jvmNodeManager, jvmNodeManager_file = self.qConstructor.jvmnodeManagerString(node)
                    datanode, datanode_file = self.qConstructor.datanodeString(node)
                    shuffle, shuffle_file = self.qConstructor.shuffleString(node)
                    qnodeManager = self.qConstructor.yarnNodeManager(nodeManager, self.tfrom, self.to, self.qsize, self.qinterval)
                    qjvmNodeManager = self.qConstructor.jvmNNquery(jvmNodeManager, self.tfrom, self.to, self.qsize, self.qinterval)
                    qdatanode = self.qConstructor.datanodeMetricsQuery(datanode, self.tfrom, self.to, self.qsize, self.qinterval)
                    qshuffle = self.qConstructor.shuffleQuery(shuffle, self.tfrom, self.to, self.qsize, self.qinterval)

                    gnodeManagerResponse = self.dmonConnector.aggQuery(qnodeManager)
                    if gnodeManagerResponse['aggregations'].values()[0].values()[0]:
                        self.dformat.dict2csv(gnodeManagerResponse, qnodeManager, nodeManager_file)
                    else:
                        logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!',
                                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)

                    gjvmNodeManagerResponse = self.dmonConnector.aggQuery(qjvmNodeManager)
                    if gjvmNodeManagerResponse['aggregations'].values()[0].values()[0]:
                        self.dformat.dict2csv(gjvmNodeManagerResponse, qjvmNodeManager, jvmNodeManager_file)
                    else:
                        logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!',
                                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)

                    gshuffleResponse = self.dmonConnector.aggQuery(qshuffle)
                    if gshuffleResponse['aggregations'].values()[0].values()[0]:
                        self.dformat.dict2csv(gshuffleResponse, qshuffle, shuffle_file)
                    else:
                        logger.info('[%s] : [INFO] Empty response from  %s no shuffle metrics!',
                                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)

                    gdatanode = self.dmonConnector.aggQuery(qdatanode)
                    if gdatanode['aggregations'].values()[0].values()[0]:
                        self.dformat.dict2csv(gdatanode, qdatanode, datanode_file)
                    else:
                        logger.info('[%s] : [INFO] Empty response from  %s no datanode metrics!',
                                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)

                    reduce = self.qConstructor.jvmRedProcessString(node)
                    map = self.qConstructor.jvmMapProcessingString(node)

                    qreduce = self.qConstructor.queryByProcess(reduce, self.tfrom, self.to, 500, self.qinterval)
                    qmap = self.qConstructor.queryByProcess(map, self.tfrom, self.to, 500, self.qinterval)

                    greduce = self.dmonConnector.aggQuery(qreduce)
                    gmap = self.dmonConnector.aggQuery(qmap)

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
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), process,
                                            host)
                            hreduce, hreduce_file = self.qConstructor.jvmRedProcessbyNameString(host, process)
                            qhreduce = self.qConstructor.jvmNNquery(hreduce, self.tfrom, self.to, self.qsize, self.qinterval)
                            ghreduce = self.dmonConnector.aggQuery(qhreduce)
                            self.dformat.dict2csv(ghreduce, qhreduce, hreduce_file)
                    else:
                        logger.info('[%s] : [INFO] No reduce process for host  %s found',
                                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), host)
                        pass

                for host, processes in nodeProcessMap.iteritems():
                    if processes:
                        for process in processes:
                            logger.info('[%s] : [INFO] Map process %s for host  %s found',
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), process,
                                            host)
                            hmap, hmap_file = self.qConstructor.jvmMapProcessbyNameString(host, process)
                            qhmap = self.qConstructor.jvmNNquery(hmap, self.tfrom, self.to, self.qsize, self.qinterval)
                            ghmap = self.dmonConnector.aggQuery(qhmap)
                            self.dformat.dict2csv(ghmap, qhmap, hmap_file)
                    else:
                        logger.info('[%s] : [INFO] No map process for host  %s found',
                                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), host)
                        pass

                        # Get non host based metrics queries and file strings
                dfs, dfs_file = self.qConstructor.dfsFString()
                dfsFs, dfsFs_file = self.qConstructor.dfsFString()
                jvmNameNodeString, jvmNameNode_file = self.qConstructor.jvmNameNodeString()
                queue, queue_file = self.qConstructor.queueResourceString()
                cluster, cluster_file = self.qConstructor.clusterMetricsSring()
                jvmResMng, jvmResMng_file = self.qConstructor.jvmResourceManagerString()
                mrapp, mrapp_file = self.qConstructor.mrappmasterString()  #todo
                jvmMrapp, jvmMrapp_file = self.qConstructor.jvmMrappmasterString()
                fsop, fsop_file = self.qConstructor.fsopDurationsString()

                # Queries
                qdfs = self.qConstructor.dfsQuery(dfs, self.tfrom, self.to, self.qsize, self.qinterval)
                qdfsFs = self.qConstructor.dfsFSQuery(dfsFs, self.tfrom, self.to, self.qsize, self.qinterval)
                qjvmNameNode = self.qConstructor.jvmNNquery(jvmNameNodeString, self.tfrom, self.to, self.qsize, self.qinterval)
                qqueue = self.qConstructor.resourceQueueQuery(queue, self.tfrom, self.to, self.qsize, self.qinterval)
                qcluster = self.qConstructor.clusterMetricsQuery(cluster, self.tfrom, self.to, self.qsize, self.qinterval)
                qjvmResMng = self.qConstructor.jvmNNquery(jvmResMng, self.tfrom, self.to, self.qsize, self.qinterval)
                qjvmMrapp = self.qConstructor.jvmNNquery(jvmMrapp, self.tfrom, self.to, self.qsize, self.qinterval)
                qfsop = self.qConstructor.fsopDurationsQuery(fsop, self.tfrom, self.to, self.qsize, self.qinterval)


                # Responses and convert to csv
                gdfs = self.dmonConnector.aggQuery(qdfs)
                self.dformat.dict2csv(gdfs, qdfs, dfs_file)

                gdfsFs = self.dmonConnector.aggQuery(qdfsFs)
                self.dformat.dict2csv(gdfsFs, qdfsFs, dfsFs_file)

                gjvmNameNode = self.dmonConnector.aggQuery(qjvmNameNode)
                self.dformat.dict2csv(gjvmNameNode, qjvmNameNode, jvmNameNode_file)

                gqueue = self.dmonConnector.aggQuery(qqueue)
                self.dformat.dict2csv(gqueue, qqueue, queue_file)

                gcluster = self.dmonConnector.aggQuery(qcluster)
                self.dformat.dict2csv(gcluster, qcluster, cluster_file)

                gjvmResourceManager = self.dmonConnector.aggQuery(qjvmResMng)
                self.dformat.dict2csv(gjvmResourceManager, qjvmResMng, jvmResMng_file)

                gjvmMrapp = self.dmonConnector.aggQuery(qjvmMrapp)
                self.dformat.dict2csv(gjvmMrapp, qjvmMrapp, jvmMrapp_file)

                gfsop = self.dmonConnector.aggQuery(qfsop)
                self.dformat.dict2csv(gfsop, qfsop, fsop_file)
                print "Query for yarn metrics complete starting merge..."

                #todo

                print "Yarn metrics merge complete"
            else:
                # cluster, nn, nm, dfs, dn, mr
                for el in queryd['yarn']:
                    if el == 'cluster':
                        self.getCluster()
                    if el == 'nn':
                        self.getNameNode()
                    if el == 'nm':
                        self.getNodeManager(desNodes)
                    if el == 'dfs':
                        self.getDFS()
                    if el == 'dn':
                        self.getDataNode(desNodes)
                    if el =='mr':
                        self.getMapnReduce(desNodes)
                    if el not in ['cluster', 'nn', 'nm', 'dfs', 'dn', 'mr']:
                        logger.error('[%s] : [ERROR] Unknown metrics context %s',
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), el)
                        sys.exit(1)
            print "Finished query for yarn metrics"
        elif 'spark' in queryd:
            print "Spark metrics" #todo
        elif 'storm' in queryd:
            print "Storm metrics" #todo

        return queryd

    def trainMethod(self):
        # use threads
        return "train method/s"

    def runMethod(self):
        # use threads
        return "select and run methods for a given time interval until adp exits"

    def loadModel(self):
        return "model"

    def reportAnomaly(self):
        return "anomaly"

    def getDFS(self):
        # Query Strings
        print "Querying DFS metrics"
        logger.info('[%s] : [INFO] Querying DFS metrics...',
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        dfs, dfs_file = self.qConstructor.dfsFString()
        dfsFs, dfsFs_file = self.qConstructor.dfsFString()
        fsop, fsop_file = self.qConstructor.fsopDurationsString()

        # Query constructor
        qdfs = self.qConstructor.dfsQuery(dfs, self.tfrom, self.to, self.qsize, self.qinterval)
        qdfsFs = self.qConstructor.dfsFSQuery(dfsFs, self.tfrom, self.to, self.qsize, self.qinterval)
        qfsop = self.qConstructor.fsopDurationsQuery(fsop, self.tfrom, self.to, self.qsize, self.qinterval)

        # Execute query
        gdfs = self.dmonConnector.aggQuery(qdfs)
        self.dformat.dict2csv(gdfs, qdfs, dfs_file)

        gdfsFs = self.dmonConnector.aggQuery(qdfsFs)
        self.dformat.dict2csv(gdfsFs, qdfsFs, dfsFs_file)

        gfsop = self.dmonConnector.aggQuery(qfsop)
        self.dformat.dict2csv(gfsop, qfsop, fsop_file)

        print "Querying DFS metrics complete."
        logger.info('[%s] : [INFO] Querying DFS metrics complete.',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

    def getNodeManager(self, nodes):
        print "Querying  Node Manager and Shuffle metrics ..."
        logger.info('[%s] : [INFO] Querying  Node Manager and Shuffle metrics...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        for node in nodes:
            nodeManager, nodeManager_file = self.qConstructor.nodeManagerString(node)
            jvmNodeManager, jvmNodeManager_file = self.qConstructor.jvmnodeManagerString(node)
            shuffle, shuffle_file = self.qConstructor.shuffleString(node)

            qnodeManager = self.qConstructor.yarnNodeManager(nodeManager, self.tfrom, self.to, self.qsize,
                                                             self.qinterval)
            qjvmNodeManager = self.qConstructor.jvmNNquery(jvmNodeManager, self.tfrom, self.to, self.qsize,
                                                           self.qinterval)
            qshuffle = self.qConstructor.shuffleQuery(shuffle, self.tfrom, self.to, self.qsize, self.qinterval)

            gnodeManagerResponse = self.dmonConnector.aggQuery(qnodeManager)
            if gnodeManagerResponse['aggregations'].values()[0].values()[0]:
                self.dformat.dict2csv(gnodeManagerResponse, qnodeManager, nodeManager_file)
            else:
                logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)

            gjvmNodeManagerResponse = self.dmonConnector.aggQuery(qjvmNodeManager)
            if gjvmNodeManagerResponse['aggregations'].values()[0].values()[0]:
                self.dformat.dict2csv(gjvmNodeManagerResponse, qjvmNodeManager, jvmNodeManager_file)
            else:
                logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)

            gshuffleResponse = self.dmonConnector.aggQuery(qshuffle)
            if gshuffleResponse['aggregations'].values()[0].values()[0]:
                self.dformat.dict2csv(gshuffleResponse, qshuffle, shuffle_file)
            else:
                logger.info('[%s] : [INFO] Empty response from  %s no shuffle metrics!',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)
        print "Querying  Node Manager and Shuffle metrics complete."
        logger.info('[%s] : [INFO] Querying  Node Manager and Shuffle metrics complete...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

    def getNameNode(self):
        print "Querying  Name Node metrics ..."
        logger.info('[%s] : [INFO] Querying  Name Node metrics ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        jvmNameNodeString, jvmNameNode_file = self.qConstructor.jvmNameNodeString()
        qjvmNameNode = self.qConstructor.jvmNNquery(jvmNameNodeString, self.tfrom, self.to, self.qsize, self.qinterval)
        gjvmNameNode = self.dmonConnector.aggQuery(qjvmNameNode)
        self.dformat.dict2csv(gjvmNameNode, qjvmNameNode, jvmNameNode_file)
        print "Querying  Name Node metrics complete"
        logger.info('[%s] : [INFO] Querying  Name Node metrics complete',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

    def getCluster(self):
        print "Querying  Cluster metrics ..."
        logger.info('[%s] : [INFO] Querying  Name Node metrics ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        queue, queue_file = self.qConstructor.queueResourceString()
        cluster, cluster_file = self.qConstructor.clusterMetricsSring()
        jvmMrapp, jvmMrapp_file = self.qConstructor.jvmMrappmasterString()
        jvmResMng, jvmResMng_file = self.qConstructor.jvmResourceManagerString()

        qjvmMrapp = self.qConstructor.jvmNNquery(jvmMrapp, self.tfrom, self.to, self.qsize, self.qinterval)
        qqueue = self.qConstructor.resourceQueueQuery(queue, self.tfrom, self.to, self.qsize, self.qinterval)
        qcluster = self.qConstructor.clusterMetricsQuery(cluster, self.tfrom, self.to, self.qsize, self.qinterval)
        qjvmResMng = self.qConstructor.jvmNNquery(jvmResMng, self.tfrom, self.to, self.qsize, self.qinterval)

        gqueue = self.dmonConnector.aggQuery(qqueue)
        self.dformat.dict2csv(gqueue, qqueue, queue_file)

        gcluster = self.dmonConnector.aggQuery(qcluster)
        self.dformat.dict2csv(gcluster, qcluster, cluster_file)

        gjvmMrapp = self.dmonConnector.aggQuery(qjvmMrapp)
        self.dformat.dict2csv(gjvmMrapp, qjvmMrapp, jvmMrapp_file)

        gjvmResourceManager = self.dmonConnector.aggQuery(qjvmResMng)
        self.dformat.dict2csv(gjvmResourceManager, qjvmResMng, jvmResMng_file)

        print "Querying  Cluster metrics complete"
        logger.info('[%s] : [INFO] Querying  Name Node metrics complete',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

    def getMapnReduce(self, nodes):
        # per slave unique process name list
        nodeProcessReduce = {}
        nodeProcessMap = {}
        print "Querying  Mapper and Reducer metrics ..."
        logger.info('[%s] : [INFO] Querying  Mapper and Reducer metrics ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        for node in nodes:
            reduce = self.qConstructor.jvmRedProcessString(node)
            map = self.qConstructor.jvmMapProcessingString(node)

            qreduce = self.qConstructor.queryByProcess(reduce, self.tfrom, self.to, 500, self.qinterval)
            qmap = self.qConstructor.queryByProcess(map, self.tfrom, self.to, 500, self.qinterval)

            greduce = self.dmonConnector.aggQuery(qreduce)
            gmap = self.dmonConnector.aggQuery(qmap)

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

        print "Querying  Reducer metrics ..."
        logger.info('[%s] : [INFO] Querying  Reducer metrics ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        for host, processes in nodeProcessReduce.iteritems():
            if processes:
                for process in processes:
                    logger.info('[%s] : [INFO] Reduce process %s for host  %s found',
                                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), process,
                                host)
                    hreduce, hreduce_file = self.qConstructor.jvmRedProcessbyNameString(host, process)
                    qhreduce = self.qConstructor.jvmNNquery(hreduce, self.tfrom, self.to, self.qsize, self.qinterval)
                    ghreduce = self.dmonConnector.aggQuery(qhreduce)
                    self.dformat.dict2csv(ghreduce, qhreduce, hreduce_file)
            else:
                logger.info('[%s] : [INFO] No reduce process for host  %s found',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), host)
                pass
        print "Querying  Reducer metrics complete"
        logger.info('[%s] : [INFO] Querying  Reducer metrics complete',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        print "Querying  Mapper metrics ..."
        logger.info('[%s] : [INFO] Querying  Mapper metrics ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        for host, processes in nodeProcessMap.iteritems():
            if processes:
                for process in processes:
                    logger.info('[%s] : [INFO] Map process %s for host  %s found',
                                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), process,
                                host)
                    hmap, hmap_file = self.qConstructor.jvmMapProcessbyNameString(host, process)
                    qhmap = self.qConstructor.jvmNNquery(hmap, self.tfrom, self.to, self.qsize, self.qinterval)
                    ghmap = self.dmonConnector.aggQuery(qhmap)
                    self.dformat.dict2csv(ghmap, qhmap, hmap_file)
            else:
                logger.info('[%s] : [INFO] No map process for host  %s found',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), host)
                pass
        print "Querying  Mapper metrics complete"
        logger.info('[%s] : [INFO] Querying  Mapper metrics complete',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

    def getDataNode(self, nodes):
        print "Querying  Data Node metrics ..."
        logger.info('[%s] : [INFO] Querying  Data Node metrics ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        for node in nodes:
            datanode, datanode_file = self.qConstructor.datanodeString(node)
            qdatanode = self.qConstructor.datanodeMetricsQuery(datanode, self.tfrom, self.to, self.qsize,
                                                               self.qinterval)
            gdatanode = self.dmonConnector.aggQuery(qdatanode)
            if gdatanode['aggregations'].values()[0].values()[0]:
                self.dformat.dict2csv(gdatanode, qdatanode, datanode_file)
            else:
                logger.info('[%s] : [INFO] Empty response from  %s no datanode metrics!',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)
        print "Querying  Data Node metrics complete"
        logger.info('[%s] : [INFO] Querying  Data Node metrics complete',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))


    def printTest(self):
        print "Endpoint -> %s" %self.esendpoint
        print "Method settings -> %s" %self.methodSettings




