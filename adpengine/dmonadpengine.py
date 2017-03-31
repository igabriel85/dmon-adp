from dmonconnector import *
from dmonpoint import *
from util import queryParser, nodesParse, str2Bool, cfilterparse, rfilterparse, pointThraesholds, parseDelay, parseMethodSettings, ut2hum
from threadRun import AdpDetectThread, AdpPointThread, AdpTrainThread
from dmonweka import dweka
from time import sleep
import tempfile
from dmonscikit import dmonscilearncluster as sdmon


class AdpEngine:
    def __init__(self, settingsDict, dataDir, modelsDir):
        self.esendpoint = settingsDict['esendpoint']
        self.esInstanceEndpoint = settingsDict['esInstanceEndpoint']
        self.adppoint = AdpPoint(settingsDict['esendpoint'])
        self.dmonPort = settingsDict['dmonPort']
        self.index = settingsDict['index']
        self.tfrom = int(settingsDict['from'])
        self.to = int(settingsDict['to'])
        self.query = settingsDict['query']
        self.qsize = settingsDict['qsize']
        self.nodes = nodesParse(settingsDict['nodes'])
        self.qinterval = settingsDict['qinterval']
        self.train = settingsDict['train']
        self.type = settingsDict['type']
        self.load = settingsDict['load']
        self.method = settingsDict['method']
        self.validate = settingsDict['validate']
        self.export = settingsDict['export']
        self.detect = settingsDict['detect']
        self.sload = settingsDict['sload']
        self.smemory = settingsDict['smemory']
        self.snetwork = settingsDict['snetwork']
        self.methodSettings = settingsDict['MethodSettings']
        self.resetIndex = settingsDict['resetindex']
        self.dataDir = dataDir
        self.modelsDir = modelsDir
        self.anomalyIndex = "anomalies"
        self.regnodeList = []
        self.allowedMethodsClustering = ['skm', 'em', 'dbscan', 'sdbscan', 'isoforest']
        self.allowefMethodsClassification = []  # TODO
        self.heap = settingsDict['heap']
        self.dmonConnector = Connector(self.esendpoint, dmonPort=self.dmonPort, index=self.index)
        self.qConstructor = QueryConstructor()
        self.dformat = DataFormatter(self.dataDir)
        self.dweka = dweka(self.dataDir, self.modelsDir, wHeap=self.heap)
        self.cfilter = settingsDict['cfilter']
        self.rfilter = settingsDict['rfilter']
        self.dfilter = settingsDict['dfilter']
        self.checkpoint = settingsDict['checkpoint']
        self.interval = settingsDict['interval']
        self.delay = settingsDict['delay']
        self.desiredNodesList = []
        self.sparkReturn = 0
        self.stormReturn = 0
        self.cassandraReturn = 0
        self.mongoReturn = 0
        self.yarnReturn = 0
        self.systemReturn = 0
        self.mapmetrics = 0
        self.reducemetrics = 0
        self.mrapp = 0
        self.dataNodeTraining = 0
        self.dataNodeDetecting = 0

    def initConnector(self):
        print "Establishing connection with dmon ....."
        resdmonInfo = self.dmonConnector.getDmonStatus()
        print "Connection established, status %s" %resdmonInfo
        resInfo = self.dmonConnector.info()
        print "General es dmon info -> %s" %resInfo

        interval = self.dmonConnector.getInterval()
        try:
            if int(self.qinterval[:-1]) < interval['System']:
                logger.warning('[%s] : [WARN] System Interval smaller than set interval!',
                               datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                print "Warning query interval difference detected, dmon interval is %s while adp is %s!" %(self.qinterval, interval['System'])
            else:
                print "Query interval check passed."
                logger.info('[%s] : [INFO] Query interval check passed!',
                               datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        except Exception as inst:
            logger.error('[%s] : [ERROR] System Interval not set in dmon!',
                               datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            print "System Interval not set in dmon! Exiting ..."
            sys.exit(1)

        resClusterState = self.dmonConnector.clusterHealth()
        print "ES cluster health -> %s" %resClusterState

        # print "Checking index %s state ...." %self.index
        # resGetIndex = self.dmonConnector.getIndex(self.index)
        # print "Index %s state -> %s" %(self.index, resGetIndex)

        print "Checking dmon registered nodes...."
        self.regnodeList = self.dmonConnector.getNodeList()
        print "Nodes found -> %s" %self.regnodeList
        self.desiredNodesList = self.getDesiredNodes()
        if str2Bool(self.resetIndex):
            print "Reseting index %s" %self.anomalyIndex
            self.dmonConnector.deleteIndex(self.anomalyIndex)
            logger.warning('[%s] : [WARN] Reset index %s complete',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.anomalyIndex)
            print "Reseting index %s complete" % self.anomalyIndex

    def getDesiredNodes(self):
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
                             datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(self.regnodeList),
                             str(desNodes))
                sys.exit(1)
        return desNodes

    def getData(self, detect=False):
        if detect:
            tfrom = "now-%s" %self.interval
            to = "now"
        else:
            tfrom = self.tfrom
            to = self.to
        queryd = queryParser(self.query)
        logger.info('[%s] : [INFO] Checking node list',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        checkpoint = str2Bool(self.checkpoint)
        desNodes = self.desiredNodesList

        if 'system' in queryd:
            if queryd['system'] == 0:
                print "Starting query for system metrics ...."
                lload = []
                lmemory = []
                linterface = []
                lpack = []

                for node in desNodes:
                    load, load_file = self.qConstructor.loadString(node)
                    memory, memory_file = self.qConstructor.memoryString(node)
                    interface, interface_file = self.qConstructor.interfaceString(node)
                    packet, packet_file = self.qConstructor.packetString(node)

                    # Queries
                    qload = self.qConstructor.systemLoadQuery(load, tfrom, to, self.qsize, self.qinterval)
                    qmemory = self.qConstructor.systemMemoryQuery(memory, tfrom, to, self.qsize, self.qinterval)
                    qinterface = self.qConstructor.systemInterfaceQuery(interface, tfrom, to, self.qsize, self.qinterval)
                    qpacket = self.qConstructor.systemInterfaceQuery(packet, tfrom, to, self.qsize, self.qinterval)

                    # Execute query and convert response to csv
                    qloadResponse = self.dmonConnector.aggQuery(qload)
                    gmemoryResponse = self.dmonConnector.aggQuery(qmemory)
                    ginterfaceResponse = self.dmonConnector.aggQuery(qinterface)
                    gpacketResponse = self.dmonConnector.aggQuery(qpacket)

                    if not checkpoint:
                        self.dformat.dict2csv(ginterfaceResponse, qinterface, interface_file)
                        self.dformat.dict2csv(gmemoryResponse, qmemory, memory_file)
                        self.dformat.dict2csv(qloadResponse, qload, load_file)
                        self.dformat.dict2csv(gpacketResponse, qpacket, packet_file)
                    else:
                        linterface.append(self.dformat.dict2csv(ginterfaceResponse, qinterface, interface_file, df=checkpoint))
                        lmemory.append(self.dformat.dict2csv(gmemoryResponse, qmemory, memory_file, df=checkpoint))
                        lload.append(self.dformat.dict2csv(qloadResponse, qload, load_file, df=checkpoint))
                        lpack.append(self.dformat.dict2csv(gpacketResponse, qpacket, packet_file, df=checkpoint))

                # Merge and rename by node system Files
                print "Query complete startin merge ..."
                if not checkpoint:
                    self.dformat.chainMergeSystem()
                    # Merge system metricsall
                    merged_df = self.dformat.chainMergeNR()
                    self.dformat.df2csv(merged_df, os.path.join(self.dataDir, "System.csv"))
                    self.systemReturn = 0
                else:
                    df_interface, df_load, df_memory, df_packet = self.dformat.chainMergeSystem(linterface=linterface,
                                                                                                lload=lload, lmemory=lmemory, lpack=lpack)
                    merged_df = self.dformat.chainMergeNR(interface=df_interface, memory=df_memory,
                                                          load=df_load, packets=df_packet)
                    self.systemReturn = merged_df
                print "System Metrics merge complete"
            else:
                print "Only for all system metrics available" #todo for metrics types
        if 'yarn' in queryd:
            print "Starting query for yarn metrics"
            if queryd['yarn'] == 0: # todo test if it works
                # per slave unique process name list
                nodeProcessReduce = {}
                nodeProcessMap = {}
                # list of dataframes
                lNM = []
                lNMJvm = []
                lShuffle = []
                lDataNode = []
                lmap = {}
                lreduce = {}
                for node in desNodes:
                    nodeManager, nodeManager_file = self.qConstructor.nodeManagerString(node)
                    jvmNodeManager, jvmNodeManager_file = self.qConstructor.jvmnodeManagerString(node)
                    datanode, datanode_file = self.qConstructor.datanodeString(node)
                    shuffle, shuffle_file = self.qConstructor.shuffleString(node)
                    reduce = self.qConstructor.jvmRedProcessString(node)
                    map = self.qConstructor.jvmMapProcessingString(node)

                    qnodeManager = self.qConstructor.yarnNodeManager(nodeManager, tfrom, to, self.qsize, self.qinterval)
                    qjvmNodeManager = self.qConstructor.jvmNNquery(jvmNodeManager, tfrom, to, self.qsize, self.qinterval)
                    qdatanode = self.qConstructor.datanodeMetricsQuery(datanode, tfrom, to, self.qsize, self.qinterval)
                    qshuffle = self.qConstructor.shuffleQuery(shuffle, tfrom, to, self.qsize, self.qinterval)
                    qreduce = self.qConstructor.queryByProcess(reduce, tfrom, to, 500, self.qinterval)
                    qmap = self.qConstructor.queryByProcess(map, tfrom, to, 500, self.qinterval)

                    gnodeManagerResponse = self.dmonConnector.aggQuery(qnodeManager)
                    gjvmNodeManagerResponse = self.dmonConnector.aggQuery(qjvmNodeManager)
                    gshuffleResponse = self.dmonConnector.aggQuery(qshuffle)
                    gdatanode = self.dmonConnector.aggQuery(qdatanode)
                    greduce = self.dmonConnector.aggQuery(qreduce)
                    gmap = self.dmonConnector.aggQuery(qmap)

                    if gnodeManagerResponse['aggregations'].values()[0].values()[0]:
                        if not checkpoint:
                            self.dformat.dict2csv(gnodeManagerResponse, qnodeManager, nodeManager_file)
                        else:
                            lNM.append(self.dformat.dict2csv(gnodeManagerResponse, qnodeManager, nodeManager_file, df=checkpoint))
                    else:
                        logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!',
                                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)

                    if gjvmNodeManagerResponse['aggregations'].values()[0].values()[0]:
                        if not checkpoint:
                            self.dformat.dict2csv(gjvmNodeManagerResponse, qjvmNodeManager, jvmNodeManager_file)
                        else:
                            lNMJvm.append(self.dformat.dict2csv(gjvmNodeManagerResponse, qjvmNodeManager, jvmNodeManager_file, df=checkpoint))
                    else:
                        logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!',
                                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)

                    if gshuffleResponse['aggregations'].values()[0].values()[0]:
                        if not checkpoint:
                            self.dformat.dict2csv(gshuffleResponse, qshuffle, shuffle_file)
                        else:
                            lShuffle.append(self.dformat.dict2csv(gshuffleResponse, qshuffle, shuffle_file, df=checkpoint))
                    else:
                        logger.info('[%s] : [INFO] Empty response from  %s no shuffle metrics!',
                                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)

                    if gdatanode['aggregations'].values()[0].values()[0]:
                        if not checkpoint:
                            self.dformat.dict2csv(gdatanode, qdatanode, datanode_file)
                        else:
                            lDataNode.append(self.dformat.dict2csv(gdatanode, qdatanode, datanode_file, df=checkpoint))
                    else:
                        logger.info('[%s] : [INFO] Empty response from  %s no datanode metrics!',
                                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)

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
                            qhreduce = self.qConstructor.jvmNNquery(hreduce, tfrom, to, self.qsize, self.qinterval)
                            ghreduce = self.dmonConnector.aggQuery(qhreduce)
                            if not checkpoint:
                                self.dformat.dict2csv(ghreduce, qhreduce, hreduce_file)
                            else:
                                # lreduce.append(self.dformat.dict2csv(ghreduce, qhreduce, hreduce_file, df=checkpoint))
                                lreduce[process] = self.dformat.dict2csv(ghreduce, qhreduce, hreduce_file, df=checkpoint)
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
                            qhmap = self.qConstructor.jvmNNquery(hmap, tfrom, to, self.qsize, self.qinterval)
                            ghmap = self.dmonConnector.aggQuery(qhmap)
                            if not checkpoint:
                                self.dformat.dict2csv(ghmap, qhmap, hmap_file)
                            else:
                                # lmap.append(self.dformat.dict2csv(ghmap, qhmap, hmap_file, df=checkpoint))
                                lmap[process] = self.dformat.dict2csv(ghmap, qhmap, hmap_file, df=checkpoint)
                    else:
                        logger.info('[%s] : [INFO] No map process for host  %s found',
                                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), host)
                        pass

                        # Get non host based metrics queries and file strings
                dfs, dfs_file = self.qConstructor.dfsString()
                dfsFs, dfsFs_file = self.qConstructor.dfsFString()
                jvmNameNodeString, jvmNameNode_file = self.qConstructor.jvmNameNodeString()
                queue, queue_file = self.qConstructor.queueResourceString()
                cluster, cluster_file = self.qConstructor.clusterMetricsSring()
                jvmResMng, jvmResMng_file = self.qConstructor.jvmResourceManagerString()
                mrapp, mrapp_file = self.qConstructor.mrappmasterString()  #todo
                jvmMrapp, jvmMrapp_file = self.qConstructor.jvmMrappmasterString()
                fsop, fsop_file = self.qConstructor.fsopDurationsString()

                # Queries
                qdfs = self.qConstructor.dfsQuery(dfs, tfrom, to, self.qsize, self.qinterval)
                qdfsFs = self.qConstructor.dfsFSQuery(dfsFs, tfrom, to, self.qsize, self.qinterval)
                qjvmNameNode = self.qConstructor.jvmNNquery(jvmNameNodeString, tfrom, to, self.qsize, self.qinterval)
                qqueue = self.qConstructor.resourceQueueQuery(queue, tfrom, to, self.qsize, self.qinterval)
                qcluster = self.qConstructor.clusterMetricsQuery(cluster, tfrom, to, self.qsize, self.qinterval)
                qjvmResMng = self.qConstructor.jvmNNquery(jvmResMng, tfrom, to, self.qsize, self.qinterval)
                qjvmMrapp = self.qConstructor.jvmNNquery(jvmMrapp, tfrom, to, self.qsize, self.qinterval)
                qfsop = self.qConstructor.fsopDurationsQuery(fsop, tfrom, to, self.qsize, self.qinterval)


                # Responses
                gdfs = self.dmonConnector.aggQuery(qdfs)
                gdfsFs = self.dmonConnector.aggQuery(qdfsFs)
                gjvmNameNode = self.dmonConnector.aggQuery(qjvmNameNode)
                gqueue = self.dmonConnector.aggQuery(qqueue)
                gcluster = self.dmonConnector.aggQuery(qcluster)
                gjvmResourceManager = self.dmonConnector.aggQuery(qjvmResMng)
                gjvmMrapp = self.dmonConnector.aggQuery(qjvmMrapp)
                gfsop = self.dmonConnector.aggQuery(qfsop)

                if not checkpoint:
                    self.dformat.dict2csv(gdfs, qdfs, dfs_file)
                    self.dformat.dict2csv(gdfsFs, qdfsFs, dfsFs_file)
                    self.dformat.dict2csv(gjvmNameNode, qjvmNameNode, jvmNameNode_file)
                    self.dformat.dict2csv(gqueue, qqueue, queue_file)
                    self.dformat.dict2csv(gcluster, qcluster, cluster_file)
                    self.dformat.dict2csv(gjvmResourceManager, qjvmResMng, jvmResMng_file)
                    self.dformat.dict2csv(gjvmMrapp, qjvmMrapp, jvmMrapp_file)
                    self.dformat.dict2csv(gfsop, qfsop, fsop_file)

                    print "Query for yarn metrics complete starting merge..."
                    merged_DFS = self.dformat.chainMergeDFS()
                    self.dformat.df2csv(merged_DFS, os.path.join(self.dataDir, 'DFS_Merged.csv'))

                    merged_cluster = self.dformat.chainMergeCluster()
                    self.dformat.df2csv(merged_cluster, os.path.join(self.dataDir, 'Cluster_Merged.csv'))

                    nm_merged, jvmnn_merged = self.dformat.chainMergeNM()
                    self.dformat.df2csv(nm_merged, os.path.join(self.dataDir, 'NM_Merged.csv'))
                    self.dformat.df2csv(jvmnn_merged, os.path.join(self.dataDir, 'JVM_NM_Merged.csv'))

                    dn_merged = self.dformat.chainMergeDN()
                    self.dformat.df2csv(dn_merged, os.path.join(self.dataDir, 'DN_Merged.csv'))

                    final_merge = self.dformat.mergeFinal()
                    self.dformat.df2csv(final_merge, os.path.join(self.dataDir, 'Final_Merge.csv'))
                    logger.info('[%s] : [INFO] Yarn metrics merge complete',
                                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                    print "Yarn metrics merge complete"
                    self.yarnReturn = 0
                else:
                   df_dfs = self.dformat.dict2csv(gdfs, qdfs, dfs_file, df=checkpoint)
                   df_dfsFs = self.dformat.dict2csv(gdfsFs, qdfsFs, dfsFs_file, df=checkpoint)
                   df_queue = self.dformat.dict2csv(gqueue, qqueue, queue_file, df=checkpoint)
                   df_cluster = self.dformat.dict2csv(gcluster, qcluster, cluster_file, df=checkpoint)
                   df_jvmResourceManager = self.dformat.dict2csv(gjvmResourceManager, qjvmResMng, jvmResMng_file, df=checkpoint)
                   df_jvmMrapp = self.dformat.dict2csv(gjvmMrapp, qjvmMrapp, jvmMrapp_file, df=checkpoint)
                   df_fsop = self.dformat.dict2csv(gfsop, qfsop, fsop_file, df=checkpoint)

                   merged_DFS = self.dformat.chainMergeDFS(dfs=df_dfs, dfsfs=df_dfsFs, fsop=df_fsop)
                   merged_cluster = self.dformat.chainMergeCluster(clusterMetrics=df_cluster, queue=df_queue,
                                                                   jvmRM=df_jvmResourceManager)
                   merge_nodemanager, jvmNode_manager, mShuffle= self.dformat.chainMergeNM(lNM=lNM, lNMJvm=lNMJvm, lShuffle=lShuffle)
                   datanode_merge = self.dformat.chainMergeDN(lDN=lDataNode)
                   df_jvmNameNode = self.dformat.dict2csv(gjvmNameNode, qjvmNameNode, jvmNameNode_file, df=checkpoint)
                   final_merge = self.dformat.mergeFinal(dfs=merged_DFS, cluster=merged_cluster, nodeMng=merge_nodemanager,
                                                         jvmnodeMng=jvmNode_manager, dataNode=datanode_merge,
                                                         jvmNameNode=df_jvmNameNode, shuffle=mShuffle,
                                                         system=self.systemReturn)

                   self.yarnReturn = final_merge
                logger.info('[%s] : [INFO] Yarn metrics merge complete',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                print "Yarn metrics merge complete"
                self.yarnReturn = final_merge
                self.mapmetrics = lmap
                self.reducemetrics = lreduce
                self.mrapp = df_jvmMrapp
            else:
                # cluster, nn, nm, dfs, dn, mr
                mCluster = mNameNode = mNodeManager = mNodeManagerJVM = mShuffle = mDFS = mDataNode = mMap = mReduce = 0
                for el in queryd['yarn']:
                    if el == 'cluster':
                        mCluster = self.getCluster(detect=detect)
                    if el == 'nn':
                        mNameNode = self.getNameNode(detect=detect)
                    if el == 'nm':
                        mNodeManager, mNodeManagerJVM, mShuffle = self.getNodeManager(desNodes, detect=detect)
                    if el == 'dfs':
                        mDFS = self.getDFS(detect=detect)
                    if el == 'dn':
                        mDataNode = self.getDataNode(desNodes, detect=detect)
                    if el == 'mr':
                       mMap, mReduce, mMRApp = self.getMapnReduce(desNodes, detect=detect)
                    if el not in ['cluster', 'nn', 'nm', 'dfs', 'dn', 'mr']:
                        logger.error('[%s] : [ERROR] Unknown metrics context %s',
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), el)
                        sys.exit(1)
            if not checkpoint:
                final_merge = self.dformat.mergeFinal()
                self.dformat.df2csv(final_merge, os.path.join(self.dataDir, 'Final_Merge.csv'))
                self.yarnReturn = 0
            else:
                final_merge = self.dformat.mergeFinal(dfs=mDFS, cluster=mCluster, nodeMng=mNodeManager,
                                                      jvmnodeMng=mNodeManagerJVM, dataNode=mDataNode,
                                                      jvmNameNode=mNameNode, shuffle=mShuffle, system=self.systemReturn)
                self.yarnReturn = final_merge
                self.reducemetrics = mReduce
                self.mapmetrics = mMap
                self.mrapp = mMRApp
                self.dformat.df2csv(final_merge, os.path.join(self.dataDir, 'cTest.csv'))
            print "Finished query and merge for yarn metrics"

        elif 'spark' in queryd:
            print "Spark metrics" #todo
            self.sparkReturn = 0
        elif 'storm' in queryd:
            print "Starting query for storm metrics"
            stormTopology = self.dmonConnector.getStormTopology()
            try:
                bolts = stormTopology['bolts']
                spouts = stormTopology['spouts']
                topology = stormTopology['Topology']
            except Exception as inst:
                logger.error('[%s] : [ERROR] No Storm topology found with %s',
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(stormTopology))
                sys.exit(1)
            storm, storm_file = self.qConstructor.stormString()
            qstorm = self.qConstructor.stormQuery(storm, tfrom, to, self.qsize, self.qinterval, bolts=bolts, spouts=spouts)
            gstorm = self.dmonConnector.aggQuery(qstorm)
            if not checkpoint:
                self.dformat.dict2csv(gstorm, qstorm, storm_file)
                self.stormReturn = 0
            else:
                df_storm = self.dformat.dict2csv(gstorm, qstorm, storm_file, df=checkpoint)
                self.stormReturn = df_storm
        elif 'cassandra' in queryd:
            # desNodes = ['cassandra-1']  #REMOVE only for casasndra testing
            self.cassandraReturn = self.getCassandra(desNodes)
        elif 'mongodb' in queryd:
            self.mongoReturn = self.getMongodb(desNodes)
        return self.systemReturn, self.yarnReturn, self.reducemetrics, self.mapmetrics, self.mrapp, self.sparkReturn, self.stormReturn, self.cassandraReturn, self.mongoReturn

    def filterData(self, df, m=False):
        '''
        :param df: -> dataframe to be filtered
        :param m: -> modify df in place or copy
        :return: ->  filtred df
        '''
        if self.cfilter is None:
            print "Column filter not set skipping ..."
            logger.info('[%s] : [INFO] Column filter not set skipping',
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        else:
            if not cfilterparse(self.cfilter):
                print "Column filter is empty skipping ..."
                logger.warning('[%s] : [WARN] Column filter is empty skipping',
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            else:
                print "Column filter is set to %s filtering ..." %cfilterparse(self.cfilter)
                logger.info('[%s] : [INFO] Column filter is set to %s filtering ...',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.cfilter)
                df = self.dformat.filterColumns(df, cfilterparse(self.cfilter))
        if self.rfilter is None:
            print "Row filter not set skipping ..."
            logger.info('[%s] : [INFO] Row filter not set skipping',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        else:
            ld, gd = rfilterparse(self.rfilter)
            if ld == 0 and gd == 0:
                print "Both ld and gd are set to zero skipping row filter ..."
                logger.info('[%s] : [INFO] Both ld and gd are set to zero skipping row filter',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            else:
                print "Row filter is set to gd->%s and ld->%s filtering ..." % (ld, gd)
                logger.info('[%s] : [INFO] Row filter is set to gd->%s and ld->%s filtering',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), ld, gd)
                df = self.dformat.filterRows(df, int(ld), int(gd))
        if self.dfilter is None:
            print"Drop columns not set skipping ..."
            logger.info('[%s] : [INFO] Drop columns not set skipping',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        else:
            if not cfilterparse(self.dfilter):
                print "Drop column filter is empty skipping ..."
                logger.warning('[%s] : [WARN] Drop column filter is empty skipping',
                               datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            else:
                print "Drop column filter is set to %s filtering ..." %cfilterparse(self.dfilter)
                if m:
                    self.dformat.dropColumns(df, cfilterparse(self.dfilter), cp=False)
                else:
                    df = self.dformat.dropColumns(df, cfilterparse(self.dfilter))
        # self.dformat.fillMissing(df)
        return df

    def trainMethod(self):
        if str2Bool(self.train):
            print "Getting data ..."
            checkpoint = str2Bool(self.checkpoint)
            queryd = queryParser(self.query)
            systemReturn, yarnReturn, reducemetrics, mapmetrics, mrapp, sparkReturn, stormReturn, cassandraReturn, mongoReturn = self.getData()
            if not checkpoint:
                if 'yarn' in queryd:
                    udata = self.dformat.toDF(os.path.join(self.dataDir, 'Final_Merge.csv'))
                elif 'storm' in queryd:
                    udata = self.dformat.toDF(os.path.join(self.dataDir, 'Storm.csv'))
                elif 'cassandra' in queryd:
                    udata = self.dformat.toDF(os.path.join(self.dataDir, 'Merged_Cassandra.csv'))
                elif 'mongodb' in queryd:
                    udata = self.dformat.toDF(os.path.join(self.dataDir, 'Merged_Mongo.csv'))
                elif 'spark' in queryd:
                    return "not yet implemented"  # todo
            else:
                if 'yarn' in queryd:
                    udata = yarnReturn
                elif 'storm' in queryd:
                    udata = stormReturn #todo important implement storm, spark, cassandra and mongodb switching
                elif 'cassandra' in queryd:
                    udata = cassandraReturn
                elif 'mongodb' in queryd:
                    udata = mongoReturn
                elif 'spark' in queryd:
                    return "not yet implemented"  # todo
            udata = self.filterData(udata) #todo check
            if self.type == 'clustering':
                if self.method in self.allowedMethodsClustering:
                    print "Training with selected method %s of type %s" % (self.method, self.type)
                    if checkpoint:
                        dfcomp = ['sdbscan', 'isoforest'] # TODO expand to all dataframe supporting methods
                        if self.method not in dfcomp:
                            dataf = tempfile.NamedTemporaryFile(suffix='.csv')
                            self.dformat.df2csv(udata, dataf.name)
                            data = dataf.name
                    else:
                        if 'yarn' in queryd:
                            dataf = os.path.join(self.dataDir, 'Final_Merge.csv')
                            if not os.path.isfile(dataf):
                                print "File %s does not exist, cannot load data! Exiting ..." % str(dataf)
                                logger.error('[%s] : [ERROR] File %s does not exist',
                                             datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                                             str(dataf))
                                sys.exit(1)
                        elif 'storm' in queryd:
                            dataf = os.path.join(self.dataDir, 'Storm.csv')
                        elif 'cassandra' in queryd:
                            dataf = os.path.join(self.dataDir, 'Merged_Cassandra.csv')
                        elif 'mongodb' in queryd:
                            dataf = os.path.join(self.dataDir, 'Merged_Mongo.csv')
                        data = dataf
                    if self.method == 'skm':
                        print "Method %s settings detected -> %s" % (self.method, str(self.methodSettings))
                        opt = parseMethodSettings(self.methodSettings)
                        if not opt:
                            opt = ['-S', '10', '-N', '10']
                        try:
                            self.dweka.simpleKMeansTrain(dataf=data, options=opt, mname=self.export)
                        except Exception as inst:
                            print "Unable to run training for method %s exited with %s and %s" % (self.method, type(inst), inst.args)
                            logger.error('[%s] : [ERROR] Unable to run training for method %s exited with %s and %s',
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.method, type(inst), inst.args)
                            sys.exit(1)
                        print "Saving model with name %s" % self.modelName(self.method, self.export)
                    elif self.method == 'em':
                        print "Method %s settings detected -> %s" % (self.method, str(self.methodSettings))
                        opt = parseMethodSettings(self.methodSettings)
                        if not opt:
                            opt = ["-I", "1000", "-N", "6",  "-M", "1.0E-6", "-num-slots", "1", "-S", "100"]
                        try:
                            self.dweka.emTrain(dataf=data, options=opt, mname=self.export)
                        except Exception as inst:
                            print "Unable to run training for method %s exited with %s and %s" % (
                            self.method, type(inst), inst.args)
                            logger.error('[%s] : [ERROR] Unable to run training for method %s exited with %s and %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.method,
                                         type(inst), inst.args)
                            sys.exit(1)
                        print "Saving model with name %s" % self.modelName(self.method, self.export)
                    elif self.method == 'dbscan':
                        print "Method %s settings detected -> %s" % (self.method, str(self.methodSettings))
                        opt = parseMethodSettings(self.methodSettings)
                        if not opt:
                            opt = ["-E",  "0.9",  "-M", "6",  "-D", "weka.clusterers.forOPTICSAndDBScan.DataObjects.EuclideanDataObject"]
                        try:
                            self.dweka.dbscanTrain(dataf=data, options=opt, mname=self.export)
                        except Exception as inst:
                            print "Unable to run training for method %s exited with %s and %s" % (
                                self.method, type(inst), inst.args)
                            logger.error('[%s] : [ERROR] Unable to run training for method %s exited with %s and %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.method,
                                         type(inst), inst.args)
                            sys.exit(1)
                        print "Saving model with name %s" % self.modelName(self.method, self.export)
                    elif self.method == 'sdbscan':
                        opt = self.methodSettings
                        if not opt or 'leaf_size' not in opt:
                            opt = {'eps': 0.9, 'min_samples': 10, 'metric': 'euclidean',
                                   'algorithm': 'auto', 'leaf_size': 30, 'p': 0.2, 'n_jobs': 1}
                        print "Using settings for sdbscan -> %s" % str(opt)
                        logger.info('[%s] : [INFO] Using settings for sdbscan -> %s ',
                                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(opt))
                        db = sdmon.SciCluster(self.modelsDir)
                        dbmodel = db.sdbscanTrain(settings=opt, mname=self.export, data=udata)
                    elif self.method == 'isoforest':
                        opt = self.methodSettings
                        if not opt or 'contamination' not in opt:
                            opt = {'n_estimators': 100, 'max_samples': 100, 'contamination': 0.01, 'bootstrap': False, 'max_features': 1.0, 'n_jobs': -1, 'random_state': None, 'verbose': 0}
                        print "Using settings for isoForest -> %s" % str(opt)
                        logger.info('[%s] : [INFO] Using settings for isoForest -> %s ',
                                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(opt))
                        isofrst = sdmon.SciCluster(self.modelsDir)
                        isofrstmodel = isofrst.isolationForest(settings=opt, mname=self.export, data=udata)
                    # Once training finished set training to false
                    self.train = False
                    return self.modelName(self.method, self.export)
                else:
                    logger.error('[%s] : [ERROR] Unknown method %s of type %s ',
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.method, self.type)
                    print "Unknown method %s of type %s" %(self.method, self.type)
                    sys.exit(1)
            elif self.type == 'classification':
                print "Not yet supported!"  # TODO
                sys.exit(0)
            else:
                logger.error('[%s] : [ERROR] Unknown type %s ',
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.type)
                sys.exit(1)
        else:
            print "Training is set to false, skipping..."
            logger.warning('[%s] : [WARN] Training is set to false, skipping...',
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            return 0

    def detectPointAnomalies(self):
        loadth = pointThraesholds(self.sload)
        if not loadth:
            loadth = {'shortterm': {'threashold': '4.5', 'bound': 'gd'},
                      'longterm': {'threashold': '3.0', 'bound': 'gd'}, 'midterm': {'threashold': '3.5', 'bound': 'gd'}}
            logger.warning('[%s] : [WARN] Using default values for point anomaly load',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        networkth = pointThraesholds(self.snetwork)
        if not loadth:
            networkth = {'rx': {'threashold': '1000000000', 'bound': 'gd'},
                         'tx': {'threashold': '1000000000', 'bound': 'gd'}}
            logger.warning('[%s] : [WARN] Using default values for point anomaly network',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        memoryth = pointThraesholds(self.smemory)
        if not memoryth:
            memoryth = {'cached': {'threashold': '231313', 'bound': 'gd'},
                        'buffered': {'threashold': '200000000', 'bound': 'gd'},
                        'used': {'threashold': '1000000000', 'bound': 'gd'},
                        'free': {'threashold': '100000000', 'bound': 'ld'}}
            logger.warning('[%s] : [WARN] Using default values for point anomaly memory',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        all = [loadth, networkth, memoryth]
        while True:
            lload = []
            lmemory = []
            linterface = []
            lpack = []
            tfrom = "now-30s" # todo compute based on delay
            to = "now"
            for node in self.desiredNodesList:
                load, load_file = self.qConstructor.loadString(node)
                memory, memory_file = self.qConstructor.memoryString(node)
                interface, interface_file = self.qConstructor.interfaceString(node)
                packet, packet_file = self.qConstructor.packetString(node)

                # Queries
                qload = self.qConstructor.systemLoadQuery(load, tfrom, to, self.qsize, self.qinterval)
                qmemory = self.qConstructor.systemMemoryQuery(memory, tfrom, to, self.qsize, self.qinterval)
                qinterface = self.qConstructor.systemInterfaceQuery(interface, tfrom, to, self.qsize,
                                                                    self.qinterval)
                qpacket = self.qConstructor.systemInterfaceQuery(packet, tfrom, to, self.qsize, self.qinterval)

                # Execute query and convert response to csv
                qloadResponse = self.dmonConnector.aggQuery(qload)
                gmemoryResponse = self.dmonConnector.aggQuery(qmemory)
                ginterfaceResponse = self.dmonConnector.aggQuery(qinterface)
                gpacketResponse = self.dmonConnector.aggQuery(qpacket)

                linterface.append(self.dformat.dict2csv(ginterfaceResponse, qinterface, interface_file, df=True))
                lmemory.append(self.dformat.dict2csv(gmemoryResponse, qmemory, memory_file, df=True))
                lload.append(self.dformat.dict2csv(qloadResponse, qload, load_file, df=True))
                lpack.append(self.dformat.dict2csv(gpacketResponse, qpacket, packet_file, df=True))

                df_interface, df_load, df_memory, df_packet = self.dformat.chainMergeSystem(linterface=linterface,
                                                                                            lload=lload, lmemory=lmemory,
                                                                                            lpack=lpack)
                df_system = self.dformat.chainMergeNR(interface=df_interface, memory=df_memory,
                                                      load=df_load, packets=df_packet)
                dict_system = self.dformat.df2dict(df_system)
                # print dict_system
                for th in all:
                    for type, val in th.iteritems():
                        responseD = {}
                        if val['bound'] == 'gd':
                            anomalies = self.adppoint.detpoint(dict_system, type=type, threashold=val['threashold'], lt=False)
                            if anomalies:
                                responseD['anomalies'] = anomalies
                                # self.reportAnomaly(responseD)
                            else:
                                logger.info('[%s] : [INFO] No point anomalies detected for type %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type)
                                print "No point anomalies detected for %s " % type
                        else:
                            anomalies = self.adppoint.detpoint(dict_system, type=type, threashold=val['threashold'], lt=True)
                            if anomalies:
                                responseD['anomalies'] = anomalies
                                # self.reportAnomaly(responseD)
                            else:
                                logger.info('[%s] : [INFO] No point anomalies detected for type %s ',
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type)
                                print "No point anomalies detected for type %s" % type
                    if responseD:
                        self.reportAnomaly(responseD)
                    else:
                        logger.info('[%s] : [INFO] No point anomalies detected',
                                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                    sleep(parseDelay(self.delay))

    def detectAnomalies(self):
        if str2Bool(self.detect):
            checkpoint = str2Bool(self.checkpoint)
            queryd = queryParser(self.query)
            logger.info('[%s] : [INFO] Detection query set as %s ',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(queryd))
            if self.type == 'clustering':
                while True:
                    print "Collect data ..."
                    systemReturn, yarnReturn, reducemetrics, mapmetrics, mrapp, sparkReturn, stormReturn, cassandraReturn, mongoReturn = self.getData(detect=True)
                    # if list(set(self.dformat.fmHead) - set(list(yarnReturn.columns.values))):
                    #     print "Mismatch between desired and loaded data"
                    #     sys.exit()
                    # if self.dataNodeTraining != self.dataNodeDetecting:
                    #     print "Detected datanode discrepancies; training %s, detecting %s" %(self.dataNodeTraining, self.dataNodeDetecting)
                    #     logger.error('[%s] : [ERROR]Detected datanode discrepancies; training %s, detecting %s',
                    #          datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.dataNodeTraining, self.dataNodeDetecting)
                    #     sys.exit(1)

                    if 'yarn' in queryd:
                        yarnReturn = self.filterData(yarnReturn) #todo
                        if checkpoint:
                            dataf = tempfile.NamedTemporaryFile(suffix='.csv')
                            self.dformat.df2csv(yarnReturn, dataf.name)
                            data = dataf.name
                        else:
                            dataf = os.path.join(self.dataDir, 'Final_Merge.csv')
                            data = dataf
                    elif 'storm' in queryd:
                        stormReturn = self.filterData(stormReturn)
                        if checkpoint:
                            data = stormReturn
                        else:
                            dataf = os.path.join(self.dataDir, 'Storm.csv')
                            data = dataf

                    if self.method in self.allowedMethodsClustering:
                        print "Detecting with selected method %s of type %s" % (self.method, self.type)
                        if os.path.isfile(os.path.join(self.modelsDir, self.modelName(self.method, self.load))):
                            print "Model found at %s" % str(
                                os.path.join(self.modelsDir, self.modelName(self.method, self.load)))
                            wekaList = ['skm', 'em', 'dbscan']
                            if self.method in wekaList:
                                anomalies = self.dweka.runclustermodel(self.method, self.load, data)
                                # print ut2hum(e)
                                a = {"method": self.method, "qinterval": self.qinterval, "anomalies": anomalies}
                                self.reportAnomaly(a)
                                sleep(parseDelay(self.delay))
                            else:
                                smodel = sdmon.SciCluster(modelDir=self.modelsDir)
                                anomalies = smodel.detect(self.method, self.load, data)
                                if not anomalies['anomalies']:
                                    logger.info('[%s] : [INFO] No anomalies detected with IsolationForest', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                                    print "No anomalies detected with IsolationForest"
                                    sleep(parseDelay(self.delay))
                                else:
                                    anomalies['method'] = self.method
                                    anomalies['qinterval'] = self.qinterval
                                    self.reportAnomaly(anomalies)
                                    sleep(parseDelay(self.delay))
                        else:
                            logger.error('[%s] : [ERROR] Model %s not found at %s ',
                             datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.load,
                                     str(os.path.join(self.modelsDir, self.modelName(self.method, self.load))))
                            print "Model not found %s" % self.modelName(self.method, self.load)
                            sys.exit(1)
                    else:
                        print "Unknown method %s of type %s" % (self.method, self.type)
                        logger.error('[%s] : [ERROR] Unknown method %s of type %s ',
                                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.method,
                                 self.type)
                        sys.exit(1)
            elif self.type == 'classification':
                print "Not yet supported!"  # TODO
                sys.exit(0)
            else:
                logger.error('[%s] : [ERROR] Unknown type %s ',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.type)
                sys.exit(1)
        else:
            print "Detect is set to false, skipping..."
            logger.warning('[%s] : [WARN] Detect is set to false, skipping...',
                       datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

    def run(self, engine):
        try:
            threadPoint = AdpPointThread(engine, 'Point')
            threadTrain = AdpTrainThread(engine, 'Train')
            threadDetect = AdpDetectThread(engine, 'Detect')

            threadPoint.start()
            threadTrain.start()
            threadDetect.start()

            threadPoint.join()
            threadTrain.join()
            threadDetect.join()
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception %s with %s during point thread execution, halting',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            sys.exit(1)
        return 0

    def modelName(self, methodname, modelName):
        '''
        :param methodname: -> name of current method (self.method)
        :param modelName: ->name of current export (self.export)
        :return:
        '''
        saveName = "%s_%s.model" %(methodname, modelName)
        if not os.path.isfile(os.path.join(self.modelsDir,saveName)):
            saveName = "%s_%s.pkl" %(methodname, modelName)
        return saveName

    def pushModel(self):
        return "model"

    def compareModel(self):
        return "Compare models"

    def reportAnomaly(self, body):
        now = datetime.utcnow()
        itime = now.strftime("%Y-%m-%dT%H:%M:%S") + ".%03d" % (now.microsecond / 1000) + "Z"
        body["timestamp"] = itime
        self.dmonConnector.pushAnomaly(anomalyIndex=self.anomalyIndex, doc_type='anomaly', body=body)

    def getDFS(self, detect=False):
        # Query Strings
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = self.tfrom
            to = self.to
        print "Querying DFS metrics"
        logger.info('[%s] : [INFO] Querying DFS metrics...',
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        checkpoint = str2Bool(self.checkpoint)
        dfs, dfs_file = self.qConstructor.dfsString()
        dfsFs, dfsFs_file = self.qConstructor.dfsFString()
        fsop, fsop_file = self.qConstructor.fsopDurationsString()

        # Query constructor
        qdfs = self.qConstructor.dfsQuery(dfs, tfrom, to, self.qsize, self.qinterval)
        qdfsFs = self.qConstructor.dfsFSQuery(dfsFs, tfrom, to, self.qsize, self.qinterval)
        qfsop = self.qConstructor.fsopDurationsQuery(fsop, tfrom, to, self.qsize, self.qinterval)

        # Execute query
        gdfs = self.dmonConnector.aggQuery(qdfs)
        gdfsFs = self.dmonConnector.aggQuery(qdfsFs)
        gfsop = self.dmonConnector.aggQuery(qfsop)

        if not checkpoint:
            self.dformat.dict2csv(gdfs, qdfs, dfs_file)
            self.dformat.dict2csv(gdfsFs, qdfsFs, dfsFs_file)
            self.dformat.dict2csv(gfsop, qfsop, fsop_file)
        else:
            df_dfs = self.dformat.dict2csv(gdfs, qdfs, dfs_file, df=checkpoint)
            df_dfsFs = self.dformat.dict2csv(gdfsFs, qdfsFs, dfsFs_file, df=checkpoint)
            df_fsop = self.dformat.dict2csv(gfsop, qfsop, fsop_file, df=checkpoint)

        print "Querying DFS metrics complete."
        logger.info('[%s] : [INFO] Querying DFS metrics complete.',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        print "Starting DFS merge ..."
        if not checkpoint:
            merged_DFS = self.dformat.chainMergeDFS()
            self.dformat.df2csv(merged_DFS, os.path.join(self.dataDir, 'Merged_DFS.csv'))
            logger.info('[%s] : [INFO] DFS merge complete',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            print "DFS merge complete."
            return 0
        else:
            merged_DFS = self.dformat.chainMergeDFS(dfs=df_dfs, dfsfs=df_dfsFs, fsop=df_fsop)
            logger.info('[%s] : [INFO] DFS merge complete',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            print "DFS merge complete."
            return merged_DFS

    def getNodeManager(self, nodes, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = self.tfrom
            to = self.to
        print "Querying  Node Manager and Shuffle metrics ..."
        logger.info('[%s] : [INFO] Querying  Node Manager and Shuffle metrics...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        checkpoint = str2Bool(self.checkpoint)
        lNM = []
        ljvmNM = []
        lShuffle = []
        for node in nodes:
            nodeManager, nodeManager_file = self.qConstructor.nodeManagerString(node)
            jvmNodeManager, jvmNodeManager_file = self.qConstructor.jvmnodeManagerString(node)
            shuffle, shuffle_file = self.qConstructor.shuffleString(node)

            qnodeManager = self.qConstructor.yarnNodeManager(nodeManager, tfrom, to, self.qsize,
                                                             self.qinterval)
            qjvmNodeManager = self.qConstructor.jvmNNquery(jvmNodeManager, tfrom, to, self.qsize,
                                                           self.qinterval)
            qshuffle = self.qConstructor.shuffleQuery(shuffle, tfrom, to, self.qsize, self.qinterval)

            gnodeManagerResponse = self.dmonConnector.aggQuery(qnodeManager)
            if gnodeManagerResponse['aggregations'].values()[0].values()[0]:
                if not checkpoint:
                    self.dformat.dict2csv(gnodeManagerResponse, qnodeManager, nodeManager_file)
                else:
                    lNM.append(self.dformat.dict2csv(gnodeManagerResponse, qnodeManager, nodeManager_file, df=checkpoint))
            else:
                logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)

            gjvmNodeManagerResponse = self.dmonConnector.aggQuery(qjvmNodeManager)
            if gjvmNodeManagerResponse['aggregations'].values()[0].values()[0]:
                if not checkpoint:
                    self.dformat.dict2csv(gjvmNodeManagerResponse, qjvmNodeManager, jvmNodeManager_file)
                else:
                    ljvmNM.append(self.dformat.dict2csv(gjvmNodeManagerResponse, qjvmNodeManager, jvmNodeManager_file, df=checkpoint))
            else:
                logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)

            gshuffleResponse = self.dmonConnector.aggQuery(qshuffle)
            if gshuffleResponse['aggregations'].values()[0].values()[0]:
                if not checkpoint:
                    self.dformat.dict2csv(gshuffleResponse, qshuffle, shuffle_file)
                else:
                    lShuffle.append(self.dformat.dict2csv(gshuffleResponse, qshuffle, shuffle_file, df=checkpoint))
            else:
                logger.info('[%s] : [INFO] Empty response from  %s no shuffle metrics!',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)
        print "Querying  Node Manager and Shuffle metrics complete."
        logger.info('[%s] : [INFO] Querying  Node Manager and Shuffle metrics complete...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        print "Starting Node Manager merge ..."
        if not checkpoint:
            nm_merged, jvmnn_merged, shuffle_merged = self.dformat.chainMergeNM()
            self.dformat.df2csv(nm_merged, os.path.join(self.dataDir, 'Merged_NM.csv'))
            self.dformat.df2csv(jvmnn_merged, os.path.join(self.dataDir, 'Merged_JVM_NM.csv'))
            self.dformat.df2csv(shuffle_merged, os.path.join(self.dataDir, 'Merged_Shuffle.csv'))
            logger.info('[%s] : [INFO] Node Manager Merge complete',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            print "Node Manager Merge Complete"
            nm_merged = 0
            jvmnn_merged = 0
            shuffle_merged = 0
        else:
            nm_merged, jvmnn_merged, shuffle_merged = self.dformat.chainMergeNM(lNM=lNM, lNMJvm=ljvmNM, lShuffle=lShuffle)
            logger.info('[%s] : [INFO] Node Manager Merge complete',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            print "Node Manager Merge Complete"
        return nm_merged, jvmnn_merged, shuffle_merged

    def getNameNode(self, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = self.tfrom
            to = self.to
        print "Querying  Name Node metrics ..."
        logger.info('[%s] : [INFO] Querying  Name Node metrics ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        checkpoint = str2Bool(self.checkpoint)
        jvmNameNodeString, jvmNameNode_file = self.qConstructor.jvmNameNodeString()
        qjvmNameNode = self.qConstructor.jvmNNquery(jvmNameNodeString, tfrom, to, self.qsize, self.qinterval)
        gjvmNameNode = self.dmonConnector.aggQuery(qjvmNameNode)
        if not checkpoint:
            self.dformat.dict2csv(gjvmNameNode, qjvmNameNode, jvmNameNode_file)
            returnNN = 0
        else:
            df_NN = self.dformat.dict2csv(gjvmNameNode, qjvmNameNode, jvmNameNode_file, df=checkpoint)
            # df_NN.set_index('key', inplace=True)
            returnNN = df_NN
        print "Querying  Name Node metrics complete"
        logger.info('[%s] : [INFO] Querying  Name Node metrics complete',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        return returnNN

    def getCluster(self, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = self.tfrom
            to = self.to
        print "Querying  Cluster metrics ..."
        logger.info('[%s] : [INFO] Querying  Name Node metrics ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        checkpoint = str2Bool(self.checkpoint)
        queue, queue_file = self.qConstructor.queueResourceString()
        cluster, cluster_file = self.qConstructor.clusterMetricsSring()
        # jvmMrapp, jvmMrapp_file = self.qConstructor.jvmMrappmasterString()
        jvmResMng, jvmResMng_file = self.qConstructor.jvmResourceManagerString()

        # qjvmMrapp = self.qConstructor.jvmNNquery(jvmMrapp, tfrom, to, self.qsize, self.qinterval)
        qqueue = self.qConstructor.resourceQueueQuery(queue, tfrom, to, self.qsize, self.qinterval)
        qcluster = self.qConstructor.clusterMetricsQuery(cluster, tfrom, to, self.qsize, self.qinterval)
        qjvmResMng = self.qConstructor.jvmNNquery(jvmResMng, tfrom, to, self.qsize, self.qinterval)

        gqueue = self.dmonConnector.aggQuery(qqueue)
        gcluster = self.dmonConnector.aggQuery(qcluster)
        # gjvmMrapp = self.dmonConnector.aggQuery(qjvmMrapp)
        gjvmResourceManager = self.dmonConnector.aggQuery(qjvmResMng)

        if not checkpoint:
            self.dformat.dict2csv(gcluster, qcluster, cluster_file)
            self.dformat.dict2csv(gqueue, qqueue, queue_file)
            # self.dformat.dict2csv(gjvmMrapp, qjvmMrapp, jvmMrapp_file)
            self.dformat.dict2csv(gjvmResourceManager, qjvmResMng, jvmResMng_file)

            print "Starting cluster merge ..."
            merged_cluster = self.dformat.chainMergeCluster()
            self.dformat.df2csv(merged_cluster, os.path.join(self.dataDir, 'Merged_Cluster.csv'))
            clusterReturn = 0
        else:
            df_cluster = self.dformat.dict2csv(gcluster, qcluster, cluster_file, df=checkpoint)
            df_queue = self.dformat.dict2csv(gqueue, qqueue, queue_file, df=checkpoint)
            # df_jvmMrapp = self.dformat.dict2csv(gjvmMrapp, qjvmMrapp, jvmMrapp_file, df=checkpoint)
            df_jvmResourceManager = self.dformat.dict2csv(gjvmResourceManager, qjvmResMng, jvmResMng_file, df=checkpoint)
            print "Starting cluster merge ..."
            merged_cluster = self.dformat.chainMergeCluster(clusterMetrics=df_cluster, queue=df_queue,
                                                                   jvmRM=df_jvmResourceManager)
            clusterReturn = merged_cluster
        print "Querying  Cluster metrics complete"
        logger.info('[%s] : [INFO] Querying  Name Node metrics complete',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        logger.info('[%s] : [INFO] Cluster Merge complete',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        print "Cluster merge complete"
        return clusterReturn

    def getMapnReduce(self, nodes, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = self.tfrom
            to = self.to
        # per slave unique process name list
        nodeProcessReduce = {}
        nodeProcessMap = {}
        lRD = {}
        lMP = {}
        checkpoint = str2Bool(self.checkpoint)
        print "Querying  Mapper and Reducer metrics ..."
        logger.info('[%s] : [INFO] Querying  Mapper and Reducer metrics ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        for node in nodes:
            reduce = self.qConstructor.jvmRedProcessString(node)
            map = self.qConstructor.jvmMapProcessingString(node)

            qreduce = self.qConstructor.queryByProcess(reduce, tfrom, to, 500, self.qinterval)
            qmap = self.qConstructor.queryByProcess(map, tfrom, to, 500, self.qinterval)

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
                    qhreduce = self.qConstructor.jvmNNquery(hreduce, tfrom, to, self.qsize, self.qinterval)
                    ghreduce = self.dmonConnector.aggQuery(qhreduce)
                    if not checkpoint:
                        self.dformat.dict2csv(ghreduce, qhreduce, hreduce_file)
                    else:
                        # lRD.append(self.dformat.dict2csv(ghreduce, qhreduce, hreduce_file, df=checkpoint))
                        lRD[process] = self.dformat.dict2csv(ghreduce, qhreduce, hreduce_file, df=checkpoint)
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
                    qhmap = self.qConstructor.jvmNNquery(hmap, tfrom, to, self.qsize, self.qinterval)
                    ghmap = self.dmonConnector.aggQuery(qhmap)
                    if not checkpoint:
                        self.dformat.dict2csv(ghmap, qhmap, hmap_file)
                    else:
                        # lMP.append(self.dformat.dict2csv(ghmap, qhmap, hmap_file, df=checkpoint))
                        lMP[process] = self.dformat.dict2csv(ghmap, qhmap, hmap_file, df=checkpoint)
            else:
                logger.info('[%s] : [INFO] No map process for host  %s found',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), host)
                pass
        print "Querying  Mapper metrics complete"
        logger.info('[%s] : [INFO] Querying  Mapper metrics complete',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        print "Querying MRApp metrics ... "
        logger.info('[%s] : [INFO] Querying  MRApp metrics ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        jvmMrapp, jvmMrapp_file = self.qConstructor.jvmMrappmasterString()

        qjvmMrapp = self.qConstructor.jvmNNquery(jvmMrapp, tfrom, to, self.qsize, self.qinterval)
        gjvmMrapp = self.dmonConnector.aggQuery(qjvmMrapp)

        if not checkpoint:
            self.dformat.dict2csv(gjvmMrapp, qjvmMrapp, jvmMrapp_file)
            df_jvmMrapp = 0
        else:
            df_jvmMrapp = self.dformat.dict2csv(gjvmMrapp, qjvmMrapp, jvmMrapp_file, df=checkpoint)
        logger.info('[%s] : [INFO] Querying  MRApp metrics complete',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        print "Querying MRApp metrics complete "
        return lMP, lRD, df_jvmMrapp

    def getDataNode(self, nodes, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = self.tfrom
            to = self.to
        print "Querying  Data Node metrics ..."
        logger.info('[%s] : [INFO] Querying  Data Node metrics ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        checkpoint = str2Bool(self.checkpoint)
        lDN = []
        for node in nodes:
            datanode, datanode_file = self.qConstructor.datanodeString(node)
            qdatanode = self.qConstructor.datanodeMetricsQuery(datanode, tfrom, to, self.qsize,
                                                               self.qinterval)
            gdatanode = self.dmonConnector.aggQuery(qdatanode)
            if gdatanode['aggregations'].values()[0].values()[0]:
                if not checkpoint:
                    self.dformat.dict2csv(gdatanode, qdatanode, datanode_file)
                else:
                    lDN.append(self.dformat.dict2csv(gdatanode, qdatanode, datanode_file, df=checkpoint))
            else:
                logger.info('[%s] : [INFO] Empty response from  %s no datanode metrics!',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), node)
        print "Querying  Data Node metrics complete"
        if detect:
            self.dataNodeDetecting = len(lDN)
        else:
            self.dataNodeTraining = len(lDN)
        logger.info('[%s] : [INFO] Querying  Data Node metrics complete',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        print "Starting Data Node metrics merge ..."
        if not checkpoint:
            dn_merged = self.dformat.chainMergeDN()
            self.dformat.df2csv(dn_merged, os.path.join(self.dataDir, 'Merged_DN.csv'))
            logger.info('[%s] : [INFO] Data Node metrics merge complete',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            print "Data Node metrics merge complete"
            return 0
        else:
            dn_merged = self.dformat.chainMergeDN(lDN=lDN)
            logger.info('[%s] : [INFO] Data Node metrics merge complete',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            print "Data Node metrics merge complete"
            return dn_merged

    def getCassandra(self, nodes, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = self.tfrom
            to = self.to
        print "Querying  Cassandra metrics ..."
        logger.info('[%s] : [INFO] Querying  Cassandra metrics ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        checkpoint = str2Bool(self.checkpoint)
        lcassandraCounter = []
        lcassandraGauge = []
        for node in nodes:
            cassandra, cassandra_file = self.qConstructor.cassandraCounterString(host=node)
            cassandragauge, cassandragauge_file = self.qConstructor.cassandraGaugeString(host=node)

            # Queries
            qcassandra = self.qConstructor.cassandraQuery(cassandra, tfrom, to, self.qsize, self.qinterval)
            qcassandragauge = self.qConstructor.cassandraQuery(cassandragauge, tfrom, to, self.qsize, self.qinterval)

            # Execute query and convert response to csv
            gcassandra = self.dmonConnector.aggQuery(qcassandra)
            gcassandragauge = self.dmonConnector.aggQuery(qcassandragauge)


            lcassandraCounter.append(self.dformat.dict2csv(gcassandra, qcassandra, cassandra_file, df=True))
            lcassandraGauge.append(
                self.dformat.dict2csv(gcassandragauge, qcassandragauge, cassandragauge_file, df=True))

            # Merge and rename by node system Files

        df_CA_Count = self.dformat.chainMergeCassandra(lcassandraCounter)
        df_CA_Gauge = self.dformat.chainMergeCassandra(lcassandraGauge)

        df_CA = self.dformat.listMerge([df_CA_Count, df_CA_Gauge])
        print "Cassandra  metrics merge complete"
        logger.info('[%s] : [INFO] Cassandra  metrics merge complete',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        if not checkpoint:
            self.dformat.df2csv(df_CA, os.path.join(self.dataDir, 'Merged_Cassandra.csv'))
            return 0
        else:
            return df_CA

    def getMongodb(self, nodes, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = self.tfrom
            to = self.to
        print "Querying Mongodb metrics ..."
        logger.info('[%s] : [INFO] Querying  MongoDB metrics ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        checkpoint = str2Bool(self.checkpoint)
        lmongoCounter = []
        lmongoGauge = []
        for node in nodes:
            mongodbCounter, mongodbCounter_file = self.qConstructor.mongodbCounterString(host=node)
            mongodbGauge, mongodbGauge_file = self.qConstructor.mongodbGaugeString(host=node)

            #Queries
            qmongodbCounter = self.qConstructor.mongoDBCounterQuery(mongodbCounter, tfrom, to, self.qsize, self.qinterval)
            qmongodbGauge = self.qConstructor.mongoDBGaugeQuery(mongodbGauge, tfrom, to, self.qsize, self.qinterval)

            # Execute query and convert response to csv
            gmongodbGauge = self.dmonConnector.aggQuery(qmongodbGauge)
            gmongodbCounter = self.dmonConnector.aggQuery(qmongodbCounter)

            lmongoCounter.append(self.dformat.dict2csv(gmongodbCounter, qmongodbCounter, mongodbCounter_file))
            lmongoGauge.append(self.dformat.dict2csv(gmongodbGauge, qmongodbGauge, mongodbGauge_file))


    #Merge and rename by node system File
        df_MD_Count = self.dformat.chainMergeMongoDB(lmongoCounter)
        df_MD_Gauge = self.dformat.chainMergeMongoDB(lmongoGauge)

        df_MD = self.dformat.listMerge([df_MD_Count, df_MD_Gauge])
        print "MongoDB metrics merged"
        logger.info('[%s] : [INFO] MongoDB  metrics merge complete',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        if not checkpoint:
            self.dformat.df2csv(df_MD, os.path.join(self.dataDir, "Merged_Mongo.csv"))
            return 0
        else:
            return df_MD


    def printTest(self):
        print "Endpoint -> %s" %self.esendpoint
        print "Method settings -> %s" %self.methodSettings
        print "Train -> %s"  % type(self.train)

    def print_time(self, threadName, delay):
        count = 0
        while count < 5:
            time.sleep(delay)
            count += 1
            print "%s: %s" % (threadName, time.ctime(time.time()))




