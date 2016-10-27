from addict import Dict


class QueryConstructor():
    def __init__(self):
        self.author = 'Constructor for dmon-adp ES connector querys'
        # self.systemLoadString = "collectd_type:\"load\" AND host:\"dice.cdh.master\""

    def loadString(self, host):
        qstring = "collectd_type:\"load\" AND host:\"%s\"" % host
        file = "Load_%s.csv" % host
        return qstring, file

    def memoryString(self, host):
        qstring = "collectd_type:\"memory\" AND host:\"%s\"" % host
        file = "Memory_%s.csv" % host
        return qstring, file

    def interfaceString(self, host):
        qstring = "plugin:\"interface\" AND collectd_type:\"if_octets\" AND host:\"%s\"" % host
        file = "Interface_%s.csv" % host
        return qstring, file

    def packetString(self, host):
        qstring = "plugin:\"interface\" AND collectd_type:\"if_packets\" AND host:\"%s\"" % host
        file = "Packets_%s.csv" % host
        return qstring, file

    def dfsString(self):
        qstring = "serviceType:\"dfs\""
        file = "DFS.csv"
        return qstring, file

    def dfsFString(self):
        qstring = "serviceType:\"dfs\" AND serviceMetrics:\"FSNamesystem\""
        file = "DFSFS.csv"
        return qstring, file

    def jvmnodeManagerString(self, host):
        qstring = "serviceType:\"jvm\" AND ProcessName:\"NodeManager\" AND hostname:\"%s\"" % host
        file = "JVM_NM_%s.csv" % host
        return qstring, file

    def jvmNameNodeString(self):
        qstring = "serviceType:\"jvm\" AND ProcessName:\"NameNode\""
        file = "JVM_NN.csv"
        return qstring, file

    def nodeManagerString(self, host):
        qstring = "serviceType:\"yarn\" AND serviceMetrics:\"NodeManagerMetrics\" AND hostname:\"%s\"" % host
        file = "NM_%s.csv" % host
        return qstring, file

    def queueResourceString(self):
        qstring = "type:\"resourcemanager-metrics\" AND serviceMetrics:\"QueueMetrics\""
        file = "ResourceManagerQueue.csv"
        return qstring, file

    def clusterMetricsSring(self):
        qstring = "type:\"resourcemanager-metrics\" AND ClusterMetrics:\"ResourceManager\""
        file = "ClusterMetrics.csv"
        return qstring, file

    def jvmMapTask(self, host):
        qstring = "hostname:\"%s\" AND type:\"maptask-metrics\"" % host  # TODO add per process name
        file = "JVM_MapTask_%s.csv" % host
        return qstring, file

    def jvmResourceManagerString(self):
        qstring = "type:\"resourcemanager-metrics\" AND serviceType:\"jvm\""
        file = "JVM_RM.csv"
        return qstring, file

    def datanodeString(self, host):
        qstring = "type:\"datanode-metrics\" AND serviceType:\"dfs\" AND hostname:\"%s\"" % host
        file = 'DN_%s.csv' % host
        return qstring, file

    def mrappmasterString(self):
        qstring = "type:\"mrappmaster-metrics\" AND serviceMetrics:\"MRAppMetrics\""
        file = 'MRAPP.csv'
        return qstring, file

    def jvmMrappmasterString(self):
        qstring = "type:\"mrappmaster-metrics\" AND serviceMetrics:\"JvmMetrics\""
        file = "JVM_MRAPP.csv"
        return qstring, file

    def fsopDurationsString(self):
        qstring = "type:\"resourcemanager-metrics\" AND serviceMetrics:\"FSOpDurations\""
        file = 'FSOP.csv'
        return qstring, file

    def shuffleString(self, host):
        qstring = "serviceMetrics:\"ShuffleMetrics\" AND serviceType:\"mapred\" AND hostname:\"%s\"" % host
        file = 'Shuffle_%s.csv' % host
        return qstring, file

    def jvmRedProcessString(self, host):
        qstring = "type:\"reducetask-metrics\" AND serviceType:\"jvm\" AND hostname:\"%s\"" % host
        return qstring

    def jvmMapProcessingString(self, host):
        qstring = "type:\"maptask-metrics\" AND serviceType:\"jvm\" AND hostname:\"%s\"" % host
        return qstring

    def jvmRedProcessbyNameString(self, host, process):
        qstring = "type:\"reducetask-metrics\" AND serviceType:\"jvm\" AND hostname:\"%s\" AND ProcessName:\"%s\"" %(host, process)
        file = 'JVM_ReduceTask_%s_%s.csv' %(host, process)
        return qstring, file

    def jvmMapProcessbyNameString(self, host, process):
        qstring = "type:\"maptask-metrics\" AND serviceType:\"jvm\" AND hostname:\"%s\" AND ProcessName:\"%s\"" % (
        host, process)
        file = 'JVM_MapTasksTask_%s_%s.csv' % (host, process)
        return qstring, file

    def loadAverage(self):  # TODO
        return "Average load across all nodes!"

    def memoryAverage(self):
        return "Average memory across all nodes!"

    def interfaceAverage(self):  # TODO
        return "Average interface across all nodes!"

    def packetAverage(self):  # TODO
        return "Average packets across all nodes!"

    def yarnNodeManager(self, qstring, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
                            qmin_doc_count=1):

        cquery = Dict()
        cquery.query.filtered.query.query_string.query = qstring
        cquery.query.filtered.query.query_string.analyze_wildcard = wildCard
        cquery.query.filtered.filter.bool.must = [
            {"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
        cquery.query.filtered.filter.bool.must_not = []
        cquery.size = qsize

        cquery.aggs["3"].date_histogram.field = "@timestamp"
        cquery.aggs["3"].date_histogram.interval = qinterval
        cquery.aggs["3"].date_histogram.time_zone = "Europe/Helsinki"
        cquery.aggs["3"].date_histogram.min_doc_count = qmin_doc_count
        cquery.aggs["3"].date_histogram.extended_bounds.min = qgte
        cquery.aggs["3"].date_histogram.extended_bounds.max = qlte

        # Specify precise metrics, and the average value expressed by 'avg' key
        cquery.aggs["3"].aggs["40"].avg.field = "ContainersLaunched"
        cquery.aggs["3"].aggs["41"].avg.field = "ContainersCompleted"
        cquery.aggs["3"].aggs["42"].avg.field = "ContainersFailed"
        cquery.aggs["3"].aggs["43"].avg.field = "ContainersKilled"
        cquery.aggs["3"].aggs["44"].avg.field = "ContainersIniting"
        cquery.aggs["3"].aggs["45"].avg.field = "ContainersRunning"
        cquery.aggs["3"].aggs["47"].avg.field = "AvailableGB"
        cquery.aggs["3"].aggs["48"].avg.field = "AllocatedContainers"
        cquery.aggs["3"].aggs["49"].avg.field = "AvailableGB"
        cquery.aggs["3"].aggs["50"].avg.field = "AllocatedVCores"
        cquery.aggs["3"].aggs["51"].avg.field = "AvailableVCores"
        cquery.aggs["3"].aggs["52"].avg.field = "ContainerLaunchDurationNumOps"
        cquery.aggs["3"].aggs["53"].avg.field = "ContainerLaunchDurationAvgTime"
        cqueryd = cquery.to_dict()
        return cqueryd

    def systemLoadQuery(self, qstring, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
                            qmin_doc_count=1):
        cquery = Dict()
        cquery.query.filtered.query.query_string.query = qstring
        cquery.query.filtered.query.query_string.analyze_wildcard = wildCard
        cquery.query.filtered.filter.bool.must = [
            {"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
        cquery.query.filtered.filter.bool.must_not = []
        cquery.size = qsize

        cquery.aggs["2"].date_histogram.field = "@timestamp"
        cquery.aggs["2"].date_histogram.interval = qinterval
        cquery.aggs["2"].date_histogram.time_zone = "Europe/Helsinki"
        cquery.aggs["2"].date_histogram.min_doc_count = qmin_doc_count
        cquery.aggs["2"].date_histogram.extended_bounds.min = qgte
        cquery.aggs["2"].date_histogram.extended_bounds.max = qlte

        # Specifc system metrics for cpu load
        cquery.aggs["2"].aggs["1"].avg.field = "shortterm"
        cquery.aggs["2"].aggs["3"].avg.field = "midterm"
        cquery.aggs["2"].aggs["4"].avg.field = "longterm"
        cqueryd = cquery.to_dict()
        return cqueryd

    def systemMemoryQuery(self, qstring, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
                            qmin_doc_count=1):
        cquery = Dict()
        cquery.query.filtered.query.query_string.query = qstring
        cquery.query.filtered.query.query_string.analyze_wildcard = wildCard
        cquery.query.filtered.filter.bool.must = [
            {"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
        cquery.query.filtered.filter.bool.must_not = []
        cquery.size = qsize

        cquery.aggs["2"].date_histogram.field = "@timestamp"
        cquery.aggs["2"].date_histogram.interval = qinterval
        cquery.aggs["2"].date_histogram.time_zone = "Europe/Helsinki"
        cquery.aggs["2"].date_histogram.min_doc_count = qmin_doc_count
        cquery.aggs["2"].date_histogram.extended_bounds.min = qgte
        cquery.aggs["2"].date_histogram.extended_bounds.max = qlte

        cquery.aggs["2"].aggs["3"].terms.field = "type_instance.raw"
        cquery.aggs["2"].aggs["3"].terms.size = 0
        cquery.aggs["2"].aggs["3"].terms.order["1"] = "desc"
        cquery.aggs["2"].aggs["3"].aggs["1"].avg.field = "value"
        cqueryd = cquery.to_dict()
        return cqueryd

    def systemInterfaceQuery(self, qstring, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
                            qmin_doc_count=1):

        cquery = Dict()
        cquery.query.filtered.query.query_string.query = qstring
        cquery.query.filtered.query.query_string.analyze_wildcard = wildCard
        cquery.query.filtered.filter.bool.must = [
            {"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
        cquery.query.filtered.filter.bool.must_not = []
        cquery.size = qsize

        cquery.aggs["3"].date_histogram.field = "@timestamp"
        cquery.aggs["3"].date_histogram.interval = qinterval
        cquery.aggs["3"].date_histogram.time_zone = "Europe/Helsinki"
        cquery.aggs["3"].date_histogram.min_doc_count = qmin_doc_count
        cquery.aggs["3"].date_histogram.extended_bounds.min = qgte
        cquery.aggs["3"].date_histogram.extended_bounds.max = qlte
        cquery.aggs["3"].aggs["1"].avg.field = "tx"
        cquery.aggs["3"].aggs["2"].avg.field = "rx"
        cqueryd = cquery.to_dict()
        return cqueryd

    def dfsQuery(self, qstring, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
                            qmin_doc_count=1):

        cquery = Dict()
        cquery.query.filtered.query.query_string.query = qstring
        cquery.query.filtered.query.query_string.analyze_wildcard = wildCard
        cquery.query.filtered.filter.bool.must = [
            {"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
        cquery.query.filtered.filter.bool.must_not = []
        cquery.size = qsize

        cquery.aggs["3"].date_histogram.field = "@timestamp"
        cquery.aggs["3"].date_histogram.interval = qinterval
        cquery.aggs["3"].date_histogram.time_zone = "Europe/Helsinki"
        cquery.aggs["3"].date_histogram.min_doc_count = qmin_doc_count
        cquery.aggs["3"].date_histogram.extended_bounds.min = qgte
        cquery.aggs["3"].date_histogram.extended_bounds.max = qlte

        #DFS metrics
        cquery.aggs["3"].aggs["2"].avg.field = "CreateFileOps"
        cquery.aggs["3"].aggs["4"].avg.field = "FilesCreated"
        cquery.aggs["3"].aggs["5"].avg.field = "FilesAppended"
        cquery.aggs["3"].aggs["6"].avg.field = "GetBlockLocations"
        cquery.aggs["3"].aggs["7"].avg.field = "FilesRenamed"
        cquery.aggs["3"].aggs["8"].avg.field = "GetListingOps"
        cquery.aggs["3"].aggs["9"].avg.field = "DeleteFileOps"
        cquery.aggs["3"].aggs["10"].avg.field = "FilesDeleted"
        cquery.aggs["3"].aggs["11"].avg.field = "FileInfoOps"
        cquery.aggs["3"].aggs["12"].avg.field = "AddBlockOps"
        cquery.aggs["3"].aggs["13"].avg.field = "GetAdditionalDatanodeOps"
        cquery.aggs["3"].aggs["14"].avg.field = "CreateSymlinkOps"
        cquery.aggs["3"].aggs["15"].avg.field = "GetLinkTargetOps"
        cquery.aggs["3"].aggs["16"].avg.field = "FilesInGetListingOps"
        cquery.aggs["3"].aggs["17"].avg.field = "AllowSnapshotOps"
        cquery.aggs["3"].aggs["18"].avg.field = "DisallowSnapshotOps"
        cquery.aggs["3"].aggs["19"].avg.field = "CreateSnapshotOps"
        cquery.aggs["3"].aggs["20"].avg.field = "DeleteSnapshotOps"
        cquery.aggs["3"].aggs["21"].avg.field = "RenameSnapshotOps"
        cquery.aggs["3"].aggs["22"].avg.field = "ListSnapshottableDirOps"
        cquery.aggs["3"].aggs["23"].avg.field = "SnapshotDiffReportOps"
        cquery.aggs["3"].aggs["24"].avg.field = "BlockReceivedAndDeletedOps"
        cquery.aggs["3"].aggs["25"].avg.field = "StorageBlockReportOps"
        cquery.aggs["3"].aggs["26"].avg.field = "TransactionsNumOps"
        cquery.aggs["3"].aggs["27"].avg.field = "TransactionsAvgTime"
        cquery.aggs["3"].aggs["28"].avg.field = "SnapshotNumOps"
        cquery.aggs["3"].aggs["29"].avg.field = "SyncsAvgTime"
        cquery.aggs["3"].aggs["30"].avg.field = "TransactionsBatchedInSync"
        cquery.aggs["3"].aggs["31"].avg.field = "BlockReportNumOps"
        cquery.aggs["3"].aggs["32"].avg.field = "BlockReportAvgTime"
        cquery.aggs["3"].aggs["33"].avg.field = "SafeModeTime"
        cquery.aggs["3"].aggs["34"].avg.field = "FsImageLoadTime"
        cquery.aggs["3"].aggs["35"].avg.field = "GetEditNumOps"
        cquery.aggs["3"].aggs["36"].avg.field = "GetGroupsAvgTime"
        cquery.aggs["3"].aggs["37"].avg.field = "GetImageNumOps"
        cquery.aggs["3"].aggs["38"].avg.field = "GetImageAvgTime"
        cquery.aggs["3"].aggs["39"].avg.field = "PutImageNumOps"
        cquery.aggs["3"].aggs["40"].avg.field = "PutImageAvgTime"
        cqueryd = cquery.to_dict()
        return cqueryd

    def dfsFSQuery(self, qstring, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
                            qmin_doc_count=1):
        cquery = Dict()
        cquery.query.filtered.query.query_string.query = qstring
        cquery.query.filtered.query.query_string.analyze_wildcard = wildCard
        cquery.query.filtered.filter.bool.must = [
            {"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
        cquery.query.filtered.filter.bool.must_not = []
        cquery.size = qsize

        cquery.aggs["34"].date_histogram.field = "@timestamp"
        cquery.aggs["34"].date_histogram.interval = qinterval
        cquery.aggs["34"].date_histogram.time_zone = "Europe/Helsinki"
        cquery.aggs["34"].date_histogram.min_doc_count = qmin_doc_count
        cquery.aggs["34"].date_histogram.extended_bounds.min = qgte
        cquery.aggs["34"].date_histogram.extended_bounds.max = qlte

        #DFS FS metrics
        cquery.aggs["34"].aggs["1"].avg.field = "BlocksTotal"
        cquery.aggs["34"].aggs["2"].avg.field = "MissingBlocks"
        cquery.aggs["34"].aggs["3"].avg.field = "MissingReplOneBlocks"
        cquery.aggs["34"].aggs["4"].avg.field = "ExpiredHeartbeats"
        cquery.aggs["34"].aggs["5"].avg.field = "TransactionsSinceLastCheckpoint"
        cquery.aggs["34"].aggs["6"].avg.field = "TransactionsSinceLastLogRoll"
        cquery.aggs["34"].aggs["7"].avg.field = "LastWrittenTransactionId"
        cquery.aggs["34"].aggs["8"].avg.field = "LastCheckpointTime"
        cquery.aggs["34"].aggs["9"].avg.field = "UnderReplicatedBlocks"
        cquery.aggs["34"].aggs["10"].avg.field = "CorruptBlocks"
        cquery.aggs["34"].aggs["11"].avg.field = "CapacityTotal"
        cquery.aggs["34"].aggs["12"].avg.field = "CapacityTotalGB"
        cquery.aggs["34"].aggs["13"].avg.field = "CapacityUsed"
        #cquery.aggs["34"].aggs["14"].avg.field = "CapacityTotalGB" ####
        cquery.aggs["34"].aggs["15"].avg.field = "CapacityUsed"
        cquery.aggs["34"].aggs["16"].avg.field = "CapacityUsedGB"
        cquery.aggs["34"].aggs["17"].avg.field = "CapacityRemaining"
        cquery.aggs["34"].aggs["18"].avg.field = "CapacityRemainingGB"
        cquery.aggs["34"].aggs["19"].avg.field = "CapacityUsedNonDFS"
        cquery.aggs["34"].aggs["20"].avg.field = "TotalLoad"
        cquery.aggs["34"].aggs["21"].avg.field = "SnapshottableDirectories"
        cquery.aggs["34"].aggs["22"].avg.field = "Snapshots"
        cquery.aggs["34"].aggs["23"].avg.field = "FilesTotal"
        cquery.aggs["34"].aggs["24"].avg.field = "PendingReplicationBlocks"
        cquery.aggs["34"].aggs["25"].avg.field = "ScheduledReplicationBlocks"
        cquery.aggs["34"].aggs["26"].avg.field = "PendingDeletionBlocks"
        cquery.aggs["34"].aggs["27"].avg.field = "ExcessBlocks"
        cquery.aggs["34"].aggs["28"].avg.field = "PostponedMisreplicatedBlocks"
        cquery.aggs["34"].aggs["29"].avg.field = "PendingDataNodeMessageCount"
        cquery.aggs["34"].aggs["30"].avg.field = "MillisSinceLastLoadedEdits"
        cquery.aggs["34"].aggs["31"].avg.field = "BlockCapacity"
        cquery.aggs["34"].aggs["32"].avg.field = "StaleDataNodes"
        cquery.aggs["34"].aggs["33"].avg.field = "TotalFiles"
        cqueryd = cquery.to_dict()
        return cqueryd

    def jvmNNquery(self, qstring, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
                            qmin_doc_count=1):
        cquery = Dict()
        cquery.query.filtered.query.query_string.query = qstring
        cquery.query.filtered.query.query_string.analyze_wildcard = wildCard
        cquery.query.filtered.filter.bool.must = [
            {"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
        cquery.query.filtered.filter.bool.must_not = []
        cquery.size = qsize

        cquery.aggs["13"].date_histogram.field = "@timestamp"
        cquery.aggs["13"].date_histogram.interval = qinterval
        cquery.aggs["13"].date_histogram.time_zone = "Europe/Helsinki"
        cquery.aggs["13"].date_histogram.min_doc_count = qmin_doc_count
        cquery.aggs["13"].date_histogram.extended_bounds.min = qgte
        cquery.aggs["13"].date_histogram.extended_bounds.max = qlte

        #NN JVM Metrics
        cquery.aggs["13"].aggs["1"].avg.field = "MemNonHeapUsedM"
        cquery.aggs["13"].aggs["2"].avg.field = "MemNonHeapCommittedM"
        cquery.aggs["13"].aggs["3"].avg.field = "MemHeapUsedM"
        cquery.aggs["13"].aggs["4"].avg.field = "MemHeapCommittedM"
        cquery.aggs["13"].aggs["5"].avg.field = "MemHeapMaxM"
        cquery.aggs["13"].aggs["6"].avg.field = "MemMaxM"
        cquery.aggs["13"].aggs["7"].avg.field = "GcCountParNew"
        cquery.aggs["13"].aggs["8"].avg.field = "GcTimeMillisParNew"
        cquery.aggs["13"].aggs["9"].avg.field = "GcCountConcurrentMarkSweep"
        cquery.aggs["13"].aggs["10"].avg.field = "GcTimeMillisConcurrentMarkSweep"
        cquery.aggs["13"].aggs["11"].avg.field = "GcCount"
        cquery.aggs["13"].aggs["12"].avg.field = "GcTimeMillis"
        cquery.aggs["13"].aggs["14"].avg.field = "GcNumWarnThresholdExceeded"
        cquery.aggs["13"].aggs["15"].avg.field = "GcNumInfoThresholdExceeded"
        cquery.aggs["13"].aggs["16"].avg.field = "GcTotalExtraSleepTime"
        cquery.aggs["13"].aggs["17"].avg.field = "ThreadsNew"
        cquery.aggs["13"].aggs["18"].avg.field = "ThreadsRunnable"
        cquery.aggs["13"].aggs["19"].avg.field = "ThreadsBlocked"
        cquery.aggs["13"].aggs["20"].avg.field = "ThreadsWaiting"
        cquery.aggs["13"].aggs["21"].avg.field = "ThreadsTimedWaiting"
        cquery.aggs["13"].aggs["22"].avg.field = "ThreadsTerminated"
        cquery.aggs["13"].aggs["23"].avg.field = "LogError"
        cquery.aggs["13"].aggs["24"].avg.field = "LogFatal"
        cquery.aggs["13"].aggs["25"].avg.field = "LogWarn"
        cquery.aggs["13"].aggs["26"].avg.field = "LogInfo"
        cqueryd = cquery.to_dict()
        return cqueryd

    def jvmMRQuery(self, qstring, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
                            qmin_doc_count=1):
        cquery = Dict()
        cquery.query.filtered.query.query_string.query = qstring
        cquery.query.filtered.query.query_string.analyze_wildcard = wildCard
        cquery.query.filtered.filter.bool.must = [
            {"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
        cquery.query.filtered.filter.bool.must_not = []
        cquery.size = qsize

        cquery.aggs["13"].date_histogram.field = "@timestamp"
        cquery.aggs["13"].date_histogram.interval = qinterval
        cquery.aggs["13"].date_histogram.time_zone = "Europe/Helsinki"
        cquery.aggs["13"].date_histogram.min_doc_count = qmin_doc_count
        cquery.aggs["13"].date_histogram.extended_bounds.min = qgte
        cquery.aggs["13"].date_histogram.extended_bounds.max = qlte

        # NN JVM Metrics
        cquery.aggs["13"].aggs["1"].avg.field = "MemNonHeapUsedM"
        cquery.aggs["13"].aggs["2"].avg.field = "MemNonHeapCommittedM"
        cquery.aggs["13"].aggs["3"].avg.field = "MemHeapUsedM"
        cquery.aggs["13"].aggs["4"].avg.field = "MemHeapCommittedM"
        cquery.aggs["13"].aggs["5"].avg.field = "MemHeapMaxM"
        cquery.aggs["13"].aggs["6"].avg.field = "MemMaxM"
        cquery.aggs["13"].aggs["7"].avg.field = "GcCountParNew"
        cquery.aggs["13"].aggs["8"].avg.field = "GcTimeMillisParNew"
        cquery.aggs["13"].aggs["9"].avg.field = "GcCountConcurrentMarkSweep"
        cquery.aggs["13"].aggs["10"].avg.field = "GcTimeMillisConcurrentMarkSweep"
        cquery.aggs["13"].aggs["11"].avg.field = "GcCount"
        cquery.aggs["13"].aggs["12"].avg.field = "GcTimeMillis"
        cquery.aggs["13"].aggs["17"].avg.field = "ThreadsNew"
        cquery.aggs["13"].aggs["18"].avg.field = "ThreadsRunnable"
        cquery.aggs["13"].aggs["19"].avg.field = "ThreadsBlocked"
        cquery.aggs["13"].aggs["20"].avg.field = "ThreadsWaiting"
        cquery.aggs["13"].aggs["21"].avg.field = "ThreadsTimedWaiting"
        cquery.aggs["13"].aggs["22"].avg.field = "ThreadsTerminated"
        cquery.aggs["13"].aggs["23"].avg.field = "LogError"
        cquery.aggs["13"].aggs["24"].avg.field = "LogFatal"
        cquery.aggs["13"].aggs["25"].avg.field = "LogWarn"
        cquery.aggs["13"].aggs["26"].avg.field = "LogInfo"
        cqueryd = cquery.to_dict()
        return cqueryd

    def resourceQueueQuery(self, qstring, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
                            qmin_doc_count=1):

        cquery = Dict()
        cquery.query.filtered.query.query_string.query = qstring
        cquery.query.filtered.query.query_string.analyze_wildcard = wildCard
        cquery.query.filtered.filter.bool.must = [
            {"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
        cquery.query.filtered.filter.bool.must_not = []
        cquery.size = qsize

        cquery.aggs["23"].date_histogram.field = "@timestamp"
        cquery.aggs["23"].date_histogram.interval = qinterval
        cquery.aggs["23"].date_histogram.time_zone = "Europe/Helsinki"
        cquery.aggs["23"].date_histogram.min_doc_count = qmin_doc_count
        cquery.aggs["23"].date_histogram.extended_bounds.min = qgte
        cquery.aggs["23"].date_histogram.extended_bounds.max = qlte

        # Resource Manager Queue Metrics
        cquery.aggs["23"].aggs["1"].avg.field = "running_0"
        cquery.aggs["23"].aggs["2"].avg.field = "running_60"
        cquery.aggs["23"].aggs["3"].avg.field = "running_300"
        cquery.aggs["23"].aggs["4"].avg.field = "running_1440"
        cquery.aggs["23"].aggs["5"].avg.field = "AppsSubmitted"
        cquery.aggs["23"].aggs["6"].avg.field = "AppsPending"
        cquery.aggs["23"].aggs["7"].avg.field = "AppsCompleted"
        cquery.aggs["23"].aggs["8"].avg.field = "AllocatedMB"
        cquery.aggs["23"].aggs["9"].avg.field = "AllocatedVCores"
        cquery.aggs["23"].aggs["10"].avg.field = "AllocatedContainers"
        cquery.aggs["23"].aggs["11"].avg.field = "AggregateContainersAllocated"
        cquery.aggs["23"].aggs["12"].avg.field = "AggregateContainersReleased"
        cquery.aggs["23"].aggs["13"].avg.field = "AvailableMB"
        cquery.aggs["23"].aggs["14"].avg.field = "AvailableVCores"
        cquery.aggs["23"].aggs["15"].avg.field = "PendingVCores"
        cquery.aggs["23"].aggs["16"].avg.field = "PendingContainers"
        cquery.aggs["23"].aggs["17"].avg.field = "ReservedMB"
        cquery.aggs["23"].aggs["18"].avg.field = "ReservedContainers"
        cquery.aggs["23"].aggs["19"].avg.field = "ActiveUsers"
        cquery.aggs["23"].aggs["20"].avg.field = "ActiveApplications"
        cquery.aggs["23"].aggs["21"].avg.field = "AppAttemptFirstContainerAllocationDelayNumOps"
        cquery.aggs["23"].aggs["22"].avg.field = "AppAttemptFirstContainerAllocationDelayAvgTime"
        cqueryd = cquery.to_dict()
        return cqueryd

    def clusterMetricsQuery(self, qstring, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
                            qmin_doc_count=1):

        cquery = Dict()
        cquery.query.filtered.query.query_string.query = qstring
        cquery.query.filtered.query.query_string.analyze_wildcard = wildCard
        cquery.query.filtered.filter.bool.must = [
            {"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
        cquery.query.filtered.filter.bool.must_not = []
        cquery.size = qsize

        cquery.aggs["2"].date_histogram.field = "@timestamp"
        cquery.aggs["2"].date_histogram.interval = qinterval
        cquery.aggs["2"].date_histogram.time_zone = "Europe/Helsinki"
        cquery.aggs["2"].date_histogram.min_doc_count = qmin_doc_count
        cquery.aggs["2"].date_histogram.extended_bounds.min = qgte
        cquery.aggs["2"].date_histogram.extended_bounds.max = qlte

        # Cluster Metrics
        cquery.aggs["2"].aggs["1"].avg.field = "NumActiveNMs"
        cquery.aggs["2"].aggs["3"].avg.field = "NumDecommissionedNMs"
        cquery.aggs["2"].aggs["4"].avg.field = "NumLostNMs"
        cquery.aggs["2"].aggs["5"].avg.field = "NumUnhealthyNMs"
        cquery.aggs["2"].aggs["6"].avg.field = "AMLaunchDelayNumOps"
        cquery.aggs["2"].aggs["7"].avg.field = "AMLaunchDelayAvgTime"
        cquery.aggs["2"].aggs["8"].avg.field = "AMRegisterDelayNumOps"
        cquery.aggs["2"].aggs["9"].avg.field = "AMRegisterDelayAvgTime"
        cquery.aggs["2"].aggs["10"].avg.field = "NumRebootedNMs"
        cqueryd = cquery.to_dict()
        return cqueryd

    def datanodeMetricsQuery(self, qstring, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
                            qmin_doc_count=1):

        cquery = Dict()
        cquery.query.filtered.query.query_string.query = qstring
        cquery.query.filtered.query.query_string.analyze_wildcard = wildCard
        cquery.query.filtered.filter.bool.must = [
            {"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
        cquery.query.filtered.filter.bool.must_not = []
        cquery.size = qsize

        cquery.aggs["12"].date_histogram.field = "@timestamp"
        cquery.aggs["12"].date_histogram.interval = qinterval
        cquery.aggs["12"].date_histogram.time_zone = "Europe/Helsinki"
        cquery.aggs["12"].date_histogram.min_doc_count = qmin_doc_count
        cquery.aggs["12"].date_histogram.extended_bounds.min = qgte
        cquery.aggs["12"].date_histogram.extended_bounds.max = qlte

        # DataNode Metrics
        cquery.aggs["12"].aggs["1"].avg.field = "BytesWritten"
        cquery.aggs["12"].aggs["2"].avg.field = "TotalWriteTime"
        cquery.aggs["12"].aggs["3"].avg.field = "BytesRead"
        cquery.aggs["12"].aggs["4"].avg.field = "TotalReadTime"
        cquery.aggs["12"].aggs["5"].avg.field = "BlocksWritten"
        cquery.aggs["12"].aggs["6"].avg.field = "BlocksRead"
        cquery.aggs["12"].aggs["7"].avg.field = "BlocksReplicated"
        cquery.aggs["12"].aggs["8"].avg.field = "BlocksRemoved"
        cquery.aggs["12"].aggs["9"].avg.field = "BlocksVerified"
        cquery.aggs["12"].aggs["10"].avg.field = "BlockVerificationFailures"
        cquery.aggs["12"].aggs["11"].avg.field = "BlocksCached"
        cquery.aggs["12"].aggs["13"].avg.field = "BlocksUncached"
        cquery.aggs["12"].aggs["14"].avg.field = "ReadsFromLocalClient"
        cquery.aggs["12"].aggs["15"].avg.field = "ReadsFromRemoteClient"
        cquery.aggs["12"].aggs["16"].avg.field = "WritesFromLocalClient"
        cquery.aggs["12"].aggs["17"].avg.field = "WritesFromRemoteClient"
        cquery.aggs["12"].aggs["18"].avg.field = "BlocksGetLocalPathInfo"
        cquery.aggs["12"].aggs["19"].avg.field = "RemoteBytesRead"
        cquery.aggs["12"].aggs["20"].avg.field = "RemoteBytesWritten"
        cquery.aggs["12"].aggs["21"].avg.field = "RamDiskBlocksWrite"
        cquery.aggs["12"].aggs["22"].avg.field = "RamDiskBlocksWriteFallback"
        cquery.aggs["12"].aggs["23"].avg.field = "RamDiskBytesWrite"
        cquery.aggs["12"].aggs["24"].avg.field = "RamDiskBlocksReadHits"
        cquery.aggs["12"].aggs["25"].avg.field = "RamDiskBlocksEvicted"
        cquery.aggs["12"].aggs["27"].avg.field = "RamDiskBlocksEvictedWithoutRead"
        cquery.aggs["12"].aggs["28"].avg.field = "RamDiskBlocksEvictionWindowMsNumOps"
        cquery.aggs["12"].aggs["29"].avg.field = "RamDiskBlocksEvictionWindowMsAvgTime"
        cquery.aggs["12"].aggs["30"].avg.field = "RamDiskBlocksLazyPersisted"
        cquery.aggs["12"].aggs["31"].avg.field = "RamDiskBlocksDeletedBeforeLazyPersisted"
        cquery.aggs["12"].aggs["32"].avg.field = "RamDiskBytesLazyPersisted"
        cquery.aggs["12"].aggs["33"].avg.field = "RamDiskBlocksLazyPersistWindowMsNumOps"
        cquery.aggs["12"].aggs["34"].avg.field = "RamDiskBlocksLazyPersistWindowMsAvgTime"
        cquery.aggs["12"].aggs["35"].avg.field = "FsyncCount"
        cquery.aggs["12"].aggs["36"].avg.field = "VolumeFailures"
        cquery.aggs["12"].aggs["37"].avg.field = "DatanodeNetworkErrors"
        cquery.aggs["12"].aggs["38"].avg.field = "ReadBlockOpNumOps"
        cquery.aggs["12"].aggs["39"].avg.field = "ReadBlockOpAvgTime"
        cquery.aggs["12"].aggs["40"].avg.field = "CopyBlockOpNumOps"
        cquery.aggs["12"].aggs["41"].avg.field = "CopyBlockOpAvgTime"
        cquery.aggs["12"].aggs["42"].avg.field = "ReplaceBlockOpNumOps"
        cquery.aggs["12"].aggs["43"].avg.field = "ReplaceBlockOpAvgTime"
        cquery.aggs["12"].aggs["44"].avg.field = "HeartbeatsNumOps"
        cquery.aggs["12"].aggs["45"].avg.field = "HeartbeatsAvgTime"
        cquery.aggs["12"].aggs["46"].avg.field = "BlockReportsNumOps"
        cquery.aggs["12"].aggs["47"].avg.field = "BlockReportsAvgTime"
        cquery.aggs["12"].aggs["48"].avg.field = "IncrementalBlockReportsNumOps"
        cquery.aggs["12"].aggs["49"].avg.field = "IncrementalBlockReportsAvgTime"
        cquery.aggs["12"].aggs["50"].avg.field = "CacheReportsNumOps"
        cquery.aggs["12"].aggs["51"].avg.field = "CacheReportsAvgTime"
        cquery.aggs["12"].aggs["52"].avg.field = "PacketAckRoundTripTimeNanosNumOps"
        cquery.aggs["12"].aggs["53"].avg.field = "FlushNanosNumOps"
        cquery.aggs["12"].aggs["54"].avg.field = "FlushNanosAvgTime"
        cquery.aggs["12"].aggs["55"].avg.field = "FsyncNanosNumOps"
        cquery.aggs["12"].aggs["56"].avg.field = "FsyncNanosAvgTime"
        cquery.aggs["12"].aggs["57"].avg.field = "SendDataPacketBlockedOnNetworkNanosNumOps"
        cquery.aggs["12"].aggs["58"].avg.field = "SendDataPacketBlockedOnNetworkNanosAvgTime"
        cquery.aggs["12"].aggs["59"].avg.field = "SendDataPacketTransferNanosNumOps"
        cquery.aggs["12"].aggs["60"].avg.field = "SendDataPacketTransferNanosAvgTime"
        cquery.aggs["12"].aggs["61"].avg.field = "WriteBlockOpNumOps"
        cquery.aggs["12"].aggs["62"].avg.field = "WriteBlockOpAvgTime"
        cquery.aggs["12"].aggs["63"].avg.field = "BlockChecksumOpNumOps"
        cquery.aggs["12"].aggs["64"].avg.field = "BlockChecksumOpAvgTime"
        cqueryd = cquery.to_dict()
        return cqueryd

    def fsopDurationsQuery(self, qstring, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
                            qmin_doc_count=1):
        cquery = Dict()
        cquery.query.filtered.query.query_string.query = qstring
        cquery.query.filtered.query.query_string.analyze_wildcard = wildCard
        cquery.query.filtered.filter.bool.must = [
            {"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
        cquery.query.filtered.filter.bool.must_not = []
        cquery.size = qsize

        cquery.aggs["2"].date_histogram.field = "@timestamp"
        cquery.aggs["2"].date_histogram.interval = qinterval
        cquery.aggs["2"].date_histogram.time_zone = "Europe/Helsinki"
        cquery.aggs["2"].date_histogram.min_doc_count = qmin_doc_count
        cquery.aggs["2"].date_histogram.extended_bounds.min = qgte
        cquery.aggs["2"].date_histogram.extended_bounds.max = qlte

        # FSOpDuration metrics
        cquery.aggs["2"].aggs["1"].avg.field = "ContinuousSchedulingRunNumOps"
        cquery.aggs["2"].aggs["3"].avg.field = "ContinuousSchedulingRunAvgTime"
        cquery.aggs["2"].aggs["4"].avg.field = "ContinuousSchedulingRunStdevTime"
        cquery.aggs["2"].aggs["5"].avg.field = "ContinuousSchedulingRunIMinTime"
        cquery.aggs["2"].aggs["6"].avg.field = "ContinuousSchedulingRunIMaxTime"
        cquery.aggs["2"].aggs["7"].avg.field = "ContinuousSchedulingRunMinTime"
        cquery.aggs["2"].aggs["8"].avg.field = "ContinuousSchedulingRunMaxTime"
        cquery.aggs["2"].aggs["9"].avg.field = "ContinuousSchedulingRunINumOps"
        cquery.aggs["2"].aggs["10"].avg.field = "NodeUpdateCallNumOps"
        cquery.aggs["2"].aggs["11"].avg.field = "NodeUpdateCallAvgTime"
        cquery.aggs["2"].aggs["12"].avg.field = "NodeUpdateCallStdevTime"
        cquery.aggs["2"].aggs["13"].avg.field = "NodeUpdateCallMinTime"
        cquery.aggs["2"].aggs["14"].avg.field = "NodeUpdateCallIMinTime"
        cquery.aggs["2"].aggs["15"].avg.field = "NodeUpdateCallMaxTime"
        cquery.aggs["2"].aggs["16"].avg.field = "NodeUpdateCallINumOps"
        cquery.aggs["2"].aggs["17"].avg.field = "UpdateThreadRunNumOps"
        cquery.aggs["2"].aggs["18"].avg.field = "UpdateThreadRunAvgTime"
        cquery.aggs["2"].aggs["19"].avg.field = "UpdateThreadRunStdevTime"
        cquery.aggs["2"].aggs["20"].avg.field = "UpdateThreadRunIMinTime"
        cquery.aggs["2"].aggs["21"].avg.field = "UpdateThreadRunMinTime"
        cquery.aggs["2"].aggs["22"].avg.field = "UpdateThreadRunMaxTime"
        cquery.aggs["2"].aggs["23"].avg.field = "UpdateThreadRunINumOps"
        cquery.aggs["2"].aggs["24"].avg.field = "UpdateCallNumOps"
        cquery.aggs["2"].aggs["25"].avg.field = "UpdateCallAvgTime"
        cquery.aggs["2"].aggs["26"].avg.field = "UpdateCallStdevTime"
        cquery.aggs["2"].aggs["27"].avg.field = "UpdateCallIMinTime"
        cquery.aggs["2"].aggs["28"].avg.field = "UpdateCallMinTime"
        cquery.aggs["2"].aggs["29"].avg.field = "UpdateCallMaxTime"
        cquery.aggs["2"].aggs["30"].avg.field = "UpdateCallINumOps"
        cquery.aggs["2"].aggs["31"].avg.field = "PreemptCallNumOps"
        cquery.aggs["2"].aggs["32"].avg.field = "PreemptCallAvgTime"
        cquery.aggs["2"].aggs["33"].avg.field = "PreemptCallStdevTime"
        cquery.aggs["2"].aggs["34"].avg.field = "PreemptCallINumOps"
        cqueryd = cquery.to_dict()
        return cqueryd

    def shuffleQuery(self, qstring, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
                            qmin_doc_count=1):
        cquery = Dict()
        cquery.query.filtered.query.query_string.query = qstring
        cquery.query.filtered.query.query_string.analyze_wildcard = wildCard
        cquery.query.filtered.filter.bool.must = [
            {"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
        cquery.query.filtered.filter.bool.must_not = []
        cquery.size = qsize

        cquery.aggs["2"].date_histogram.field = "@timestamp"
        cquery.aggs["2"].date_histogram.interval = qinterval
        cquery.aggs["2"].date_histogram.time_zone = "Europe/Helsinki"
        cquery.aggs["2"].date_histogram.min_doc_count = qmin_doc_count
        cquery.aggs["2"].date_histogram.extended_bounds.min = qgte
        cquery.aggs["2"].date_histogram.extended_bounds.max = qlte

        #Shuffle metrics
        cquery.aggs["2"].aggs["1"].avg.field = "ShuffleConnections"
        cquery.aggs["2"].aggs["3"].avg.field = "ShuffleOutputBytes"
        cquery.aggs["2"].aggs["4"].avg.field = "ShuffleOutputsFailed"
        cquery.aggs["2"].aggs["5"].avg.field = "ShuffleOutputsOK"
        cqueryd = cquery.to_dict()
        return cqueryd

    def queryByProcess(self, qstring, qgte, qlte, qsize, qinterval, wildCard=True, qtformat="epoch_millis",
                            qmin_doc_count=1):
        cquery = Dict()
        cquery.query.filtered.query.query_string.query = qstring
        cquery.query.filtered.query.query_string.analyze_wildcard = wildCard
        cquery.query.filtered.filter.bool.must = [
            {"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
        cquery.query.filtered.filter.bool.must_not = []
        cquery.size = qsize

        cquery.aggs["2"].date_histogram.field = "@timestamp"
        cquery.aggs["2"].date_histogram.interval = qinterval
        cquery.aggs["2"].date_histogram.time_zone = "Europe/Helsinki"
        cquery.aggs["2"].date_histogram.min_doc_count = qmin_doc_count
        cquery.aggs["2"].date_histogram.extended_bounds.min = qgte
        cquery.aggs["2"].date_histogram.extended_bounds.max = qlte

        cquery.fields = ["*", "_source"]
        cquery.script_fields = {}
        cquery.fielddata_fields = ["@timestamp"]

        cquery.sort = [{"@timestamp": {"order": "desc", "unmapped_type": "boolean"}}]
        cqueryd = cquery.to_dict()
        return cqueryd

    def yarnQuery(self):
        return "Yarn metrics query"

    def stormQuery(self):
        return "Storm metrics query"

    def sparkQuery(self):
        return "Spark metrics query"


