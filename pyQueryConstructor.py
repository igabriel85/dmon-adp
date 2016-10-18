from addict import Dict


class QueryConstructor():
    def __init__(self):
        self.author = 'Constructor for dmon-adp ES connector querys'
        # self.systemLoadString = "collectd_type:\"load\" AND host:\"dice.cdh.master\""

    def loadString(self, host):
        qstring = "collectd_type:\"load\" AND host:\"%s\"" % host
        return qstring

    def memoryString(self, host):
        qstring = "collectd_type:\"memory\" AND host:\"%s\"" % host
        return qstring

    def interfaceString(self, host):
        qstring = "plugin:\"interface\" AND collectd_type:\"if_octets\" AND host:\"%s\"" % host
        return qstring

    def packetString(self, host):
        qstring = "plugin:\"interface\" AND collectd_type:\"if_packets\" AND host:\"%s\"" % host
        return qstring

    def dfsString(self):
        qstring = "serviceType:\"dfs\""
        return qstring

    def dfsFString(self):
        qstring = "serviceType:\"dfs\" AND serviceMetrics:\"FSNamesystem\""
        return qstring

    def jvmnodeManagerString(self, host):
        qstring = "serviceType:\"jvm\" AND ProcessName:\"NodeManager\" AND hostname:\"%s\"" % host
        return qstring

    def jvmNameNodeString(self):
        qstring = "serviceType:\"jvm\" AND ProcessName:\"NameNode\""
        return qstring

    def nodeManagerString(self, host):
        qstring = "serviceType:\"yarn\" AND serviceMetrics:\"NodeManagerMetrics\" AND hostname:\"%s\"" % host
        return qstring

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


    def yarnQuery(self):
        return "Yarn metrics query"

    def stormQuery(self):
        return "Storm metrics query"

    def sparkQuery(self):
        return "Spark metrics query"


