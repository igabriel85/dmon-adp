from pyQueryConstructor import *

qdir = "qdire"
qstring = "serviceType:\"yarn\" AND hostname:\"dice.cdh.slave1\""
qString0 = "serviceType:\"yarn\" AND serviceMetrics:\"NodeManagerMetrics\" AND hostname:\"dice.cdh.slave1\"" #for yarn metrics
wildCard = True
qgte = 1475842980000
qlte = 1475845200000
qtformat = "epoch_millis"
qsize = 0
qinterval = "1s"
qmin_doc_count = 1

qConstructor = QueryConstructor(qdir)
test = qConstructor.yarnNodeManager(qstring, qgte, qlte, qsize, qinterval, wildCard, qtformat,
                                                 qmin_doc_count)

tquery = {
        "query": {
            "filtered": {
                "query": {
                    "query_string": {
                        "query": "serviceType:\"yarn\" AND hostname:\"dice.cdh.slave1\"",
                        "analyze_wildcard": True
                    }
                },
                "filter": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": 1475842980000,
                                        "lte": 1475845200000,
                                        "format": "epoch_millis"
                                    }
                                }
                            }
                        ],
                        "must_not": []
                    }
                }
            }
        },
        "size": 0,
        "aggs": {
            "3": {
                "date_histogram": {
                    "field": "@timestamp",
                    "interval": "1s",
                    "time_zone": "Europe/Helsinki",
                    "min_doc_count": 1,
                    "extended_bounds": {
                        "min": 1475842980000,
                        "max": 1475845200000
                    }
                },
                "aggs": {
                    "40": {
                        "avg": {
                            "field": "ContainersLaunched"
                        }
                    },
                    "41": {
                        "avg": {
                            "field": "ContainersCompleted"
                        }
                    },
                    "42": {
                        "avg": {
                            "field": "ContainersFailed"
                        }
                    },
                    "43": {
                        "avg": {
                            "field": "ContainersKilled"
                        }
                    },
                    "44": {
                        "avg": {
                            "field": "ContainersIniting"
                        }
                    },
                    "45": {
                        "avg": {
                            "field": "ContainersRunning"
                        }
                    },
                    "47": {
                        "avg": {
                            "field": "AllocatedGB"
                        }
                    },
                    "48": {
                        "avg": {
                            "field": "AllocatedContainers"
                        }
                    },
                    "49": {
                        "avg": {
                            "field": "AvailableGB"
                        }
                    },
                    "50": {
                        "avg": {
                            "field": "AllocatedVCores"
                        }
                    },
                    "51": {
                        "avg": {
                            "field": "AvailableVCores"
                        }
                    },
                    "52": {
                        "avg": {
                            "field": "ContainerLaunchDurationNumOps"
                        }
                    },
                    "53": {
                        "avg": {
                            "field": "ContainerLaunchDurationAvgTime"
                        }
                    }
                }
            }
        }
    }

print "Gen->%s" % test
print "Org->%s" % tquery


if test != tquery:
    print "Failed Test 1"
else:
    print "Passed Test 1"


systemRequest = {
  "size": 0,
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "collectd_type:\"load\" AND host:\"dice.cdh.master\"",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      },
      "aggs": {
        "1": {
          "avg": {
            "field": "shortterm"
          }
        },
        "3": {
          "avg": {
            "field": "midterm"
          }
        },
        "4": {
          "avg": {
            "field": "longterm"
          }
        }
      }
    }
  }
}

qstring2 = "collectd_type:\"load\" AND host:\"dice.cdh.master\""
test2 = qConstructor.systemLoadQuery(qstring2, qgte, qlte, qsize, qinterval, wildCard, qtformat,
                                                 qmin_doc_count)

print "Gen->%s" % test2
print "Org->%s" % systemRequest

if test2 != systemRequest:
    print "Failed Test 2"
else:
    print "Passed Test 2"


systemMemory = {
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "collectd_type:\"memory\" AND host:\"dice.cdh.master\"",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "size": 0,
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      },
      "aggs": {
        "3": {
          "terms": {
            "field": "type_instance.raw",
            "size": 0,
            "order": {
              "1": "desc"
            }
          },
          "aggs": {
            "1": {
              "avg": {
                "field": "value"
              }
            }
          }
        }
      }
    }
  }
}

qstring3 = "collectd_type:\"memory\" AND host:\"dice.cdh.master\""
test3 = qConstructor.systemMemoryQuery(qstring3, qgte, qlte, qsize, qinterval, wildCard, qtformat,
                                                 qmin_doc_count)

print "Gen->%s" % test3
print "Org->%s" % systemMemory

if test3 != systemMemory:
    print "Failed Test 3"
else:
    print "Passed Test 3"


systemInterface = {
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "plugin:\"interface\" AND collectd_type:\"if_octets\" AND host:\"dice.cdh.master\"",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "size": 0,
  "aggs": {
    "3": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      },
      "aggs": {
        "1": {
          "avg": {
            "field": "tx"
          }
        },
        "2": {
          "avg": {
            "field": "rx"
          }
        }
      }
    }
  }
}



qstring4 = "plugin:\"interface\" AND collectd_type:\"if_octets\" AND host:\"dice.cdh.master\""
test4 = qConstructor.systemInterfaceQuery(qstring4, qgte, qlte, qsize, qinterval, wildCard, qtformat,
                                                 qmin_doc_count)

print "Gen->%s" % test4
print "Org->%s" % systemInterface

if test4 != systemInterface:
    print "Failed Test 4"
else:
    print "Passed Test 4"

systemInterfacePackets = {
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "plugin:\"interface\" AND collectd_type:\"if_packets\" AND host:\"dice.cdh.master\"",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "size": 0,
  "aggs": {
    "3": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      },
      "aggs": {
        "1": {
          "avg": {
            "field": "tx"
          }
        },
        "2": {
          "avg": {
            "field": "rx"
          }
        }
      }
    }
  }
}


qstring5 = "plugin:\"interface\" AND collectd_type:\"if_packets\" AND host:\"dice.cdh.master\""
test5 = qConstructor.systemInterfaceQuery(qstring5, qgte, qlte, qsize, qinterval, wildCard, qtformat,
                                                 qmin_doc_count)

print "Gen->%s" % test5
print "Org->%s" % systemInterfacePackets

if test5 != systemInterfacePackets:
    print "Failed Test 5"
else:
    print "Passed Test 5"

dfs = {
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "serviceType:\"dfs\"",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "size": 0,
  "aggs": {
    "3": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      },
      "aggs": {
        "2": {
          "avg": {
            "field": "CreateFileOps"
          }
        },
        "4": {
          "avg": {
            "field": "FilesCreated"
          }
        },
        "5": {
          "avg": {
            "field": "FilesAppended"
          }
        },
        "6": {
          "avg": {
            "field": "GetBlockLocations"
          }
        },
        "7": {
          "avg": {
            "field": "FilesRenamed"
          }
        },
        "8": {
          "avg": {
            "field": "GetListingOps"
          }
        },
        "9": {
          "avg": {
            "field": "DeleteFileOps"
          }
        },
        "10": {
          "avg": {
            "field": "FilesDeleted"
          }
        },
        "11": {
          "avg": {
            "field": "FileInfoOps"
          }
        },
        "12": {
          "avg": {
            "field": "AddBlockOps"
          }
        },
        "13": {
          "avg": {
            "field": "GetAdditionalDatanodeOps"
          }
        },
        "14": {
          "avg": {
            "field": "CreateSymlinkOps"
          }
        },
        "15": {
          "avg": {
            "field": "GetLinkTargetOps"
          }
        },
        "16": {
          "avg": {
            "field": "FilesInGetListingOps"
          }
        },
        "17": {
          "avg": {
            "field": "AllowSnapshotOps"
          }
        },
        "18": {
          "avg": {
            "field": "DisallowSnapshotOps"
          }
        },
        "19": {
          "avg": {
            "field": "CreateSnapshotOps"
          }
        },
        "20": {
          "avg": {
            "field": "DeleteSnapshotOps"
          }
        },
        "21": {
          "avg": {
            "field": "RenameSnapshotOps"
          }
        },
        "22": {
          "avg": {
            "field": "ListSnapshottableDirOps"
          }
        },
        "23": {
          "avg": {
            "field": "SnapshotDiffReportOps"
          }
        },
        "24": {
          "avg": {
            "field": "BlockReceivedAndDeletedOps"
          }
        },
        "25": {
          "avg": {
            "field": "StorageBlockReportOps"
          }
        },
        "26": {
          "avg": {
            "field": "TransactionsNumOps"
          }
        },
        "27": {
          "avg": {
            "field": "TransactionsAvgTime"
          }
        },
        "28": {
          "avg": {
            "field": "SnapshotNumOps"
          }
        },
        "29": {
          "avg": {
            "field": "SyncsAvgTime"
          }
        },
        "30": {
          "avg": {
            "field": "TransactionsBatchedInSync"
          }
        },
        "31": {
          "avg": {
            "field": "BlockReportNumOps"
          }
        },
        "32": {
          "avg": {
            "field": "BlockReportAvgTime"
          }
        },
        "33": {
          "avg": {
            "field": "SafeModeTime"
          }
        },
        "34": {
          "avg": {
            "field": "FsImageLoadTime"
          }
        },
        "35": {
          "avg": {
            "field": "GetEditNumOps"
          }
        },
        "36": {
          "avg": {
            "field": "GetGroupsAvgTime"
          }
        },
        "37": {
          "avg": {
            "field": "GetImageNumOps"
          }
        },
        "38": {
          "avg": {
            "field": "GetImageAvgTime"
          }
        },
        "39": {
          "avg": {
            "field": "PutImageNumOps"
          }
        },
        "40": {
          "avg": {
            "field": "PutImageAvgTime"
          }
        }
      }
    }
  }
}


qstring6 = "serviceType:\"dfs\""
test6 = qConstructor.dfsQuery(qstring6, qgte, qlte, qsize, qinterval, wildCard, qtformat,
                                                 qmin_doc_count)

print "Gen->%s" % test6
print "Org->%s" % dfs

if test6 != dfs:
    print "Failed Test 6"
else:
    print "Passed Test 6"


dfsFS = {
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "serviceType:\"dfs\" AND serviceMetrics:\"FSNamesystem\"",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "size": 0,
  "aggs": {
    "34": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      },
      "aggs": {
        "1": {
          "avg": {
            "field": "BlocksTotal"
          }
        },
        "2": {
          "avg": {
            "field": "MissingBlocks"
          }
        },
        "3": {
          "avg": {
            "field": "MissingReplOneBlocks"
          }
        },
        "4": {
          "avg": {
            "field": "ExpiredHeartbeats"
          }
        },
        "5": {
          "avg": {
            "field": "TransactionsSinceLastCheckpoint"
          }
        },
        "6": {
          "avg": {
            "field": "TransactionsSinceLastLogRoll"
          }
        },
        "7": {
          "avg": {
            "field": "LastWrittenTransactionId"
          }
        },
        "8": {
          "avg": {
            "field": "LastCheckpointTime"
          }
        },
        "9": {
          "avg": {
            "field": "UnderReplicatedBlocks"
          }
        },
        "10": {
          "avg": {
            "field": "CorruptBlocks"
          }
        },
        "11": {
          "avg": {
            "field": "CapacityTotal"
          }
        },
        "12": {
          "avg": {
            "field": "CapacityTotalGB"
          }
        },
        "13": {
          "avg": {
            "field": "CapacityUsed"
          }
        },
        "15": {
          "avg": {
            "field": "CapacityUsed"
          }
        },
        "16": {
          "avg": {
            "field": "CapacityUsedGB"
          }
        },
        "17": {
          "avg": {
            "field": "CapacityRemaining"
          }
        },
        "18": {
          "avg": {
            "field": "CapacityRemainingGB"
          }
        },
        "19": {
          "avg": {
            "field": "CapacityUsedNonDFS"
          }
        },
        "20": {
          "avg": {
            "field": "TotalLoad"
          }
        },
        "21": {
          "avg": {
            "field": "SnapshottableDirectories"
          }
        },
        "22": {
          "avg": {
            "field": "Snapshots"
          }
        },
        "23": {
          "avg": {
            "field": "FilesTotal"
          }
        },
        "24": {
          "avg": {
            "field": "PendingReplicationBlocks"
          }
        },
        "25": {
          "avg": {
            "field": "ScheduledReplicationBlocks"
          }
        },
        "26": {
          "avg": {
            "field": "PendingDeletionBlocks"
          }
        },
        "27": {
          "avg": {
            "field": "ExcessBlocks"
          }
        },
        "28": {
          "avg": {
            "field": "PostponedMisreplicatedBlocks"
          }
        },
        "29": {
          "avg": {
            "field": "PendingDataNodeMessageCount"
          }
        },
        "30": {
          "avg": {
            "field": "MillisSinceLastLoadedEdits"
          }
        },
        "31": {
          "avg": {
            "field": "BlockCapacity"
          }
        },
        "32": {
          "avg": {
            "field": "StaleDataNodes"
          }
        },
        "33": {
          "avg": {
            "field": "TotalFiles"
          }
        }
      }
    }
  }
}

qstring7 = "serviceType:\"dfs\" AND serviceMetrics:\"FSNamesystem\""

test7 = qConstructor.dfsFSQuery(qstring7, qgte, qlte, qsize, qinterval, wildCard, qtformat,
                                                 qmin_doc_count)

print "Gen->%s" % test7
print "Org->%s" % dfsFS

if test7 != dfsFS:
    print "Failed Test 7"
else:
    print "Passed Test 7"


nnjvm = {
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "serviceType:\"jvm\" AND ProcessName:\"NameNode\"",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "size": 0,
  "aggs": {
    "13": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      },
      "aggs": {
        "1": {
          "avg": {
            "field": "MemNonHeapUsedM"
          }
        },
        "2": {
          "avg": {
            "field": "MemNonHeapCommittedM"
          }
        },
        "3": {
          "avg": {
            "field": "MemHeapUsedM"
          }
        },
        "4": {
          "avg": {
            "field": "MemHeapCommittedM"
          }
        },
        "5": {
          "avg": {
            "field": "MemHeapMaxM"
          }
        },
        "6": {
          "avg": {
            "field": "MemMaxM"
          }
        },
        "7": {
          "avg": {
            "field": "GcCountParNew"
          }
        },
        "8": {
          "avg": {
            "field": "GcTimeMillisParNew"
          }
        },
        "9": {
          "avg": {
            "field": "GcCountConcurrentMarkSweep"
          }
        },
        "10": {
          "avg": {
            "field": "GcTimeMillisConcurrentMarkSweep"
          }
        },
        "11": {
          "avg": {
            "field": "GcCount"
          }
        },
        "12": {
          "avg": {
            "field": "GcTimeMillis"
          }
        },
        "14": {
          "avg": {
            "field": "GcNumWarnThresholdExceeded"
          }
        },
        "15": {
          "avg": {
            "field": "GcNumInfoThresholdExceeded"
          }
        },
        "16": {
          "avg": {
            "field": "GcTotalExtraSleepTime"
          }
        },
        "17": {
          "avg": {
            "field": "ThreadsNew"
          }
        },
        "18": {
          "avg": {
            "field": "ThreadsRunnable"
          }
        },
        "19": {
          "avg": {
            "field": "ThreadsBlocked"
          }
        },
        "20": {
          "avg": {
            "field": "ThreadsWaiting"
          }
        },
        "21": {
          "avg": {
            "field": "ThreadsTimedWaiting"
          }
        },
        "22": {
          "avg": {
            "field": "ThreadsTerminated"
          }
        },
        "23": {
          "avg": {
            "field": "LogError"
          }
        },
        "24": {
          "avg": {
            "field": "LogFatal"
          }
        },
        "25": {
          "avg": {
            "field": "LogWarn"
          }
        },
        "26": {
          "avg": {
            "field": "LogInfo"
          }
        }
      }
    }
  }
}


qstring8 = "serviceType:\"jvm\" AND ProcessName:\"NameNode\""

test8 = qConstructor.jvmNNquery(qstring8, qgte, qlte, qsize, qinterval, wildCard, qtformat,
                                                 qmin_doc_count)

print "Gen->%s" % test8
print "Org->%s" % nnjvm

if test8 != nnjvm:
    print "Failed Test 8"
else:
    print "Passed Test 8"


nmjvm={
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "serviceType:\"jvm\" AND ProcessName:\"NodeManager\" AND hostname:\"dice.cdh.slave1\"",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "size": 0,
  "aggs": {
    "13": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      },
      "aggs": {
        "1": {
          "avg": {
            "field": "MemNonHeapUsedM"
          }
        },
        "2": {
          "avg": {
            "field": "MemNonHeapCommittedM"
          }
        },
        "3": {
          "avg": {
            "field": "MemHeapUsedM"
          }
        },
        "4": {
          "avg": {
            "field": "MemHeapCommittedM"
          }
        },
        "5": {
          "avg": {
            "field": "MemHeapMaxM"
          }
        },
        "6": {
          "avg": {
            "field": "MemMaxM"
          }
        },
        "7": {
          "avg": {
            "field": "GcCountParNew"
          }
        },
        "8": {
          "avg": {
            "field": "GcTimeMillisParNew"
          }
        },
        "9": {
          "avg": {
            "field": "GcCountConcurrentMarkSweep"
          }
        },
        "10": {
          "avg": {
            "field": "GcTimeMillisConcurrentMarkSweep"
          }
        },
        "11": {
          "avg": {
            "field": "GcCount"
          }
        },
        "12": {
          "avg": {
            "field": "GcTimeMillis"
          }
        },
        "14": {
          "avg": {
            "field": "GcNumWarnThresholdExceeded"
          }
        },
        "15": {
          "avg": {
            "field": "GcNumInfoThresholdExceeded"
          }
        },
        "16": {
          "avg": {
            "field": "GcTotalExtraSleepTime"
          }
        },
        "17": {
          "avg": {
            "field": "ThreadsNew"
          }
        },
        "18": {
          "avg": {
            "field": "ThreadsRunnable"
          }
        },
        "19": {
          "avg": {
            "field": "ThreadsBlocked"
          }
        },
        "20": {
          "avg": {
            "field": "ThreadsWaiting"
          }
        },
        "21": {
          "avg": {
            "field": "ThreadsTimedWaiting"
          }
        },
        "22": {
          "avg": {
            "field": "ThreadsTerminated"
          }
        },
        "23": {
          "avg": {
            "field": "LogError"
          }
        },
        "24": {
          "avg": {
            "field": "LogFatal"
          }
        },
        "25": {
          "avg": {
            "field": "LogWarn"
          }
        },
        "26": {
          "avg": {
            "field": "LogInfo"
          }
        }
      }
    }
  }
}

qstring9 = "serviceType:\"jvm\" AND ProcessName:\"NodeManager\" AND hostname:\"dice.cdh.slave1\""

test9 = qConstructor.jvmNNquery(qstring9, qgte, qlte, qsize, qinterval, wildCard, qtformat,
                                                 qmin_doc_count)

print "Gen->%s" % test9
print "Org->%s" % nmjvm
if test9 != nmjvm:
    print "Failed Test 9"
else:
    print "Passed Test 9"

queueMetrics = {
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "type:\"resourcemanager-metrics\" AND serviceMetrics:\"QueueMetrics\"",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "size": 0,
  "aggs": {
    "23": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      },
      "aggs": {
        "1": {
          "avg": {
            "field": "running_0"
          }
        },
        "2": {
          "avg": {
            "field": "running_60"
          }
        },
        "3": {
          "avg": {
            "field": "running_300"
          }
        },
        "4": {
          "avg": {
            "field": "running_1440"
          }
        },
        "5": {
          "avg": {
            "field": "AppsSubmitted"
          }
        },
        "6": {
          "avg": {
            "field": "AppsPending"
          }
        },
        "7": {
          "avg": {
            "field": "AppsCompleted"
          }
        },
        "8": {
          "avg": {
            "field": "AllocatedMB"
          }
        },
        "9": {
          "avg": {
            "field": "AllocatedVCores"
          }
        },
        "10": {
          "avg": {
            "field": "AllocatedContainers"
          }
        },
        "11": {
          "avg": {
            "field": "AggregateContainersAllocated"
          }
        },
        "12": {
          "avg": {
            "field": "AggregateContainersReleased"
          }
        },
        "13": {
          "avg": {
            "field": "AvailableMB"
          }
        },
        "14": {
          "avg": {
            "field": "AvailableVCores"
          }
        },
        "15": {
          "avg": {
            "field": "PendingVCores"
          }
        },
        "16": {
          "avg": {
            "field": "PendingContainers"
          }
        },
        "17": {
          "avg": {
            "field": "ReservedMB"
          }
        },
        "18": {
          "avg": {
            "field": "ReservedContainers"
          }
        },
        "19": {
          "avg": {
            "field": "ActiveUsers"
          }
        },
        "20": {
          "avg": {
            "field": "ActiveApplications"
          }
        },
        "21": {
          "avg": {
            "field": "AppAttemptFirstContainerAllocationDelayNumOps"
          }
        },
        "22": {
          "avg": {
            "field": "AppAttemptFirstContainerAllocationDelayAvgTime"
          }
        }
      }
    }
  }
}

qstring10 = "type:\"resourcemanager-metrics\" AND serviceMetrics:\"QueueMetrics\""

test10 = qConstructor.resourceQueueQuery(qstring10, qgte, qlte, qsize, qinterval, wildCard, qtformat,
                                                 qmin_doc_count)

print "Gen->%s" % test10
print "Org->%s" % queueMetrics
if test10 != queueMetrics:
    print "Failed Test 10"
else:
    print "Passed Test 10"


clusterMetrics = {
  "size": 0,
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "type:\"resourcemanager-metrics\" AND ClusterMetrics:\"ResourceManager\"",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      },
      "aggs": {
        "1": {
          "avg": {
            "field": "NumActiveNMs"
          }
        },
        "3": {
          "avg": {
            "field": "NumDecommissionedNMs"
          }
        },
        "4": {
          "avg": {
            "field": "NumLostNMs"
          }
        },
        "5": {
          "avg": {
            "field": "NumUnhealthyNMs"
          }
        },
        "6": {
          "avg": {
            "field": "AMLaunchDelayNumOps"
          }
        },
        "7": {
          "avg": {
            "field": "AMLaunchDelayAvgTime"
          }
        },
        "8": {
          "avg": {
            "field": "AMRegisterDelayNumOps"
          }
        },
        "9": {
          "avg": {
            "field": "AMRegisterDelayAvgTime"
          }
        },
        "10": {
          "avg": {
            "field": "NumRebootedNMs"
          }
        }
      }
    }
  }
}

qstring11 = "type:\"resourcemanager-metrics\" AND ClusterMetrics:\"ResourceManager\""

test11 = qConstructor.clusterMetricsQuery(qstring11, qgte, qlte, qsize, qinterval, wildCard, qtformat,
                                          qmin_doc_count)

print "Gen->%s" % test11
print "Org->%s" % clusterMetrics
if test11 != clusterMetrics:
    print "Failed Test 11"
else:
    print "Passed Test 11"


datanode = {
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "type:\"datanode-metrics\" AND serviceType:\"dfs\" AND hostname:\"dice.cdh.slave1\"",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "size": 0,
  "aggs": {
    "12": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      },
      "aggs": {
        "1": {
          "avg": {
            "field": "BytesWritten"
          }
        },
        "2": {
          "avg": {
            "field": "TotalWriteTime"
          }
        },
        "3": {
          "avg": {
            "field": "BytesRead"
          }
        },
        "4": {
          "avg": {
            "field": "TotalReadTime"
          }
        },
        "5": {
          "avg": {
            "field": "BlocksWritten"
          }
        },
        "6": {
          "avg": {
            "field": "BlocksRead"
          }
        },
        "7": {
          "avg": {
            "field": "BlocksReplicated"
          }
        },
        "8": {
          "avg": {
            "field": "BlocksRemoved"
          }
        },
        "9": {
          "avg": {
            "field": "BlocksVerified"
          }
        },
        "10": {
          "avg": {
            "field": "BlockVerificationFailures"
          }
        },
        "11": {
          "avg": {
            "field": "BlocksCached"
          }
        },
        "13": {
          "avg": {
            "field": "BlocksUncached"
          }
        },
        "14": {
          "avg": {
            "field": "ReadsFromLocalClient"
          }
        },
        "15": {
          "avg": {
            "field": "ReadsFromRemoteClient"
          }
        },
        "16": {
          "avg": {
            "field": "WritesFromLocalClient"
          }
        },
        "17": {
          "avg": {
            "field": "WritesFromRemoteClient"
          }
        },
        "18": {
          "avg": {
            "field": "BlocksGetLocalPathInfo"
          }
        },
        "19": {
          "avg": {
            "field": "RemoteBytesRead"
          }
        },
        "20": {
          "avg": {
            "field": "RemoteBytesWritten"
          }
        },
        "21": {
          "avg": {
            "field": "RamDiskBlocksWrite"
          }
        },
        "22": {
          "avg": {
            "field": "RamDiskBlocksWriteFallback"
          }
        },
        "23": {
          "avg": {
            "field": "RamDiskBytesWrite"
          }
        },
        "24": {
          "avg": {
            "field": "RamDiskBlocksReadHits"
          }
        },
        "25": {
          "avg": {
            "field": "RamDiskBlocksEvicted"
          }
        },
        "27": {
          "avg": {
            "field": "RamDiskBlocksEvictedWithoutRead"
          }
        },
        "28": {
          "avg": {
            "field": "RamDiskBlocksEvictionWindowMsNumOps"
          }
        },
        "29": {
          "avg": {
            "field": "RamDiskBlocksEvictionWindowMsAvgTime"
          }
        },
        "30": {
          "avg": {
            "field": "RamDiskBlocksLazyPersisted"
          }
        },
        "31": {
          "avg": {
            "field": "RamDiskBlocksDeletedBeforeLazyPersisted"
          }
        },
        "32": {
          "avg": {
            "field": "RamDiskBytesLazyPersisted"
          }
        },
        "33": {
          "avg": {
            "field": "RamDiskBlocksLazyPersistWindowMsNumOps"
          }
        },
        "34": {
          "avg": {
            "field": "RamDiskBlocksLazyPersistWindowMsAvgTime"
          }
        },
        "35": {
          "avg": {
            "field": "FsyncCount"
          }
        },
        "36": {
          "avg": {
            "field": "VolumeFailures"
          }
        },
        "37": {
          "avg": {
            "field": "DatanodeNetworkErrors"
          }
        },
        "38": {
          "avg": {
            "field": "ReadBlockOpNumOps"
          }
        },
        "39": {
          "avg": {
            "field": "ReadBlockOpAvgTime"
          }
        },
        "40": {
          "avg": {
            "field": "CopyBlockOpNumOps"
          }
        },
        "41": {
          "avg": {
            "field": "CopyBlockOpAvgTime"
          }
        },
        "42": {
          "avg": {
            "field": "ReplaceBlockOpNumOps"
          }
        },
        "43": {
          "avg": {
            "field": "ReplaceBlockOpAvgTime"
          }
        },
        "44": {
          "avg": {
            "field": "HeartbeatsNumOps"
          }
        },
        "45": {
          "avg": {
            "field": "HeartbeatsAvgTime"
          }
        },
        "46": {
          "avg": {
            "field": "BlockReportsNumOps"
          }
        },
        "47": {
          "avg": {
            "field": "BlockReportsAvgTime"
          }
        },
        "48": {
          "avg": {
            "field": "IncrementalBlockReportsNumOps"
          }
        },
        "49": {
          "avg": {
            "field": "IncrementalBlockReportsAvgTime"
          }
        },
        "50": {
          "avg": {
            "field": "CacheReportsNumOps"
          }
        },
        "51": {
          "avg": {
            "field": "CacheReportsAvgTime"
          }
        },
        "52": {
          "avg": {
            "field": "PacketAckRoundTripTimeNanosNumOps"
          }
        },
        "53": {
          "avg": {
            "field": "FlushNanosNumOps"
          }
        },
        "54": {
          "avg": {
            "field": "FlushNanosAvgTime"
          }
        },
        "55": {
          "avg": {
            "field": "FsyncNanosNumOps"
          }
        },
        "56": {
          "avg": {
            "field": "FsyncNanosAvgTime"
          }
        },
        "57": {
          "avg": {
            "field": "SendDataPacketBlockedOnNetworkNanosNumOps"
          }
        },
        "58": {
          "avg": {
            "field": "SendDataPacketBlockedOnNetworkNanosAvgTime"
          }
        },
        "59": {
          "avg": {
            "field": "SendDataPacketTransferNanosNumOps"
          }
        },
        "60": {
          "avg": {
            "field": "SendDataPacketTransferNanosAvgTime"
          }
        },
        "61": {
          "avg": {
            "field": "WriteBlockOpNumOps"
          }
        },
        "62": {
          "avg": {
            "field": "WriteBlockOpAvgTime"
          }
        },
        "63": {
          "avg": {
            "field": "BlockChecksumOpNumOps"
          }
        },
        "64": {
          "avg": {
            "field": "BlockChecksumOpAvgTime"
          }
        }
      }
    }
  }
}


qstring12 = "type:\"datanode-metrics\" AND serviceType:\"dfs\" AND hostname:\"dice.cdh.slave1\""

test12 = qConstructor.datanodeMetricsQuery(qstring12, qgte, qlte, qsize, qinterval, wildCard, qtformat,
                                          qmin_doc_count)

print "Gen->%s" % test12
print "Org->%s" % datanode
if test12 != datanode:
    print "Failed Test 12"
    print "+" * 50
    print "Gen-q->%s" % test12['query']
    print "Org-q->%s" % datanode['query']
    print "-" * 50
    print "Gen-a->%s" %test12['aggs']
    print "Org-a->%s" %datanode['aggs']
    print "-" * 50
    print "Gen-ad->%s" %sorted(test12['aggs']['12']['aggs'].keys())
    print "Org-ad->%s" %sorted(datanode['aggs']['12']['aggs'].keys())
    print "-" * 50
    for k, v in test12['aggs']['12']['aggs'].iteritems():
        if v != datanode['aggs']['12']['aggs'][k]:
            print "%" * 50
            print "Mismatch Key value in original and generated"
            print "Generate has %s -> %s" % (k, v)
            print "Original has %s -> %s" %(k, datanode['aggs']['12']['aggs'][k])
            print "%" * 50
        else:
            print "Match"

    print "-" * 50
    print "Gen-s>%s" %test12['size']
    print "Org-s>%s" %datanode['size']
    print "+" * 50
else:
    print "Passed Test 12"

fsop = {
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "analyze_wildcard": True,
          "query": "type:\"resourcemanager-metrics\" AND serviceMetrics:\"FSOpDurations\""
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "size": 0,
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      },
      "aggs": {
        "1": {
          "avg": {
            "field": "ContinuousSchedulingRunNumOps"
          }
        },
        "3": {
          "avg": {
            "field": "ContinuousSchedulingRunAvgTime"
          }
        },
        "4": {
          "avg": {
            "field": "ContinuousSchedulingRunStdevTime"
          }
        },
        "5": {
          "avg": {
            "field": "ContinuousSchedulingRunIMinTime"
          }
        },
        "6": {
          "avg": {
            "field": "ContinuousSchedulingRunIMaxTime"
          }
        },
        "7": {
          "avg": {
            "field": "ContinuousSchedulingRunMinTime"
          }
        },
        "8": {
          "avg": {
            "field": "ContinuousSchedulingRunMaxTime"
          }
        },
        "9": {
          "avg": {
            "field": "ContinuousSchedulingRunINumOps"
          }
        },
        "10": {
          "avg": {
            "field": "NodeUpdateCallNumOps"
          }
        },
        "11": {
          "avg": {
            "field": "NodeUpdateCallAvgTime"
          }
        },
        "12": {
          "avg": {
            "field": "NodeUpdateCallStdevTime"
          }
        },
        "13": {
          "avg": {
            "field": "NodeUpdateCallMinTime"
          }
        },
        "14": {
          "avg": {
            "field": "NodeUpdateCallIMinTime"
          }
        },
        "15": {
          "avg": {
            "field": "NodeUpdateCallMaxTime"
          }
        },
        "16": {
          "avg": {
            "field": "NodeUpdateCallINumOps"
          }
        },
        "17": {
          "avg": {
            "field": "UpdateThreadRunNumOps"
          }
        },
        "18": {
          "avg": {
            "field": "UpdateThreadRunAvgTime"
          }
        },
        "19": {
          "avg": {
            "field": "UpdateThreadRunStdevTime"
          }
        },
        "20": {
          "avg": {
            "field": "UpdateThreadRunIMinTime"
          }
        },
        "21": {
          "avg": {
            "field": "UpdateThreadRunMinTime"
          }
        },
        "22": {
          "avg": {
            "field": "UpdateThreadRunMaxTime"
          }
        },
        "23": {
          "avg": {
            "field": "UpdateThreadRunINumOps"
          }
        },
        "24": {
          "avg": {
            "field": "UpdateCallNumOps"
          }
        },
        "25": {
          "avg": {
            "field": "UpdateCallAvgTime"
          }
        },
        "26": {
          "avg": {
            "field": "UpdateCallStdevTime"
          }
        },
        "27": {
          "avg": {
            "field": "UpdateCallIMinTime"
          }
        },
        "28": {
          "avg": {
            "field": "UpdateCallMinTime"
          }
        },
        "29": {
          "avg": {
            "field": "UpdateCallMaxTime"
          }
        },
        "30": {
          "avg": {
            "field": "UpdateCallINumOps"
          }
        },
        "31": {
          "avg": {
            "field": "PreemptCallNumOps"
          }
        },
        "32": {
          "avg": {
            "field": "PreemptCallAvgTime"
          }
        },
        "33": {
          "avg": {
            "field": "PreemptCallStdevTime"
          }
        },
        "34": {
          "avg": {
            "field": "PreemptCallINumOps"
          }
        }
      }
    }
  }
}



qstring13 = "type:\"resourcemanager-metrics\" AND serviceMetrics:\"FSOpDurations\""

test13 = qConstructor.fsopDurationsQuery(qstring13, qgte, qlte, qsize, qinterval, wildCard, qtformat,
                                          qmin_doc_count)

print "Gen->%s" % test13
print "Org->%s" % fsop
if test13 != fsop:
    print "Failed Test 13"
    print "+" * 50
    print "Gen-q->%s" % test13['query']
    print "Org-q->%s" % fsop['query']
    print "-" * 50
    print "Gen-a->%s" %test13['aggs']
    print "Org-a->%s" %fsop['aggs']
    print "-" * 50
    print "Gen-ad->%s" %sorted(test13['aggs']['2']['aggs'].keys())
    print "Org-ad->%s" %sorted(fsop['aggs']['2']['aggs'].keys())
    print "-" * 50
    for k, v in test13['aggs']['2']['aggs'].iteritems():
        if v != fsop['aggs']['2']['aggs'][k]:
            print "%" * 50
            print "Mismatch Key value in original and generated"
            print "Generate has %s -> %s" % (k, v)
            print "Original has %s -> %s" %(k, fsop['aggs']['2']['aggs'][k])
            print "%" * 50
        else:
            print "Match"

    print "-" * 50
    print "Gen-s>%s" %test13['size']
    print "Org-s>%s" %fsop['size']
    print "+" * 50
else:
    print "Passed Test 13"


shuffle = {
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "serviceMetrics:\"ShuffleMetrics\" AND serviceType:\"mapred\" AND hostname:\"dice.cdh.slave1\"",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "size": 0,
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      },
      "aggs": {
        "1": {
          "avg": {
            "field": "ShuffleConnections"
          }
        },
        "3": {
          "avg": {
            "field": "ShuffleOutputBytes"
          }
        },
        "4": {
          "avg": {
            "field": "ShuffleOutputsFailed"
          }
        },
        "5": {
          "avg": {
            "field": "ShuffleOutputsOK"
          }
        }
      }
    }
  }
}

qstring14 = "serviceMetrics:\"ShuffleMetrics\" AND serviceType:\"mapred\" AND hostname:\"dice.cdh.slave1\""

test14 = qConstructor.shuffleQuery(qstring14, qgte, qlte, qsize, qinterval, wildCard, qtformat,
                                          qmin_doc_count)

print "Gen->%s" % test14
print "Org->%s" % shuffle
if test14 != shuffle:
    print "Failed Test 14"
    print "+" * 50
    print "Gen-q->%s" % test14['query']
    print "Org-q->%s" % shuffle['query']
    print "-" * 50
    print "Gen-a->%s" %test14['aggs']
    print "Org-a->%s" %shuffle['aggs']
    print "-" * 50
    print "Gen-ad->%s" %sorted(test14['aggs']['2']['aggs'].keys())
    print "Org-ad->%s" %sorted(shuffle['aggs']['2']['aggs'].keys())
    print "-" * 50
    for k, v in test14['aggs']['2']['aggs'].iteritems():
        if v != shuffle['aggs']['2']['aggs'][k]:
            print "%" * 50
            print "Mismatch Key value in original and generated"
            print "Generate has %s -> %s" % (k, v)
            print "Original has %s -> %s" % (k, fsop['aggs']['2']['aggs'][k])
            print "%" * 50
        else:
            print "Match"

    print "-" * 50
    print "Gen-s>%s" %test14['size']
    print "Org-s>%s" %shuffle['size']
    print "+" * 50
else:
    print "Passed Test 14"


processQ = {
  "size": 0,
  "sort": [
    {
      "@timestamp": {
        "order": "desc",
        "unmapped_type": "boolean"
      }
    }
  ],
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "type:\"reducetask-metrics\" AND serviceType:\"jvm\"",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 0,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      }
    }
  },
  "fields": [
    "*",
    "_source"
  ],
  "script_fields": {},
  "fielddata_fields": [
    "@timestamp"
  ]
}

qstring15 = "type:\"reducetask-metrics\" AND serviceType:\"jvm\""

test15 = qConstructor.queryByProcess(qstring15, qgte, qlte, qsize, qinterval, wildCard, qtformat, 0)

print "Gen->%s" % test15
print "Org->%s" % processQ
if test15 != processQ:
    print "Failed Test 15"
else:
    print "Passed Test 15"


stormQ = {
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "type:storm-cluster",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1475842980000,
                  "lte": 1475845200000,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "size": 0,
  "aggs": {
    "5": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1475845200000
        }
      },
      "aggs": {
        "1": {
          "avg": {
            "field": "bolts_0_acked"
          }
        },
        "2": {
          "avg": {
            "field": "bolts_0_capacity"
          }
        },
        "3": {
          "avg": {
            "field": "bolts_0_emitted"
          }
        },
        "4": {
          "avg": {
            "field": "bolts_0_executed"
          }
        },
        "6": {
          "avg": {
            "field": "bolts_0_executors"
          }
        },
        "7": {
          "avg": {
            "field": "bolts_0_failed"
          }
        },
        "8": {
          "avg": {
            "field": "bolts_0_tasks"
          }
        },
        "9": {
          "avg": {
            "field": "bolts_0_transferred"
          }
        },
        "10": {
          "avg": {
            "field": "bolts_1_acked"
          }
        },
        "11": {
          "avg": {
            "field": "bolts_1_emitted"
          }
        },
        "12": {
          "avg": {
            "field": "bolts_1_executed"
          }
        },
        "13": {
          "avg": {
            "field": "bolts_1_executors"
          }
        },
        "14": {
          "avg": {
            "field": "bolts_1_failed"
          }
        },
        "15": {
          "avg": {
            "field": "bolts_1_tasks"
          }
        },
        "16": {
          "avg": {
            "field": "bolts_1_transferred"
          }
        },
        "17": {
          "avg": {
            "field": "executorsTotal"
          }
        },
        "18": {
          "avg": {
            "field": "slotsFree"
          }
        },
        "19": {
          "avg": {
            "field": "slotsTotal"
          }
        },
        "20": {
          "avg": {
            "field": "msgTimeout"
          }
        },
        "21": {
          "avg": {
            "field": "slotsUsed"
          }
        },
        "22": {
          "avg": {
            "field": "spouts_0_acked"
          }
        },
        "23": {
          "avg": {
            "field": "spouts_0_emitted"
          }
        },
        "24": {
          "avg": {
            "field": "spouts_0_executors"
          }
        },
        "25": {
          "avg": {
            "field": "spouts_0_failed"
          }
        },
        "26": {
          "avg": {
            "field": "spouts_0_tasks"
          }
        },
        "27": {
          "avg": {
            "field": "spouts_0_transferred"
          }
        },
        "28": {
          "avg": {
            "field": "supervisors"
          }
        },
        "29": {
          "avg": {
            "field": "tasksTotal"
          }
        },
        "30": {
          "avg": {
            "field": "topology_0_executorsTotal"
          }
        },
        "31": {
          "avg": {
            "field": "topology_0_tasksTotal"
          }
        },
        "32": {
          "avg": {
            "field": "topology_0_workersTotal"
          }
        },
        "33": {
          "avg": {
            "field": "topologyStats_10m_acked"
          }
        },
        "34": {
          "avg": {
            "field": "topologyStats_10m_emitted"
          }
        },
        "35": {
          "avg": {
            "field": "topologyStats_10m_failed"
          }
        },
        "36": {
          "avg": {
            "field": "topologyStats_10m_transferred"
          }
        },
        "37": {
          "avg": {
            "field": "topologyStats_1d_acked"
          }
        },
        "38": {
          "avg": {
            "field": "topologyStats_1d_emitted"
          }
        },
        "39": {
          "avg": {
            "field": "topologyStats_1d_failed"
          }
        },
        "40": {
          "avg": {
            "field": "topologyStats_1d_transferred"
          }
        },
        "41": {
          "avg": {
            "field": "topologyStats_3h_acked"
          }
        },
        "42": {
          "avg": {
            "field": "topologyStats_3h_emitted"
          }
        },
        "43": {
          "avg": {
            "field": "topologyStats_3h_failed"
          }
        },
        "44": {
          "avg": {
            "field": "topologyStats_3h_transferred"
          }
        },
        "45": {
          "avg": {
            "field": "topologyStats_all_acked"
          }
        },
        "46": {
          "avg": {
            "field": "topologyStats_all_emitted"
          }
        },
        "47": {
          "avg": {
            "field": "topologyStats_all_failed"
          }
        },
        "48": {
          "avg": {
            "field": "topologyStats_all_transferred"
          }
        },
        "49": {
          "avg": {
            "field": "workersTotal"
          }
        },
        "50": {
          "avg": {
            "field": "bolts_0_executeLatency"
          }
        },
        "51": {
          "avg": {
            "field": "bolts_0_processLatency"
          }
        },
        "52": {
          "avg": {
            "field": "spouts_0_completeLatency"
          }
        },
        "53": {
          "avg": {
            "field": "topologyStats_10m_window"
          }
        },
        "54": {
          "avg": {
            "field": "topologyStats_10m_completeLatency"
          }
        },
        "55": {
          "avg": {
            "field": "topologyStats_3h_window"
          }
        },
        "56": {
          "avg": {
            "field": "topologyStats_3h_completeLatency"
          }
        },
        "57": {
          "avg": {
            "field": "topologyStats_1d_window"
          }
        },
        "58": {
          "avg": {
            "field": "topologyStats_1d_completeLatency"
          }
        },
        "59": {
          "avg": {
            "field": "topologyStats_all_completeLatency"
          }
        }
      }
    }
  }
}

qstring16 = "type:\"storm-topology\""
bolts = 2
spouts = 1
test16 = qConstructor.stormQuery(qstring15, qgte, qlte, qsize, qinterval, bolts, spouts, wildCard, qtformat, 0)
print test16

cassandraQ = {
  "size": 0,
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "analyze_wildcard": True,
          "query": "plugin:\"GenericJMX\""
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1481030996803,
                  "lte": 1481635796803,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1h",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1475842980000,
          "max": 1481635796803
        }
      },
      "aggs": {
        "3": {
          "terms": {
            "field": "type_instance",
            "size": 1000,
            "order": {
              "1": "desc"
            }
          },
          "aggs": {
            "1": {
              "avg": {
                "field": "value"
              }
            }
          }
        }
      }
    }
  }
}


cepQ = {
  "highlight": {
    "pre_tags": [
      "@kibana-highlighted-field@"
    ],
    "post_tags": [
      "@/kibana-highlighted-field@"
    ],
    "fields": {
      "*": {}
    },
    "require_field_match": False,
    "fragment_size": 2147483647
  },
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "type:cep-posidonia AND DComp:DMON",
          "analyze_wildcard": True
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1498057785356,
                  "lte": 1498144185356,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "size": 500,
  "sort": [
    {
      "@timestamp": {
        "order": "desc",
        "unmapped_type": "boolean"
      }
    }
  ],
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "30s",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 0,
        "extended_bounds": {
          "min": 1498057785356,
          "max": 1498144185356
        }
      }
    }
  },
  "fields": [
    "*",
    "_source"
  ],
  "script_fields": {},
  "fielddata_fields": [
    "@timestamp"
  ]
}

qstring17 = "type:cep-posidonia AND DComp:DMON"
qgte = 1498057785356
qlte = 1498144185356
qsize = 500
qinterval ="30s"
test17 = qConstructor.cepQuery(qstring=qstring17, qgte=qgte, qlte=qlte, qsize=qsize, qinterval=qinterval, qmin_doc_count=0)

print "Gen->%s" % test17
print "Org->%s" % cepQ
if test17 != cepQ:
    print "Failed Test 17"
    print "+" * 50
    print "Gen-q->%s" % test17['query']
    print "Org-q->%s" % cepQ['query']
    print "-" * 50
    print "Gen-a->%s" % test17['aggs']
    print "Org-a->%s" % cepQ['aggs']
    print "-" * 50
    # print "Gen-ad->%s" % sorted(test17['aggs']['2']['aggs'].keys())
    # print "Org-ad->%s" % sorted(cepQ['aggs']['2']['aggs'].keys())
    # print "-" * 50
    # for k, v in test17['aggs']['2']['aggs'].iteritems():
    #     if v != cepQ['aggs']['2']['aggs'][k]:
    #         print "%" * 50
    #         print "Mismatch Key value in original and generated"
    #         print "Generate has %s -> %s" % (k, v)
    #         print "Original has %s -> %s" % (k, fsop['aggs']['2']['aggs'][k])
    #         print "%" * 50
    #     else:
    #         print "Match"

    print "-" * 50
    print "Gen-s>%s" % test17['size']
    print "Org-s>%s" % cepQ['size']
    print "Fail"
    print "+" * 50
else:
    print "Passed Test 17"

sparkQ = {
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "analyze_wildcard": True,
          "query": "*"
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "@timestamp": {
                  "gte": 1506290380885,
                  "lte": 1506376780885,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "size": 0,
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "10m",
        "time_zone": "Europe/Helsinki",
        "min_doc_count": 1,
        "extended_bounds": {
          "min": 1506290380885,
          "max": 1506376780885
        }
      },
      "aggs": {
        "1": {
          "avg": {
            "field": "jvm_total_committed"
          }
        },
        "3": {
          "avg": {
            "field": "jvm_heap_init"
          }
        },
        "4": {
          "avg": {
            "field": "jvm_total_used"
          }
        },
        "5": {
          "avg": {
            "field": "jvm_pools_Tenured-Gen_committed"
          }
        },
        "6": {
          "avg": {
            "field": "jvm_pools_Tenured-Gen_init"
          }
        },
        "7": {
          "avg": {
            "field": "jvm_pools_Tenured-Gen_max"
          }
        },
        "8": {
          "avg": {
            "field": "jvm_pools_Tenured-Gen_usage"
          }
        },
        "9": {
          "avg": {
            "field": "jvm_pools_Tenured-Gen_used"
          }
        },
        "10": {
          "avg": {
            "field": "jvm_pools_Survivor-Space_committed"
          }
        },
        "11": {
          "avg": {
            "field": "jvm_pools_Survivor-Space_init"
          }
        },
        "12": {
          "avg": {
            "field": "jvm_pools_Survivor-Space_max"
          }
        },
        "13": {
          "avg": {
            "field": "jvm_pools_Survivor-Space_usage"
          }
        },
        "14": {
          "avg": {
            "field": "jvm_pools_Survivor-Space_used"
          }
        },
        "15": {
          "avg": {
            "field": "jvm_pools_Metaspace_committed"
          }
        },
        "16": {
          "avg": {
            "field": "jvm_pools_Metaspace_init"
          }
        },
        "17": {
          "avg": {
            "field": "jvm_pools_Metaspace_max"
          }
        },
        "18": {
          "avg": {
            "field": "jvm_pools_Metaspace_usage"
          }
        },
        "19": {
          "avg": {
            "field": "jvm_pools_Metaspace_used"
          }
        },
        "20": {
          "avg": {
            "field": "jvm_pools_Eden-Space_committed"
          }
        },
        "21": {
          "avg": {
            "field": "jvm_pools_Eden-Space_init"
          }
        },
        "22": {
          "avg": {
            "field": "jvm_pools_Eden-Space_max"
          }
        },
        "23": {
          "avg": {
            "field": "jvm_pools_Eden-Space_usage"
          }
        },
        "24": {
          "avg": {
            "field": "jvm_pools_Eden-Space_used"
          }
        },
        "25": {
          "avg": {
            "field": "jvm_pools_Compressed-Class-Space_committed"
          }
        },
        "26": {
          "avg": {
            "field": "jvm_pools_Compressed-Class-Space_init"
          }
        },
        "27": {
          "avg": {
            "field": "jvm_pools_Compressed-Class-Space_max"
          }
        },
        "28": {
          "avg": {
            "field": "jvm_pools_Compressed-Class-Space_usage"
          }
        },
        "29": {
          "avg": {
            "field": "jvm_pools_Compressed-Class-Space_used"
          }
        },
        "30": {
          "avg": {
            "field": "jvm_pools_Code-Cache_committed"
          }
        },
        "31": {
          "avg": {
            "field": "jvm_pools_Code-Cache_init"
          }
        },
        "32": {
          "avg": {
            "field": "jvm_pools_Code-Cache_max"
          }
        },
        "33": {
          "avg": {
            "field": "jvm_pools_Code-Cache_usage"
          }
        },
        "34": {
          "avg": {
            "field": "jvm_pools_Code-Cache_used"
          }
        },
        "35": {
          "avg": {
            "field": "jvm_non-heap_committed"
          }
        },
        "36": {
          "avg": {
            "field": "jvm_non-heap_init"
          }
        },
        "37": {
          "avg": {
            "field": "jvm_non-heap_max"
          }
        },
        "38": {
          "avg": {
            "field": "jvm_non-heap_usage"
          }
        },
        "39": {
          "avg": {
            "field": "jvm_non-heap_used"
          }
        },
        "40": {
          "avg": {
            "field": "jvm_MarkSweepCompact_time"
          }
        },
        "41": {
          "avg": {
            "field": "jvm_MarkSweepCompact_count"
          }
        },
        "42": {
          "avg": {
            "field": "jvm_heap_committed"
          }
        },
        "43": {
          "avg": {
            "field": "jvm_heap_init"
          }
        },
        "44": {
          "avg": {
            "field": "jvm_heap_max"
          }
        },
        "45": {
          "avg": {
            "field": "jvm_heap_usage"
          }
        },
        "46": {
          "avg": {
            "field": "jvm_heap_used"
          }
        },
        "47": {
          "avg": {
            "field": "jvm_MarkSweepCompact_time"
          }
        },
        "48": {
          "avg": {
            "field": "jvm_MarkSweepCompact_count"
          }
        },
        "49": {
          "avg": {
            "field": "jvm_Copy_time"
          }
        },
        "50": {
          "avg": {
            "field": "jvm_Copy_count"
          }
        },
        "51": {
          "avg": {
            "field": "driver_jvm_total_used"
          }
        },
        "52": {
          "avg": {
            "field": "driver_jvm_total_max"
          }
        },
        "53": {
          "avg": {
            "field": "driver_jvm_total_init"
          }
        },
        "54": {
          "avg": {
            "field": "driver_jvm_total_committed"
          }
        },
        "55": {
          "avg": {
            "field": "driver_jvm_pools_Tenured-Gen_used"
          }
        },
        "56": {
          "avg": {
            "field": "driver_jvm_pools_Tenured-Gen_usage"
          }
        },
        "57": {
          "avg": {
            "field": "driver_jvm_pools_Tenured-Gen_max"
          }
        },
        "58": {
          "avg": {
            "field": "driver_jvm_pools_Tenured-Gen_init"
          }
        },
        "59": {
          "avg": {
            "field": "driver_jvm_pools_Tenured-Gen_committed"
          }
        },
        "60": {
          "avg": {
            "field": "driver_jvm_pools_Survivor-Space_used"
          }
        },
        "61": {
          "avg": {
            "field": "driver_jvm_pools_Survivor-Space_usage"
          }
        },
        "62": {
          "avg": {
            "field": "driver_jvm_pools_Survivor-Space_max"
          }
        },
        "63": {
          "avg": {
            "field": "driver_jvm_pools_Survivor-Space_init"
          }
        },
        "64": {
          "avg": {
            "field": "driver_jvm_pools_Survivor-Space_committed"
          }
        },
        "65": {
          "avg": {
            "field": "driver_jvm_pools_Metaspace_used"
          }
        },
        "66": {
          "avg": {
            "field": "driver_jvm_pools_Metaspace_usage"
          }
        },
        "67": {
          "avg": {
            "field": "driver_jvm_pools_Metaspace_max"
          }
        },
        "68": {
          "avg": {
            "field": "driver_jvm_pools_Metaspace_init"
          }
        },
        "69": {
          "avg": {
            "field": "driver_jvm_pools_Metaspace_committed"
          }
        },
        "70": {
          "avg": {
            "field": "driver_jvm_pools_Eden-Space_used"
          }
        },
        "71": {
          "avg": {
            "field": "driver_jvm_pools_Eden-Space_usage"
          }
        },
        "72": {
          "avg": {
            "field": "driver_jvm_pools_Eden-Space_max"
          }
        },
        "73": {
          "avg": {
            "field": "driver_jvm_pools_Eden-Space_init"
          }
        },
        "74": {
          "avg": {
            "field": "driver_jvm_pools_Eden-Space_committed"
          }
        },
        "75": {
          "avg": {
            "field": "driver_jvm_pools_Compressed-Class-Space_used"
          }
        },
        "76": {
          "avg": {
            "field": "driver_jvm_pools_Compressed-Class-Space_usage"
          }
        },
        "77": {
          "avg": {
            "field": "driver_jvm_pools_Survivor-Space_max"
          }
        },
        "78": {
          "avg": {
            "field": "driver_jvm_pools_Compressed-Class-Space_init"
          }
        },
        "79": {
          "avg": {
            "field": "driver_jvm_pools_Compressed-Class-Space_committed"
          }
        },
        "80": {
          "avg": {
            "field": "driver_jvm_pools_Code-Cache_used"
          }
        },
        "81": {
          "avg": {
            "field": "driver_jvm_pools_Code-Cache_usage"
          }
        },
        "82": {
          "avg": {
            "field": "driver_jvm_pools_Code-Cache_max"
          }
        },
        "83": {
          "avg": {
            "field": "driver_jvm_pools_Code-Cache_init"
          }
        },
        "84": {
          "avg": {
            "field": "driver_jvm_pools_Code-Cache_committed"
          }
        },
        "85": {
          "avg": {
            "field": "driver_jvm_non-heap_used"
          }
        },
        "86": {
          "avg": {
            "field": "driver_jvm_non-heap_usage"
          }
        },
        "87": {
          "avg": {
            "field": "driver_jvm_non-heap_max"
          }
        },
        "88": {
          "avg": {
            "field": "driver_jvm_non-heap_init"
          }
        },
        "89": {
          "avg": {
            "field": "driver_jvm_non-heap_committed"
          }
        },
        "90": {
          "avg": {
            "field": "driver_jvm_MarkSweepCompact_time"
          }
        },
        "91": {
          "avg": {
            "field": "driver_jvm_MarkSweepCompact_count"
          }
        },
        "92": {
          "avg": {
            "field": "driver_jvm_heap_used"
          }
        },
        "93": {
          "avg": {
            "field": "driver_jvm_heap_usage"
          }
        },
        "94": {
          "avg": {
            "field": "driver_jvm_heap_max"
          }
        },
        "95": {
          "avg": {
            "field": "driver_jvm_non-heap_init"
          }
        },
        "96": {
          "avg": {
            "field": "driver_jvm_heap_committed"
          }
        },
        "97": {
          "avg": {
            "field": "driver_jvm_Copy_time"
          }
        },
        "98": {
          "avg": {
            "field": "driver_jvm_Copy_count"
          }
        },
        "99": {
          "avg": {
            "field": "driver_ExecutorAllocationManager_executors_numberTargetExecutors"
          }
        },
        "100": {
          "avg": {
            "field": "driver_ExecutorAllocationManager_executors_numberMaxNeededExecutors"
          }
        },
        "101": {
          "avg": {
            "field": "driver_ExecutorAllocationManager_executors_numberExecutorsToAdd"
          }
        },
        "102": {
          "avg": {
            "field": "driver_ExecutorAllocationManager_executors_numberExecutorsPendingToRemove"
          }
        },
        "103": {
          "avg": {
            "field": "driver_ExecutorAllocationManager_executors_numberAllExecutors"
          }
        },
        "104": {
          "avg": {
            "field": "driver_DAGScheduler_stage_waitingStages"
          }
        },
        "105": {
          "avg": {
            "field": "driver_DAGScheduler_stage_runningStages"
          }
        },
        "106": {
          "avg": {
            "field": "driver_DAGScheduler_stage_failedStages"
          }
        },
        "107": {
          "avg": {
            "field": "driver_DAGScheduler_messageProcessingTime_count"
          }
        },
        "108": {
          "avg": {
            "field": "driver_DAGScheduler_messageProcessingTime_m1_rate"
          }
        },
        "109": {
          "avg": {
            "field": "driver_DAGScheduler_messageProcessingTime_m5_rate"
          }
        },
        "110": {
          "avg": {
            "field": "driver_DAGScheduler_messageProcessingTime_m15_rate"
          }
        },
        "111": {
          "avg": {
            "field": "driver_DAGScheduler_messageProcessingTime_max"
          }
        },
        "112": {
          "avg": {
            "field": "driver_DAGScheduler_messageProcessingTime_mean"
          }
        },
        "113": {
          "avg": {
            "field": "driver_DAGScheduler_messageProcessingTime_mean_rate"
          }
        },
        "114": {
          "avg": {
            "field": "driver_DAGScheduler_messageProcessingTime_min"
          }
        },
        "115": {
          "avg": {
            "field": "driver_DAGScheduler_job_allJobs"
          }
        },
        "116": {
          "avg": {
            "field": "driver_DAGScheduler_job_activeJobs"
          }
        },
        "117": {
          "avg": {
            "field": "driver_BlockManager_memory_remainingOnHeapMem_MB"
          }
        },
        "118": {
          "avg": {
            "field": "driver_BlockManager_memory_remainingOffHeapMem_MB"
          }
        },
        "119": {
          "avg": {
            "field": "driver_BlockManager_memory_remainingMem_MB"
          }
        },
        "120": {
          "avg": {
            "field": "driver_BlockManager_memory_onHeapMemUsed_MB"
          }
        },
        "121": {
          "avg": {
            "field": "driver_BlockManager_memory_offHeapMemUsed_MB"
          }
        },
        "122": {
          "avg": {
            "field": "driver_BlockManager_memory_memUsed_MB"
          }
        },
        "123": {
          "avg": {
            "field": "driver_BlockManager_memory_maxOnHeapMem_MB"
          }
        },
        "124": {
          "avg": {
            "field": "driver_BlockManager_memory_maxOffHeapMem_MB"
          }
        },
        "125": {
          "avg": {
            "field": "driver_BlockManager_memory_maxMem_MB"
          }
        },
        "126": {
          "avg": {
            "field": "driver_BlockManager_disk_diskSpaceUsed_MB"
          }
        },
        "127": {
          "avg": {
            "field": "2_executor_threadpool_maxPool_size"
          }
        },
        "128": {
          "avg": {
            "field": "2_executor_threadpool_currentPool_size"
          }
        },
        "129": {
          "avg": {
            "field": "2_executor_filesystem_hdfs_write_ops"
          }
        },
        "130": {
          "avg": {
            "field": "2_executor_filesystem_hdfs_write_bytes"
          }
        },
        "131": {
          "avg": {
            "field": "2_executor_filesystem_hdfs_read_ops"
          }
        },
        "132": {
          "avg": {
            "field": "2_executor_filesystem_hdfs_read_bytes"
          }
        },
        "133": {
          "avg": {
            "field": "2_executor_filesystem_hdfs_largeRead_ops"
          }
        },
        "134": {
          "avg": {
            "field": "2_executor_filesystem_file_write_ops"
          }
        },
        "135": {
          "avg": {
            "field": "2_executor_filesystem_file_write_bytes"
          }
        },
        "136": {
          "avg": {
            "field": "2_executor_filesystem_file_read_ops"
          }
        },
        "137": {
          "avg": {
            "field": "2_executor_filesystem_file_read_bytes"
          }
        },
        "138": {
          "avg": {
            "field": "2_executor_filesystem_file_largeRead_ops"
          }
        },
        "139": {
          "avg": {
            "field": "1_executor_threadpool_maxPool_size"
          }
        },
        "140": {
          "avg": {
            "field": "1_executor_threadpool_currentPool_size"
          }
        },
        "141": {
          "avg": {
            "field": "1_executor_filesystem_hdfs_write_ops"
          }
        },
        "142": {
          "avg": {
            "field": "1_executor_filesystem_hdfs_write_bytes"
          }
        },
        "143": {
          "avg": {
            "field": "1_executor_filesystem_hdfs_read_ops"
          }
        },
        "144": {
          "avg": {
            "field": "1_executor_filesystem_hdfs_read_bytes"
          }
        },
        "145": {
          "avg": {
            "field": "1_executor_filesystem_hdfs_largeRead_ops"
          }
        },
        "146": {
          "avg": {
            "field": "1_executor_filesystem_file_write_ops"
          }
        },
        "147": {
          "avg": {
            "field": "1_executor_filesystem_file_write_bytes"
          }
        },
        "148": {
          "avg": {
            "field": "1_executor_filesystem_file_read_ops"
          }
        },
        "149": {
          "avg": {
            "field": "1_executor_filesystem_file_read_bytes"
          }
        },
        "150": {
          "avg": {
            "field": "1_executor_filesystem_file_largeRead_ops"
          }
        }
      }
    }
  }
}

qstring18 = "*"
qgte = 1506290380885
qlte = 1506376780885
qsize = 0
qinterval ="10m"
test18 = qConstructor.sparkQuery(qstring=qstring18, qgte=qgte, qlte=qlte, qsize=qsize, qinterval=qinterval, qmin_doc_count=1)

print "Gen->%s" % test18
print "Org->%s" % sparkQ
if test18 != sparkQ:
    print "Failed Test 18"
    print "+" * 50
    print "Gen-q->%s" % test18['query']
    print "Org-q->%s" % sparkQ['query']
    print "-" * 50
    print "Gen-a->%s" % test18['aggs']
    print "Org-a->%s" % sparkQ['aggs']
    print "-" * 50
    # print "Gen-ad->%s" % sorted(test17['aggs']['2']['aggs'].keys())
    # print "Org-ad->%s" % sorted(cepQ['aggs']['2']['aggs'].keys())
    # print "-" * 50
    # for k, v in test17['aggs']['2']['aggs'].iteritems():
    #     if v != cepQ['aggs']['2']['aggs'][k]:
    #         print "%" * 50
    #         print "Mismatch Key value in original and generated"
    #         print "Generate has %s -> %s" % (k, v)
    #         print "Original has %s -> %s" % (k, fsop['aggs']['2']['aggs'][k])
    #         print "%" * 50
    #     else:
    #         print "Match"

    print "-" * 50
    print "Gen-s>%s" % test18['size']
    print "Org-s>%s" % sparkQ['size']
    print "Fail"
    print "+" * 50
else:
    print "Passed Test 18"
