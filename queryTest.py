from pyQueryConstructor import *


qstring = "serviceType:\"yarn\" AND hostname:\"dice.cdh.slave1\""
qString0 = "serviceType:\"yarn\" AND serviceMetrics:\"NodeManagerMetrics\" AND hostname:\"dice.cdh.slave1\"" #for yarn metrics
wildCard = True
qgte = 1475842980000
qlte = 1475845200000
qtformat = "epoch_millis"
qsize = 0
qinterval = "1s"
qmin_doc_count = 1

qConstructor = QueryConstructor()
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
                            "field": "AvailableGB"
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

print test
print tquery


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

print test2
print systemRequest

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

print test3
print systemMemory

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

print test4
print systemInterface

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

print test5
print systemInterfacePackets

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

print test6
print dfs

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

print test7
print dfsFS

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

print test8
print nnjvm

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

print test9
print nmjvm
if test9 != nmjvm:
    print "Failed Test 9"
else:
    print "Passed Test 9"

