from addict import Dict

# class QueryConstructyor():
#     def __init__(self):
#         print 'something'


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
        "46": {
          "avg": {
            "field": "AllocatedGB"
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

#Variables for aggregated query construction
qstring = "serviceType:\"yarn\" AND hostname:\"dice.cdh.slave1\""
wildCard = True
qgte = 1475842980000
qlte = 1475845200000
qtformat = "epoch_millis"
qsize = 0
qinterval = 1
qmin_doc_count = 1


cquery = Dict()
cquery.query.filtered.query.query_string.query = qstring
cquery.query.filtered.query.query_string.analyze_wildcard = wildCard


cquery.query.filtered.filter.bool.must = [{"range": {"@timestamp": {"gte": qgte, "lte": qlte, "format": qtformat}}}]
cquery.query.filtered.filter.bool.must_not = []
cquery.size = qsize
cquery.aggs[3].date_histogram.field = "@timestamp"
cquery.aggs[3].date_histogram.interval = qinterval
cquery.aggs[3].date_histogram.time_zone = "Europe/Helsinki"
cquery.aggs[3].date_histogram.min_doc_count = qmin_doc_count
cquery.aggs[3].date_histogram.extended_bounds.min = qgte
cquery.aggs[3].date_histogram.extended_bounds.max = qlte

#Specify precise metrics, and the average value expressed by 'avg' key
cquery.aggs[3].aggs[40].avg.field = "ContainersLaunched"
cquery.aggs[3].aggs[41].avg.field = "ContainersCompleted"
cquery.aggs[3].aggs[42].avg.field = "ContainersFailed"
cquery.aggs[3].aggs[43].avg.field = "ContainersKilled"
cquery.aggs[3].aggs[44].avg.field = "ContainersIniting"
cquery.aggs[3].aggs[45].avg.field = "ContainersRunning"
cquery.aggs[3].aggs[46].avg.field = "AllocatedGB"
cquery.aggs[3].aggs[47].avg.field = "AvailableGB"
cquery.aggs[3].aggs[48].avg.field = "AllocatedContainers"
cquery.aggs[3].aggs[49].avg.field = "AvailableGB"
cquery.aggs[3].aggs[50].avg.field = "AllocatedVCores"
cquery.aggs[3].aggs[51].avg.field = "AvailableVCores"
cquery.aggs[3].aggs[52].avg.field = "ContainerLaunchDurationNumOps"
cquery.aggs[3].aggs[53].avg.field = "ContainerLaunchDurationAvgTime"


print cquery

if cquery != tquery:
    print "Different!"
else:
    print "The same!"

for k in cquery:
    print k

for t in tquery:
    print t