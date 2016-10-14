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

dataDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

class Connector():
    def __init__(self, esEndpoint):
        self.esInstance = Elasticsearch(esEndpoint)

    def query(self, queryBody, allm=True, dMetrics=[], debug=False, myIndex="logstash-*"):
        res = self.esInstance.search(index=myIndex, body=queryBody)
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
            logger.error('[%s] : [ERROR] Exception has occured with type %s at arguments %s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
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

    def aggQuery(self, queryBody, myIndex="logstash-*"):
        res = self.esInstance.search(index=myIndex, body=queryBody)
        return res


if __name__ == '__main__':
    testConnector = Connector('85.120.206.27')
    #print testConnector.info()
    query = {
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

query2 = {
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
        "interval": "10s",
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

response = testConnector.aggQuery(query)
# logger.info('This is a test')
response2 = testConnector.aggQuery(query2)

#print type(response['aggregations'])
#print len(response)
#print response2
#print len(response2)

requiredMetrics = []
for key, value in response['aggregations'].iteritems():
    for k, v in value.iteritems():
        for r in v:
            dictMetrics = {}
            for rKey, rValue in r.iteritems():
                if rKey == 'doc_count' or rKey == 'key_as_string':
                    pass
                elif rKey == 'key':
                   # print "%s -> %s"% (rKey, rValue)
                    dictMetrics['key'] = rValue
                else:
                   # print "%s -> %s"% (rKey, rValue['value'])
                    dictMetrics[rKey] = rValue['value']
            requiredMetrics.append(dictMetrics)

print requiredMetrics
cheaders = []
kvImp = {}
for qKey, qValue in query['aggs'].iteritems():
    #print qValue['aggs']
    for v, t in qValue['aggs'].iteritems():
        #print v
        #print t['avg']['field']
        kvImp[v] = t['avg']['field']
        cheaders.append(v)

cheaders.append('key')
filename = 'test.csv'
csvOut = os.path.join(dataDir, filename)
try:
    with open(csvOut, 'wb') as csvfile:
        w = csv.DictWriter(csvfile, cheaders)
        w.writeheader()
        for metrics in requiredMetrics:
            w.writerow(metrics)
    csvfile.close()
except EnvironmentError:
    logger.error('[%s] : [ERROR] File %s could not be created', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), csvOut)
    sys.exit(1)

