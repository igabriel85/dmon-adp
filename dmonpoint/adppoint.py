from elasticsearch import Elasticsearch
from elasticsearch_watcher import WatcherClient
from adplogger import logger
from datetime import datetime
from random import randint
import time


class AdpPoint:
    def __init__(self, dmonEndpoint):
        self.dmonEndpoint = dmonEndpoint
        self.esInstance = Elasticsearch(dmonEndpoint)
        self.watcher = WatcherClient.infect_client(self.esInstance)

    def watcherInfo(self):
        try:
            ver = self.esInstance.watcher.info()['version']['number']
        except Exception as inst:
            logger.error('[%s] : [ERROR] Could not find ES watcher with %s and %s!',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            return 1
        logger.info('[%s] : [INFO] Watcher version %s detected',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), ver)
        return ver

    def reinitialize(self):
        try:
            self.esInstance.indices.delete(index=['anomalies', '.watches', 'watch_history*'], ignore=404)
        except Exception as inst:
            logger.warning('[%s] : [WARN] Watcher index reinitialization failed with %s and %s',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            return 1

        logger.info('[%s] : [INFO] Watcher index reinitialization succesfull!',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        return 0

    def addWatch(self, watch_id=True, watch_body=True):
        self.esInstance.watcher.put_watch(
            id='error_500',
            body={
                "trigger": {
                    "schedule": {
                        "interval": "10s"
                    }
                },
                "input": {
                    "search": {
                        "request": {
                            "indices": [
                                "logstash-*"
                            ],
                            "types": ['collectd'],
                            "body": {
                                "size": 10,
                                "query": {
                                    "filtered": {
                                        "filter": {
                                            "range": {
                                                "timestamp": {
                                                    "gte": "now-2m",
                                                    "lte": "now"
                                                }
                                            }
                                        }
                                    }
                                },
                                "aggs": {
                                    "minutes": {
                                        "date_histogram": {
                                            "field": "timestamp",
                                            "interval": "minute"
                                        },
                                        "aggs": {
                                            "nodes": {
                                                "terms": {
                                                    "field": "host.raw",
                                                    "size": 10,
                                                    "order": {
                                                        "cpu": "desc"
                                                    }
                                                },
                                                "aggs": {
                                                    "cpu": {
                                                        "avg": {
                                                            "field": "shortterm"
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "throttle_period": "30m",
                "condition": {
                    "script": "if (ctx.payload.aggregations.minutes.buckets.size() == 0) return false; def latest = ctx.payload.aggregations.minutes.buckets[-1]; def node = latest.nodes.buckets[0]; return node && node.cpu && node.cpu.value >= 0.1;"
                },
                "actions": {"index_payload": {

                    "transform": {"script": "return [ watch_id: ctx.watch_id, payload: ctx.payload]"},
                    "index": {"index": "anomaly", "doc_type": "anomaly"}
                }
                }}

        )

    def deleteWatch(self, watch_id):
        try:
            self.esInstance.watcher.delete_watch(id=watch_id, force=True)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Could not delete watch %s with %s and %s!',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), watch_id, type(inst),
                         inst.args)
        logger.info('[%s] : [INFO] Watch %s succesfully deleted!',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

    def watchBodyConstructor(self):
        return 0

    def testData(self):
        for _ in range(5):
            self.esInstance.index(
                index='test',
                doc_type='d',
                body={
                    'timestamp': datetime.utcnow(),
                    'status': 500,
                    'host': '10.0.0.%d' % randint(1, 3)
                }
            )

    def display(self):
        print('=' * 80)
        s = self.esInstance.search(
            index='.watch_history*',
            q='watch_id:error_500',
            # sort='trigger_event.schedule.triggered_time:asc'
        )
        for hit in s['hits']['hits']:
            print('%s: %s' % (hit['_id'], hit['_source']['state']))

    def detpoint(self, data, type, threashold, lt=False):
        '''
        :param data: -> json data
        :param type: -> metric type
        :param threashold: -> set threashold
        :param lt: -> less than is set to false then use greather then
        :return: -> list containing  detected anomalies
        '''
        anomalies = []
        for k, v in data.iteritems():
            if type in k:
                for t, vl in v.iteritems():
                    times = datetime.fromtimestamp(t / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    if lt:
                        if vl > threashold:
                            print "Found anomaly for %s at %s with value %f" % (k, times, vl)
                            logger.info('[%s] : [INFO] Found anomaly for %s at %s with value %f!',
                                     datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), k, times, vl)
                            anomalies.append(
                                {"type": "point", "metric": k, "time": times, "value": vl, "threashold": threashold,
                                     "threashold_type": "upper"})
                    else:
                        if vl < threashold:
                            print "Found anomaly for %s at %s with value %f" % (k, t, vl)
                            anomalies.append(
                                    {"type": "point", "metric": k, "time": times, "value": vl, "threashold": threashold,
                                     "threashold_type": "lower"})
                            logger.info('[%s] : [INFO] Found anomaly for %s at %s with value %f!',
                                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), k, times, vl)
        return anomalies


if __name__ == '__main__':
    testWatcher = AdpPoint('85.120.206.27')

    print testWatcher.watcherInfo()
    testWatcher.reinitialize()
    testWatcher.addWatch()
    # testWatcher.testData()
    # for _ in range(30):
    #     sleep(1)
    #     print '.'
    # testWatcher.display()
    # testWatcher.deleteWatch('error_500')


    # es = Elasticsearch('85.120.206.27')
    # watcher = WatcherClient.infect_client(es)
    # test = es.watcher.info()
    #
    # print test
    # print test['version']['number']
