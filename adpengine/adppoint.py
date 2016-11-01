from elasticsearch import Elasticsearch
from elasticsearch_watcher import WatcherClient
from adplogger import logger
from datetime import datetime
from random import randint
import time
from time import sleep


class adpPoint:

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
            self.esInstance.indices.delete(index=['alerts', 'test', '.watches', 'watch_history*'], ignore=404)
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
                # label the watch
                'metadata': {'tags': ['errors']},

                # Run the watch every 10 seconds
                'trigger': {'schedule': {'interval': '10s'}},

                # Search for at least 3 documents matching the condition
                'condition': {'script': {'inline': 'ctx.payload.hits.total > 3'}},

                # Throttle the watch execution for 30 seconds
                'throttle_period': '30s',

                # The search request to execute
                'input': {
                    'search': {
                        'request': {
                            'indices': ['test'],
                            'body': {
                                'query': {
                                    'filtered': {
                                        'query': {'match': {'status': 500}},
                                        'filter': {'range': {
                                            'timestamp': {'from': '{{ctx.trigger.scheduled_time}}||-5m',
                                                          'to': '{{ctx.trigger.triggered_time}}'}}}
                                    }
                                },
                                # Return statistics about different hosts
                                'aggregations': {
                                    'hosts': {'terms': {'field': 'host'}}
                                }
                            }}}},

                # The actions to perform
                'actions': {
                    'send_email': {
                        'transform': {
                            # Transform the data for the template
                            'script': '''return [
                                    total: ctx.payload.hits.total,
                                    hosts: ctx.payload.aggregations.hosts.buckets.collect { [ host: it.key, errors: it.doc_count ] },
                                    errors: ctx.payload.hits.hits.collect { it._source }
                                ];'''
                        },
                        'email': {
                            'to': 'you@example.com',
                            'subject': '[ALERT] {{ctx.watch_id}}',
                            'attach_data': True,
                            'body': '''
                                Received {{ctx.payload.total}} error documents in the last 5 minutes.

                                Hosts:

                                {{#ctx.payload.hosts}}* {{host}} ({{errors}})
                                {{/ctx.payload.hosts}}'''.replace('\n' + ' ' * 24, '\n').strip(),
                        }
                    },
                    'index_payload': {
                        # Transform the data to be stored
                        'transform': {'script': 'return [ watch_id: ctx.watch_id, payload: ctx.payload ]'},
                        'index': {'index': 'alerts', 'doc_type': 'alert'}
                    },
                    'ping_webhook': {
                        'webhook': {
                            'method': 'POST',
                            'host': 'localhost',
                            'port': 8000,
                            'path': '/',
                            'body': '{"watch_id" : "{{ctx.watch_id}}", "payload" : "{{ctx.payload}}"}'
                        }
                    }
                }
            }
        )

    def deleteWatch(self, watch_id):
        try:
            self.esInstance.watcher.delete_watch(id=watch_id, force=True)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Could not delete watch %s with %s and %s!',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), watch_id, type(inst), inst.args)
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
            #sort='trigger_event.schedule.triggered_time:asc'
        )
        for hit in s['hits']['hits']:
            print('%s: %s' % (hit['_id'], hit['_source']['state']))


testWatcher = adpPoint('85.120.206.27')

print testWatcher.watcherInfo()
#testWatcher.reinitialize()
#testWatcher.putWatch()
#testWatcher.testData()
# for _ in range(30):
#     sleep(1)
#     print '.'
testWatcher.display()
testWatcher.deleteWatch('error_500')


# es = Elasticsearch('85.120.206.27')
# watcher = WatcherClient.infect_client(es)
# test = es.watcher.info()
#
# print test
# print test['version']['number']
