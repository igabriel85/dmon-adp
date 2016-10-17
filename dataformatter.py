"""
Copyright 2016, Institute e-Austria, Timisoara, Romania
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
from adplogger import logger
import csv
import os
from datetime import datetime
import time
import sys
from util import convertCsvtoArff
import weka.core.jvm as jvm


class DataFormatter():

    def __init__(self, dataDir):
        self.dataDir = dataDir

    rawdataset = {}

    def getJson(self):
        return 'load Json'

    def filter(self):
        return 'filter dataset'

    def dict2csv(self, response, query, filename):
        '''
        :param response: elasticsearch response
        :param query: elasticserch query
        :param filename: name of file
        :return:
        '''
        requiredMetrics = []
        logger.info('[%s] : [INFO] Started response to csv conversion',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        for key, value in response['aggregations'].iteritems():
            for k, v in value.iteritems():
                for r in v:
                    dictMetrics = {}
                    for rKey, rValue in r.iteritems():
                        if rKey == 'doc_count' or rKey == 'key_as_string':
                            pass
                        elif rKey == 'key':
                            logger.debug('[%s] : [DEBUG] Request has keys %s and  values %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), rKey, rValue)
                            # print "%s -> %s"% (rKey, rValue)
                            dictMetrics['key'] = rValue
                        else:
                            # print "%s -> %s"% (rKey, rValue['value'])
                            logger.debug('[%s] : [DEBUG] Request has keys %s and flattened values %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), rKey, rValue['value'])
                            dictMetrics[rKey] = rValue['value']
                    requiredMetrics.append(dictMetrics)

        cheaders = []
        kvImp = {}
        for qKey, qValue in query['aggs'].iteritems():
            logger.info('[%s] : [INFO] Value aggs from query %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), qValue['aggs'])
            for v, t in qValue['aggs'].iteritems():
                kvImp[v] = t['avg']['field']
                cheaders.append(v)

        cheaders.append('key')
        for key, value in kvImp.iteritems():
            cheaders[cheaders.index(key)] = value
        for e in requiredMetrics:
            for krep, vrep in kvImp.iteritems():
                e[vrep] = e.pop(krep)
        logger.info('[%s] : [INFO] Headers detected %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(cheaders))
        logger.info('[%s] : [INFO] Dict translator %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(kvImp))
        csvOut = os.path.join(self.dataDir, filename)
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
        logger.info('[%s] : [INFO] Finished csv %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), filename)

    def dict2arff(self, fileIn, fileOut):
        dataIn = os.path.join(self.dataDir, fileIn)
        dataOut = os.path.join(self.dataDir, fileOut)
        logger.info('[%s] : [INFO] Starting conversion of %s to %s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), dataIn, dataOut)
        try:
            jvm.start()
            convertCsvtoArff(dataIn, dataOut)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception occured while converting to arff with %s and %s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
        finally:
            jvm.stop()
        logger.info('[%s] : [INFO] Finished conversion of %s to %s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), dataIn, dataOut)

    def normalize(self):
        return "normalized dataset"