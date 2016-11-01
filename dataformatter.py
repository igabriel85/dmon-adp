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
import pandas as pd
import glob
from util import convertCsvtoArff
import weka.core.jvm as jvm


class DataFormatter():

    def __init__(self, dataDir):
        self.dataDir = dataDir

    rawdataset = {}

    def getJson(self):
        return 'load Json'

    def filterColumns(self, df, lColumns):
        '''
        :param df: -> dataframe
        :param lColumns: -> column names
        :return: -> filtered df
        '''
        if not isinstance(lColumns, list):
            logger.error('[%s] : [ERROR] Dataformatter filter method expects list of column names not %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(lColumns))
            sys.exit(1)
        return df[lColumns]

    def filterRows(self):
        return "Filtered rows"

    def merge(self, csvOne, csvTwo, merged):
        '''
        :param csvOne: first csv to load
        :param csvTwo: second csv to load
        :param merged: merged file name
        :return:
        '''
        fone = pd.read_csv(csvOne)
        ftwo = pd.read_csv(csvTwo)
        mergedCsv = fone.merge(ftwo, on='key')
        mergedCsv.to_csv(merged, index=False)
        logger.info('[%s] : [INFO] Merged %s and %s into %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                    str(csvOne), str(csvTwo), str(merged))

    def merge2(self, csvOne, csvTwo, merged):
        fone = pd.read_csv(csvOne)
        ftwo = pd.read_csv(csvTwo)
        mergedCsv = pd.concat([fone, ftwo], axis=1, keys='key')
        mergedCsv.to_csv(merged, index=False)
        return "Merge second version"

    def mergeall(self, datadir, merged):
        all_files = glob.glob(os.path.join(datadir, "*.csv"))

        df_from_each_file = (pd.read_csv(f) for f in all_files)
        concatDF = pd.concat(df_from_each_file, ignore_index=True)
        concatDF.to_csv(merged)

    def chainMerge(self, lFiles, colNames, iterStart=1):
        '''
        :param lFiles: -> list of files to be opened
        :param colNames: -> dict with master column names
        :param iterStart: -> start of iteration default is 1
        :return: -> merged dataframe
        '''
        #Parsing colNames
        slaveCol = {}
        for k, v in colNames.iteritems():
            slaveCol[k] = '_'.join([v.split('_')[0], 'slave'])

        dfList = []
        for f in lFiles:
            df = pd.read_csv(f)
            dfList.append(df)

        # Get first df and set as master
        current = dfList[0].rename(columns=colNames)
        for i, frame in enumerate(dfList[1:], iterStart):
            iterSlave ={}
            for k, v in slaveCol.iteritems():
                iterSlave[k] = v+str(i)
            current = current.merge(frame).rename(columns=iterSlave)
        #current.to_csv(mergedFile)
        current.set_index('key', inplace=True)
        return current

    def chainMergeNR(self):
        '''
        :return: -> merged dataframe
        '''

        interface = os.path.join(self.dataDir, "Interface.csv")
        memory = os.path.join(self.dataDir, "Memory.csv")
        load = os.path.join(self.dataDir, "Load.csv")
        packets = os.path.join(self.dataDir, "Packets.csv")

        lFiles = [interface, memory, load, packets]

        return self.listMerge(lFiles)

    def listMerge(self, lFiles):
        dfList = []
        for f in lFiles:
            df = pd.read_csv(f)
            dfList.append(df)

        current = reduce(lambda x, y: pd.merge(x, y, on='key'), dfList)
        current.set_index('key', inplace=True)
        return current

    def df2csv(self, dataFrame, mergedFile):
        '''
        :param dataFrame: dataframe to save as csv
        :param mergedFile: merged csv file name
        :return:
        '''
        dataFrame.to_csv(mergedFile)

    def chainMergeSystem(self):
        logger.info('[%s] : [INFO] Startig system metrics merge .......',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        # Read files
        allIterface = glob.glob(os.path.join(self.dataDir, "Interface_*.csv"))
        allLoad = glob.glob(os.path.join(self.dataDir, "Load_*.csv"))
        allMemory = glob.glob(os.path.join(self.dataDir, "Memory_*.csv"))
        allPackets = glob.glob(os.path.join(self.dataDir, "Packets_*.csv"))

        # Name of merged files
        mergedInterface = os.path.join(self.dataDir, "Interface.csv")
        mergedLoad = os.path.join(self.dataDir, "Load.csv")
        mergedMemory = os.path.join(self.dataDir, "Memory.csv")
        mergedPacket = os.path.join(self.dataDir, "Packets.csv")

        colNamesInterface = {'rx': 'rx_master', 'tx': 'tx_master'}
        df_interface = self.chainMerge(allIterface, colNamesInterface)
        self.df2csv(df_interface, mergedInterface)
        logger.info('[%s] : [INFO] Interface metrics merge complete',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        colNamesPacket = {'rx': 'rx_master', 'tx': 'tx_master'}
        df_packet = self.chainMerge(allPackets, colNamesPacket)
        self.df2csv(df_packet, mergedPacket)
        logger.info('[%s] : [INFO] Packet metrics merge complete',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        colNamesLoad = {'shortterm': 'shortterm_master', 'midterm': 'midterm_master', 'longterm': 'longterm_master'}
        df_load = self.chainMerge(allLoad, colNamesLoad)
        self.df2csv(df_load, mergedLoad)
        logger.info('[%s] : [INFO] Load metrics merge complete',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        colNamesMemory = {'cached': 'cached_master', 'buffered': 'buffered_master',
                          'used': 'used_master', 'free': 'free_master'}
        df_memory = self.chainMerge(allMemory, colNamesMemory)
        self.df2csv(df_memory, mergedMemory)
        logger.info('[%s] : [INFO] Memory metrics merge complete',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        logger.info('[%s] : [INFO] Sistem metrics merge complete',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

    def chainMergeYarn(self):
        logger.info('[%s] : [INFO] Startig YARN metrics merge .......',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        # Read Yarn files
        allJvmNodeManager = glob.glob(os.path.join(self.dataDir, "JVM_NM_*.csv"))
        allNodeManager = glob.glob(os.path.join(self.dataDir, "NM_*.csv"))
        allDataNode = glob.glob(os.path.join(self.dataDir, "DN_*.csv"))

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
        # print "This is the query _------------_-> %s" %query
        # print "This is the response _------------_-> %s" %response
        for key, value in response['aggregations'].iteritems():
            for k, v in value.iteritems():
                for r in v:
                    dictMetrics = {}
                    # print "This is the dictionary ---------> %s " % str(r)
                    for rKey, rValue in r.iteritems():
                        if rKey == 'doc_count' or rKey == 'key_as_string':
                            pass
                        elif rKey == 'key':
                            logger.debug('[%s] : [DEBUG] Request has keys %s and  values %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), rKey, rValue)
                            # print "%s -> %s"% (rKey, rValue)
                            dictMetrics['key'] = rValue
                        elif query['aggs'].values()[0].values()[1].values()[0].values()[0].values()[0] =='type_instance.raw':
                            logger.debug('[%s] : [DEBUG] Detected Memory type aggregation', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                            # print "This is  rValue ________________> %s" % str(rValue)
                            # print "Keys of rValue ________________> %s" % str(rValue.keys())
                            for val in rValue['buckets']:
                                dictMetrics[val['key']] = val['1']['value']
                        else:
                            # print "Values -> %s" % rValue
                            # print "rKey -> %s" % rKey
                            # print "This is the rValue ___________> %s " % str(rValue)
                            logger.debug('[%s] : [DEBUG] Request has keys %s and flattened values %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), rKey, rValue['value'])
                            dictMetrics[rKey] = rValue['value']
                    requiredMetrics.append(dictMetrics)
        # print "Required Metrics -> %s" % requiredMetrics

        csvOut = os.path.join(self.dataDir, filename)
        cheaders = []
        if query['aggs'].values()[0].values()[1].values()[0].values()[0].values()[0] == "type_instance.raw":
            logger.debug('[%s] : [DEBUG] Detected Memory type query', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            cheaders = requiredMetrics[0].keys()
        else:
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
            logger.info('[%s] : [INFO] Dict translator %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(kvImp))
        logger.info('[%s] : [INFO] Headers detected %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(cheaders))

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

    def normalize(self, dataFrame):
        '''
        :param dataFrame: dataframe to be normalized
        :return: normalized data frame
        '''
        dataFrame_norm = (dataFrame -dataFrame.mean())/(dataFrame.max()-dataFrame.min())
        return dataFrame_norm

    def loadData(self, csvList=[]):
        '''
        :param csvList: list of CSVs
        :return: list of data frames
        '''
        if csvList:
            all_files = csvList
        else:
            all_files = glob.glob(os.path.join(self.dataDir, "*.csv"))
        #df_from_each_file = (pd.read_csv(f) for f in all_files)
        dfList = []
        for f in all_files:
            df = pd.read_csv(f)
            dfList.append(df)
        return dfList
