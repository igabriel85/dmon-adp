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
import io
import cStringIO
from datetime import datetime
import time
import sys
import pandas as pd
import glob
from util import convertCsvtoArff, csvheaders2colNames
import weka.core.jvm as jvm


class DataFormatter:

    def __init__(self, dataDir):
        self.dataDir = dataDir
        self.fmHead = 0

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
        if not lColumns in df.columns.values:
            logger.error('[%s] : [ERROR] Dataformatter filter method unknown columns %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), lColumns)
            sys.exit(1)
        return df[lColumns]

    def filterRows(self, df, ld, gd=0):
        '''
        :param df: -> dataframe
        :param ld: -> less then key based timeframe in utc
        :param gd: -> greter then key based timeframe in utc
        :return: -> new filtered dataframe
        '''
        if gd:
            try:
                df = df[df.key > gd]
                return df[df.key < ld]
            except Exception as inst:
                logger.error('[%s] : [ERROR] Dataformatter filter method row exited with %s and %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
                sys.exit(1)
        else:
            try:
                return df[df.key < ld]
            except Exception as inst:
                logger.error('[%s] : [ERROR] Dataformatter filter method row exited with %s and %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
                sys.exit(1)

    def dropColumns(self, df, lColumns, cp=True):
        '''
        Inplace true means the selected df will be modified
        :param df: dataframe
        :param lColumns: filtere clolumns
        :param cp: create new df
        '''
        if cp:
            try:
                return df.drop(lColumns, axis=1)
            except Exception as inst:
                logger.error('[%s] : [ERROR] Dataformatter filter method drop columns exited with %s and %s',
                             datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
                sys.exit(1)
        else:
            try:
                df.drop(lColumns, axis=1, inplace=True)
            except Exception as inst:
                logger.error('[%s] : [ERROR] Dataformatter filter method drop columns exited with %s and %s',
                             datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
                sys.exit(1)
            return 0

    def fillMissing(self, df):
        df.fillna(0, inplace=True)

    def dropMissing(self, df):
        df.dropna(axis=1, how='all', inplace=True)

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
        '''
        Second version
        :param csvOne: first csv to load
        :param csvTwo: second csv to load
        :param merged: merged file name
        :return:
        '''
        fone = pd.read_csv(csvOne)
        ftwo = pd.read_csv(csvTwo)
        mergedCsv = pd.concat([fone, ftwo], axis=1, keys='key')
        mergedCsv.to_csv(merged, index=False)

    def mergeall(self, datadir, merged):
        '''
        :param datadir: -> datadir lication
        :param merged: -> name of merged file
        :return:
        '''
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
        if all(isinstance(x, str) for x in lFiles):
            for f in lFiles:
                df = pd.read_csv(f)
                dfList.append(df)
        elif all(isinstance(x, pd.DataFrame) for x in lFiles):
            dfList = lFiles
        else:
            logger.error('[%s] : [ERROR] Cannot merge type %s ',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(type(dfList[0])))
            sys.exit(1)
        # Get first df and set as master
        current = dfList[0].rename(columns=colNames)
        for i, frame in enumerate(dfList[1:], iterStart):
            iterSlave = {}
            for k, v in slaveCol.iteritems():
                iterSlave[k] = v+str(i)
            current = current.merge(frame).rename(columns=iterSlave)
        #current.to_csv(mergedFile)
        # current.set_index('key', inplace=True)
        return current

    def chainMergeNR(self, interface=None, memory=None, load=None, packets=None):
        '''
        :return: -> merged dataframe System metrics
        '''
        if interface is None and memory is None and load is None and packets is None:
            interface = os.path.join(self.dataDir, "Interface.csv")
            memory = os.path.join(self.dataDir, "Memory.csv")
            load = os.path.join(self.dataDir, "Load.csv")
            packets = os.path.join(self.dataDir, "Packets.csv")

        lFiles = [interface, memory, load, packets]
        return self.listMerge(lFiles)

    def chainMergeDFS(self, dfs=None, dfsfs=None, fsop=None):
        '''
        :return: -> merged dfs metrics
        '''
        if dfs is None and dfsfs is None and fsop is None:
            dfs = os.path.join(self.dataDir, "DFS.csv")
            dfsfs = os.path.join(self.dataDir, "DFSFS.csv")
            fsop = os.path.join(self.dataDir, "FSOP.csv")

        lFiles = [dfs, dfsfs, fsop]
        return self.listMerge(lFiles)

    def chainMergeCluster(self, clusterMetrics=None, queue=None, jvmRM=None):
        '''
        :return: -> merged cluster metrics
        '''
        if clusterMetrics is None and queue is None and jvmRM is None:
            clusterMetrics = os.path.join(self.dataDir, "ClusterMetrics.csv")
            queue = os.path.join(self.dataDir, "ResourceManagerQueue.csv")
            jvmRM = os.path.join(self.dataDir, "JVM_RM.csv")
            # jvmmrapp = os.path.join(self.dataDir, "JVM_MRAPP.csv")

        lFiles = [clusterMetrics, queue, jvmRM]

        return self.listMerge(lFiles)

    def chainMergeNM(self, lNM=None, lNMJvm=None, lShuffle=None):
        '''
        :return: -> merged namemanager metrics
        '''

        # Read files
        if lNM is None and lNMJvm is None and lShuffle is None:
            allNM = glob.glob(os.path.join(self.dataDir, "NM_*.csv"))
            allNMJvm = glob.glob(os.path.join(self.dataDir, "JVM_NM_*.csv"))
            allShuffle = glob.glob(os.path.join(self.dataDir, "Shuffle_*.csv"))
        else:
            allNM =lNM
            allNMJvm = lNMJvm
            allShuffle = lShuffle

        # Get column headers and gen dict with new col headers
        colNamesNM = csvheaders2colNames(allNM[0], 'slave1')
        df_NM = self.chainMerge(allNM, colNamesNM, iterStart=2)

        colNamesJVMNM = csvheaders2colNames(allNMJvm[0], 'slave1')
        df_NM_JVM = self.chainMerge(allNMJvm, colNamesJVMNM, iterStart=2)

        colNamesShuffle = csvheaders2colNames(allShuffle[0], 'slave1')
        df_Shuffle = self.chainMerge(allShuffle, colNamesShuffle, iterStart=2)

        return df_NM, df_NM_JVM, df_Shuffle

    def chainMergeDN(self, lDN=None):
        '''
        :return: -> merged datanode metrics
        '''
        # Read files
        if lDN is None:
            allDN = glob.glob(os.path.join(self.dataDir, "DN_*.csv"))
        else:
            allDN = lDN

        # Get column headers and gen dict with new col headers
        colNamesDN = csvheaders2colNames(allDN[0], 'slave1')
        df_DN = self.chainMerge(allDN, colNamesDN, iterStart=2)
        return df_DN

    def chainMergeCassandra(self, lcassandra):
        '''
        :param lcassandra: -> list of cassandra dataframes
        :return: -> merged Cassandra metrics
        '''
        # Read files
        # Get column headers and gen dict with new col headers
        colNamesCa = csvheaders2colNames(lcassandra[0], 'node1')
        df_CA = self.chainMerge(lcassandra, colNamesCa, iterStart=2)
        return df_CA

    def chainMergeMongoDB(self, lmongo):
        '''
        :param lmongo: -> list of mongodb dataframes
        :return: -> merged mongodb metrics
        '''
        # Read files
        # Get column headers and gen dict with new col headers
        colNamesMD = csvheaders2colNames(lmongo[0], 'node1')
        df_MD = self.chainMerge(lmongo, colNamesMD, iterStart=2)
        return df_MD

    def listMerge(self, lFiles):
        '''
        :param lFiles: -> list of files
        :return: merged dataframe
        :note: Only use if dataframes have divergent headers
        '''
        dfList = []
        if all(isinstance(x, str) for x in lFiles):
            for f in lFiles:
                if not f:
                    logger.warning('[%s] : [WARN] Found empty string instead of abs path ...',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                try:
                    df = pd.read_csv(f)
                except Exception as inst:
                    logger.error('[%s] : [ERROR] Cannot load file at %s exiting',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), f)
                    sys.exit(1)
                dfList.append(df)
        elif all(isinstance(x, pd.DataFrame) for x in lFiles):
            dfList = lFiles
        else:
            logger.error('[%s] : [INFO] Cannot merge type %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(type(dfList[0])))
        # for d in dfList:
        #     if d.empty:
        #         logger.warning('[%s] : [INFO] Detected empty dataframe in final merge, removing ...',
        #                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        #
        #         dfList.pop(dfList.index(d))
        try:
            current = reduce(lambda x, y: pd.merge(x, y, on='key'), dfList)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Merge dataframes exception %s with args %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            logger.error('[%s] : [ERROR] Merge dataframes exception df list %s',
                     datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), dfList)
            sys.exit(1)

        # current.set_index('key', inplace=True)
        return current

    def df2csv(self, dataFrame, mergedFile):
        '''
        :param dataFrame: dataframe to save as csv
        :param mergedFile: merged csv file name
        :return:
        '''
        # dataFrame.set_index('key', inplace=True) -> if inplace it modifies all copies of df including
        # in memory resident ones
        if dataFrame.empty:
            logger.error('[%s] : [ERROR] Received empty dataframe for  %s ',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), mergedFile)
            print "Received mepty dataframe for %s " % mergedFile
            sys.exit(1)
        try:
            kDF = dataFrame.set_index('key')
        except Exception as inst:
            logger.error('[%s] : [ERROR] Cannot write dataframe exception %s with arguments %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            print "Cannot write dataframe exception %s with %s'" % (type(inst), inst.args)
            sys.exit(1)

        kDF.to_csv(mergedFile)

    def chainMergeSystem(self, linterface=None, lload=None, lmemory=None, lpack=None):
        logger.info('[%s] : [INFO] Startig system metrics merge .......',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        # Read files

        if linterface is None and lload is None and lmemory is None and lpack is None:
            allIterface = glob.glob(os.path.join(self.dataDir, "Interface_*.csv"))
            allLoad = glob.glob(os.path.join(self.dataDir, "Load_*.csv"))
            allMemory = glob.glob(os.path.join(self.dataDir, "Memory_*.csv"))
            allPackets = glob.glob(os.path.join(self.dataDir, "Packets_*.csv"))

            # Name of merged files
            mergedInterface = os.path.join(self.dataDir, "Interface.csv")
            mergedLoad = os.path.join(self.dataDir, "Load.csv")
            mergedMemory = os.path.join(self.dataDir, "Memory.csv")
            mergedPacket = os.path.join(self.dataDir, "Packets.csv")
            ftd = 1
        else:
            allIterface = linterface
            allLoad = lload
            allMemory = lmemory
            allPackets = lpack
            ftd = 0

        colNamesInterface = {'rx': 'rx_master', 'tx': 'tx_master'}
        df_interface = self.chainMerge(allIterface, colNamesInterface)

        logger.info('[%s] : [INFO] Interface metrics merge complete',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        colNamesPacket = {'rx': 'rx_master', 'tx': 'tx_master'}
        df_packet = self.chainMerge(allPackets, colNamesPacket)

        logger.info('[%s] : [INFO] Packet metrics merge complete',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        colNamesLoad = {'shortterm': 'shortterm_master', 'midterm': 'midterm_master', 'longterm': 'longterm_master'}
        df_load = self.chainMerge(allLoad, colNamesLoad)

        logger.info('[%s] : [INFO] Load metrics merge complete',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        colNamesMemory = {'cached': 'cached_master', 'buffered': 'buffered_master',
                          'used': 'used_master', 'free': 'free_master'}
        df_memory = self.chainMerge(allMemory, colNamesMemory)
        logger.info('[%s] : [INFO] Memory metrics merge complete',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        logger.info('[%s] : [INFO] Sistem metrics merge complete',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        if ftd:
            self.df2csv(df_interface, mergedInterface)
            self.df2csv(df_packet, mergedPacket)
            self.df2csv(df_load, mergedLoad)
            self.df2csv(df_memory, mergedMemory)
            return 0
        else:
            return df_interface, df_load, df_memory, df_packet

    def mergeFinal(self, dfs=None, cluster=None, nodeMng=None, jvmnodeMng=None, dataNode=None, jvmNameNode=None, shuffle=None, system=None):

        if dfs is None and cluster is None and nodeMng is None and jvmnodeMng is None and dataNode is None and jvmNameNode is None and system is None and shuffle is None:
            dfs = os.path.join(self.dataDir, "Merged_DFS.csv")
            cluster = os.path.join(self.dataDir, "Merged_Cluster.csv")
            nodeMng = os.path.join(self.dataDir, "Merged_NM.csv")
            jvmnodeMng = os.path.join(self.dataDir, "Merged_JVM_NM.csv")
            dataNode = os.path.join(self.dataDir, "Merged_DN.csv")
            system = os.path.join(self.dataDir, "System.csv")
            jvmNameNode = os.path.join(self.dataDir, "JVM_NN.csv")
            shuffle = os.path.join(self.dataDir, "Merged_Shuffle.csv")

        lFile = [dfs, cluster, nodeMng, jvmnodeMng, dataNode, jvmNameNode, shuffle, system]
        merged_df = self.listMerge(lFile)
        merged_df.sort_index(axis=1, inplace=True)
        # merged_df.set_index('key', inplace=True)
        #self.dropMissing(merged_df)
        self.fillMissing(merged_df)
        self.fmHead = list(merged_df.columns.values)
        return merged_df

    def dict2csv(self, response, query, filename, df=False):
        '''
        :param response: elasticsearch response
        :param query: elasticserch query
        :param filename: name of file
        :param df: if set to true method returns dataframe and doesn't save to file.
        :return: 0 if saved to file and dataframe if not
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
                        elif query['aggs'].values()[0].values()[1].values()[0].values()[0].values()[0] == 'type_instance.raw' \
                                or query['aggs'].values()[0].values()[1].values()[0].values()[0].values()[0] == 'type_instance':
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
        if query['aggs'].values()[0].values()[1].values()[0].values()[0].values()[0] == "type_instance.raw" or \
                        query['aggs'].values()[0].values()[1].values()[0].values()[0].values()[0] == 'type_instance':
            logger.debug('[%s] : [DEBUG] Detected Memory type query', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            try:
                cheaders = requiredMetrics[0].keys()
            except IndexError:
                logger.error('[%s] : [ERROR] Empty response detected from DMon, stoping detection, check DMon.', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                print "Empty response detected from DMon, stoping detection, check DMon"
                sys.exit(1)
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
        if not df:
            try:
                with open(csvOut, 'wb') as csvfile:
                    w = csv.DictWriter(csvfile, cheaders)
                    w.writeheader()
                    for metrics in requiredMetrics:
                        if set(cheaders) != set(metrics.keys()):
                            logger.error('[%s] : [ERROR] Headers different from required metrics: headers -> %s, metrics ->%s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(cheaders), str(metrics.keys()))
                            diff = list(set(metrics.keys()) - set(cheaders))
                            print "Headers different from required metrics with %s " % diff
                            print "Check qInterval setting for all metrics. Try increasing it!"
                            sys.exit(1)
                        w.writerow(metrics)
                csvfile.close()
            except EnvironmentError:
                logger.error('[%s] : [ERROR] File %s could not be created', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), csvOut)
                sys.exit(1)
            logger.info('[%s] : [INFO] Finished csv %s',
                                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), filename)
            return 0
        else:
            df = pd.DataFrame(requiredMetrics)
            # df.set_index('key', inplace=True)
            logger.info('[%s] : [INFO] Created dataframe',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            return df

    def df2dict(self, df):
        kdf = df.set_index('key')
        return kdf.to_dict()

    def dict2arff(self, fileIn, fileOut):
        '''
        :param fileIn: name of csv file
        :param fileOut: name of new arff file
        :return:
        '''
        dataIn = os.path.join(self.dataDir, fileIn)
        dataOut = os.path.join(self.dataDir, fileOut)
        logger.info('[%s] : [INFO] Starting conversion of %s to %s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), dataIn, dataOut)
        try:
            jvm.start()
            convertCsvtoArff(dataIn, dataOut)
        except Exception as inst:
            pass
        finally:
            logger.error('[%s] : [ERROR] Exception occured while converting to arff with %s and %s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
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

    def toDF(self, fileName):
        '''
        :param fileName: absolute path to file
        :return: dataframe
        '''
        if not os.path.isfile(fileName):
            print "File %s does not exist, cannot load data! Exiting ..." % str(fileName)
            logger.error('[%s] : [ERROR] File %s does not exist',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(fileName))
            sys.exit(1)
        df = pd.read_csv(fileName)
        return df

    def df2BytesIO(self, df):
        out = io.BytesIO()
        self.df2csv(df, out)
        return out

    def df2cStringIO(self, df):
        out = cStringIO.StringIO()
        self.df2csv(df, out)
        return out


