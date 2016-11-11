import os
from weka.clusterers import Clusterer
import weka.core.converters as converters
import weka.core.serialization as serialization
import traceback
import weka.core.jvm as jvm
from adplogger import logger
from datetime import datetime
import time
from collections import Counter


class dweka:
    def __init__(self,  dataDir, modelsDir, wHeap = '512m'):
        self.wHeap = wHeap
        self.dataDir = dataDir
        self.modelDir = modelsDir

    def loadData(self, fName, temp=True):
        if temp:
            data = converters.load_any_file(fName)
        else:
            data = converters.load_any_file(os.path.join(self.dataDir, fName))
        return data

    def saveModel(self, model, method, mname):
        finalname = "%s_%s.model" %(method, mname)
        serialization.write(os.path.join(self.modelDir, finalname), model)
        logger.info('[%s] : [INFO] Saved mode %s ',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), finalname)

    def loadClusterModel(self, method, mname):
        finalname = "%s_%s.model" % (method, mname)
        cluster = Clusterer(jobject=serialization.read(os.path.join(self.modelDir, finalname)))
        logger.info('[%s] : [INFO] Loaded clusterer mode %s ',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), finalname)
        return cluster

    def runclustermodel(self, model, method, dataf, temp=True):
        anomalies = []
        try:
            jvm.start()
            data = self.loadData(dataf, temp)
            cluster = self.loadClusterModel(model, method)
            clusterMembership = []
            for inst in data:
                cl = cluster.cluster_instance(inst)
                dist = cluster.distribution_for_instance(inst)
                print ("cluster=" + str(cl) + ", distribution=" + str(dist))
                clusterMembership.append(cl)

            # print data.attribute_by_name('key')
            # print data.num_instances
            # print data.get_instance(3)

            pa = self.calcThreashold(dict(Counter(clusterMembership)), 21)
            for a in pa:
                # print data.get_instance(a).get_value(0)  #todo always set key as first atribute
                anomalies.append(data.get_instance(a).get_value(0))
            print "Detected using %s anomalies at timestamp(s) %s" % (model, str(anomalies))
        except Exception, e:
            print(traceback.format_exc())
        finally:
            jvm.stop()
        return anomalies

    def simpleKMeansTrain(self, dataf, options, mname, temp=True):
        '''
        :param data: -> data to be clustered
        :param options: -> SimpleKMeans options
                      N -> number of clusters
                      A -> Distance function to use (ex: default is "weka.core.EuclideanDistance -R first-last")
                      l -> maximum number of iterations default 500
              num-slots -> number of execution slots, 1 means no parallelism
                      S -> Random number seed (default 10)
              example => ["-N", "10", "-S", "10"]
        :return:
        '''
        try:
            jvm.start()
            data = self.loadData(dataf, temp=True)
            clusterer = Clusterer(classname="weka.clusterers.SimpleKMeans", options=options)
            clusterer.build_clusterer(data)
            print clusterer
            # cluster the data
            for inst in data:
                cl = clusterer.cluster_instance(inst)  # 0-based cluster index
                dist = clusterer.distribution_for_instance(inst)  # cluster membership distribution
                print("cluster=" + str(cl) + ", distribution=" + str(dist))
            self.saveModel(clusterer, 'skm', mname)
        except Exception, e:
            print(traceback.format_exc())
        finally:
            jvm.stop()

    def dbscanTrain(self, dataf, options, mname, temp=True):
        '''
        :param data: -> data to be clustered
        :param options: -> dbscan options
                      E -> epsilon (default = 0.9)
                      M -> minPoints (default = 6)
                      D -> default weka.clusterers.forOPTICSAndDBScan.DataObjects.EuclideanDataObject
                      I -> index (database) used for DBSCAN (default = weka.clusterers.forOPTICSAndDBScan.Databases.SequentialDatabase)
                example => ["-E",  "0.9",  "-M", "6", "-I", "weka.clusterers.forOPTICSAndDBScan.Databases.SequentialDatabase", "-D", "weka.clusterers.forOPTICSAndDBScan.DataObjects.EuclideanDataObject"]
        :return:
        '''

        try:
            jvm.start()
            data = self.loadData(dataf, temp)
            clusterDBSCAN = Clusterer(classname="weka.clusterers.DBSCAN", options=options)
            clusterDBSCAN.build_clusterer(data)
            print clusterDBSCAN
            self.saveModel(clusterDBSCAN, 'dbscan', mname)
            # cluster the data
        except Exception, e:
            print(traceback.format_exc())
        finally:
            jvm.stop()

    def emTrain(self, dataf, options, mname, temp=True):
        '''
        :param data: -> data to be clustered
        :param options: -> EM options
                      I -> number of iterations
                      N -> number of clusters
                      M -> Minimum standard deviation for normal density (default=1.0E-6)
              num-slots -> number of execution slots, 1 means no parallelism
                      S -> random seed (default=100)
                example => ["-I", "1000", "-N", "6", "-X", "10", "-max", "-1", "-ll-cv", "1.0E-6",
                                       "-ll-iter", "1.0E-6", "-M", "1.0E-6", "-num-slots", "1", "-S", "100"]
        :return:
        '''
        try:
            jvm.start()
            data = self.loadData(dataf, temp)
            clusterEM = Clusterer(classname="weka.clusterers.EM",
                              options=options)
            clusterEM.build_clusterer(data)
            print clusterEM
            self.saveModel(clusterEM, 'em', mname, )
        except Exception, e:
            print(traceback.format_exc())
        finally:
            jvm.stop()

    def calcThreashold(self, cInstances, limit):
        minim = min(cInstances, key=cInstances.get)
        maxim = max(cInstances, key=cInstances.get)
        if 100 * float(cInstances[minim]) / float(cInstances[maxim]) > limit:
            return 0
        lminim = []
        nd = cInstances
        # map(nd.pop, [minim])
        for k, v in nd.iteritems():
            if v == cInstances[minim]:
                lminim.append(k)
        lminim.append(minim)
        return list(set(lminim))