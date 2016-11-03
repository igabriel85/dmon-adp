import os
from weka.clusterers import Clusterer
import weka.core.converters as converters
import traceback
import weka.core.jvm as jvm


class dweka:
    def __init__(self, wHeap = '512m'):
        self.wHeap = wHeap
        self.dataDir = os.path.join(os.path.dirname(os.path.abspath('')), 'data')
        self.modelDir = os.path.join(os.path.dirname(os.path.abspath('')), 'models')

    def loadData(self, fName):
        data = converters.load_any_file(os.path.join(self.dataDir, fName))
        return data

    def simpleKMeans(self, data, options):
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
            clusterer = Clusterer(classname="weka.clusterers.SimpleKMeans", options=options)
            clusterer.build_clusterer(data)
            print clusterer
            # cluster the data
            for inst in data:
                cl = clusterer.cluster_instance(inst)  # 0-based cluster index
                dist = clusterer.distribution_for_instance(inst)  # cluster membership distribution
                print("cluster=" + str(cl) + ", distribution=" + str(dist))
        except Exception, e:
            print(traceback.format_exc())
        finally:
            jvm.stop()
        return "kmeans"

    def dbscan(self, data, options):
        '''
        :param data: -> data to be clustered
        :param options: -> SimpleKMeans options
                      E -> epsilon (default = 0.9)
                      M -> minPoints (default = 6)
                      D -> default weka.clusterers.forOPTICSAndDBScan.DataObjects.EuclideanDataObject
                      I -> index (database) used for DBSCAN (default = weka.clusterers.forOPTICSAndDBScan.Databases.SequentialDatabase)
                example => ["-E",  "0.9",  "-M", "6", "-I", "weka.clusterers.forOPTICSAndDBScan.Databases.SequentialDatabase", "-D", "weka.clusterers.forOPTICSAndDBScan.DataObjects.EuclideanDataObject"]
        :return:
        '''

        try:
            jvm.start()
            clusterDBSCAN = Clusterer(classname="weka.clusterers.DBSCAN", options=options)
            clusterDBSCAN.build_clusterer(data)
            print clusterDBSCAN
            # cluster the data
        except Exception, e:
            print(traceback.format_exc())
        finally:
            jvm.stop()

    def em(self, data, options):
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
            clusterEM = Clusterer(classname="weka.clusterers.EM",
                              options=options)
            clusterEM.build_clusterer(data)
            print clusterEM
        except Exception, e:
            print(traceback.format_exc())
        finally:
            jvm.stop()
