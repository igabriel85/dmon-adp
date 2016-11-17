import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.ensemble import IsolationForest
from sklearn import metrics
from sklearn.datasets.samples_generator import make_blobs
from sklearn.preprocessing import StandardScaler
import cPickle as pickle
import os
import pandas as pd
from adplogger import logger
from datetime import datetime
import time
import sys
import glob
from util import ut2hum


class SciCluster:
    def __init__(self, modelDir):
        self.modelDir = modelDir

    def sdbscanTrain(self, settings, mname, data):
        '''
        :param data: -> dataframe with data
        :param settings: -> settings dictionary
        :param mname: -> name of serialized clusterer
        :return: -> clusterer
        :example settings: -> {eps:0.9, min_samples:10, metric:'euclidean' ,
        algorithm:'auto, leaf_size:30, p:0.2, n_jobs:1}
        '''
        sdata = StandardScaler().fit_transform(data)
        try:
            db = DBSCAN(eps=settings['eps'], min_samples=settings['min_samples'], metric=settings['metric'],
                        algorithm=settings['algorithm'], leaf_size=settings['leaf_size'], p=settings['p'],
                        n_jobs=settings['n_jobs']).fit(sdata)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Cannot instanciate sDBSCAN with %s and %s',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            print "Error while  instanciating sDBSCAN with %s and %s" % (type(inst), inst.args)
            sys.exit(1)
        labels = db.labels_
        print labels
        n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
        print'Estimated number of clusters: %d' % n_clusters_
        self.__serializemodel(db, 'sdbscan', mname)
        return db

    def isolationForest(self, settings, mname, data):
        '''
        :param settings: -> settings dictionary
        :param mname: -> name of serialized cluster
        :return: -> isolation forest instance
        :example settings: -> {n_estimators:100, max_samples:100, contamination:0.1, bootstrap:False,
                        max_features:1.0, n_jobs:1, random_state:None, verbose:0}
        '''
        # rng = np.random.RandomState(42)
        for k, v in settings.iteritems():
            print "%s->%s" % (k, v)
        try:
            clf = IsolationForest(n_estimators=settings['n_estimators'], max_samples=settings['max_samples'], contamination=settings['contamination'], bootstrap=settings['bootstrap'],
                        max_features=settings['max_features'], n_jobs=settings['n_jobs'], random_state=settings['random_state'], verbose=settings['verbose'])
        except Exception as inst:
            logger.error('[%s] : [ERROR] Cannot instanciate isolation forest with %s and %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            print "Error while  instanciating isolation forest with %s and %s" % (type(inst), inst.args)
            sys.exit(1)
        # clf = IsolationForest(max_samples=100, random_state=rng)
        print "*&*&*&& %s" % type(data)
        clf.fit(data)
        print clf.contamination
        predict = clf.predict(data)
        print predict
        self.__serializemodel(clf, 'isoforest', mname)
        return clf

    def detect(self, method, model, data):
        '''
        :param method: -> method name
        :param model: -> trained clusterer
        :param data: -> dataframe with data
        :return: -> dictionary that contains the list of anomalous timestamps
        '''
        smodel = self.__loadClusterModel(method, model)
        if not smodel:
            pass
        else:
            anomalieslist = []
            if isinstance(smodel, IsolationForest):
                print "Detected IsolationForest model"
                print "Contamination -> %s" % smodel.contamination
                print "Max_Features -> %s" % smodel.max_features
                print "Max_Samples -> %s" % smodel.max_samples_
                print "Threashold -> %s " % smodel.threshold_
                predict = smodel.predict(data)

            elif isinstance(smodel, DBSCAN):
                print "Detected DBSCAN model"
                print "Leaf_zise -> %s" % smodel.leaf_size
                print "Algorithm -> %s" % smodel.algorithm
                print "EPS -> %s" % smodel.eps
                print "Min_Samples -> %s" % smodel.min_samples
                print "N_jobs -> %s" % smodel.n_jobs
                predict = smodel.fit_predict(data)

            anomalyarray = np.argwhere(predict == -1)
            for an in anomalyarray:
                anomalies = {}
                anomalies['utc'] = int(data.iloc[an[0]]['key'])
                anomalies['hutc'] = ut2hum(int(data.iloc[an[0]]['key']))
                anomalieslist.append(anomalies)
            # print anomalyarray
            # {method: "<method_name>",
            #  qInterval: "<qInterval>",
            #  anomalies: [
            #      {"utc": "<utc_time>",
            #       "hutc": "<hutctime>"},
            #      {"utc": "<utc_time>",
            #       "hutc": "<hutctime>"}]
            #  }
        anomaliesDict = {}
        anomaliesDict['anomalies'] = anomalieslist
        logger.info('[%s] : [INFO] Detected anomalies with model %s using method %s are -> %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), model, method, str(anomaliesDict))
        return anomaliesDict

    def __serializemodel(self, model, method, mname):
        '''
        :param model: -> model
        :param method: -> method name
        :param mname: -> name to be used for saved model
        :result: -> Serializez current clusterer/classifier
        '''
        fpath = "%s_%s.pkl" % (method, mname)
        fname = os.path.join(self.modelDir, fpath)
        pickle.dump(model, open(fname, "wb"))
        print 'Saved sdbscan model at %s' % fpath

    def __loadClusterModel(self, method, model):
        '''
        :param method: -> method name
        :param model: -> model name
        :return: -> instance of serialized object
        '''
        lmodel = glob.glob(os.path.join(self.modelDir, ("%s_%s.pkl" % (method, model))))
        if not lmodel:
            print "No %s model with the name %s found" %(method, model)
            logger.warning('[%s] : [WARN] No %s model with the name %s found',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), method, model)
            return 0
        else:
            smodel = pickle.load(open(lmodel[0], "rb"))
            print "Succesfully loaded %s model with the name %s" % (method, model)
            logger.info('[%s] : [INFO] Succesfully loaded %s model with the name %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), method, model)
            return smodel
