import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.ensemble import IsolationForest
from sklearn import metrics
from sklearn.datasets.samples_generator import make_blobs
from sklearn.preprocessing import StandardScaler
import cPickle as pickle
import os
from util import str2Bool
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
        for k, v in settings.iteritems():
            logger.info('[%s] : [INFO] SDBSCAN %s set to %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), k, v)
            print "SDBSCAN %s set to %s" % (k, v)
        sdata = StandardScaler().fit_transform(data)
        try:
            db = DBSCAN(eps=float(settings['eps']), min_samples=int(settings['min_samples']), metric=settings['metric'],
                        algorithm=settings['algorithm'], leaf_size=int(settings['leaf_size']), p=float(settings['p']),
                        n_jobs=int(settings['n_jobs'])).fit(sdata)
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
        if settings['random_state'] == 'None':
            settings['random_state'] = None

        if isinstance(settings['bootstrap'], str):
            settings['bootstrap'] = str2Bool(settings['bootstrap'])

        if isinstance(settings['verbose'], str):
            settings['verbose'] = str2Bool(settings['verbose'])

        if settings['max_samples'] != 'auto':
            settings['max_samples'] = int(settings['max_samples'])
        # print type(settings['max_samples'])
        for k, v in settings.iteritems():
            logger.info('[%s] : [INFO] IsolationForest %s set to %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), k, v)
            print "IsolationForest %s set to %s" % (k, v)
        try:
            clf = IsolationForest(n_estimators=int(settings['n_estimators']), max_samples=settings['max_samples'], contamination=float(settings['contamination']), bootstrap=settings['bootstrap'],
                        max_features=float(settings['max_features']), n_jobs=int(settings['n_jobs']), random_state=settings['random_state'], verbose=settings['verbose'])
        except Exception as inst:
            logger.error('[%s] : [ERROR] Cannot instanciate isolation forest with %s and %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            print "Error while  instanciating isolation forest with %s and %s" % (type(inst), inst.args)
            sys.exit(1)
        # clf = IsolationForest(max_samples=100, random_state=rng)
        # print "*&*&*&& %s" % type(data)
        try:
            clf.fit(data)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Cannot fit isolation forest model with %s and %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            sys.exit(1)
        predict = clf.predict(data)
        print "Anomaly Array:"
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
        anomalieslist = []
        if not smodel:
            dpredict = 0
        else:
            if data.shape[0]:
                if isinstance(smodel, IsolationForest):
                    print "Detected IsolationForest model"
                    print "Contamination -> %s" % smodel.contamination
                    print "Max_Features -> %s" % smodel.max_features
                    print "Max_Samples -> %s" % smodel.max_samples_
                    print "Threashold -> %s " % smodel.threshold_
                    try:
                        dpredict = smodel.predict(data)
                        print "IsolationForest Prediction Array -> %s" %str(dpredict)
                    except Exception as inst:
                        logger.error('[%s] : [ERROR] Error while fitting isolationforest model to event with %s and %s',
                             datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
                        dpredict = 0

                elif isinstance(smodel, DBSCAN):
                    print "Detected DBSCAN model"
                    print "Leaf_zise -> %s" % smodel.leaf_size
                    print "Algorithm -> %s" % smodel.algorithm
                    print "EPS -> %s" % smodel.eps
                    print "Min_Samples -> %s" % smodel.min_samples
                    print "N_jobs -> %s" % smodel.n_jobs
                    try:
                        dpredict = smodel.fit_predict(data)
                    except Exception as inst:
                        logger.error('[%s] : [ERROR] Error while fitting sDBSCAN model to event with %s and %s',
                                     datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst),
                                     inst.args)
                        dpredict = 0
            else:
                dpredict = 0
                logger.warning('[%s] : [WARN] Dataframe empty with shape (%s,%s)',
                             datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(data.shape[0]),
                             str(data.shape[1]))
                print "Empty dataframe received with shape (%s,%s)" % (str(data.shape[0]),
                             str(data.shape[1]))
            print "dpredict type is %s" % (type(dpredict))
            if type(dpredict) is not int:
                anomalyarray = np.argwhere(dpredict == -1)
                for an in anomalyarray:
                    anomalies = {}
                    anomalies['utc'] = int(data.iloc[an[0]]['key'])
                    anomalies['hutc'] = ut2hum(int(data.iloc[an[0]]['key']))
                    anomalieslist.append(anomalies)
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
        print 'Saved %s model at %s' %(method, fpath)

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