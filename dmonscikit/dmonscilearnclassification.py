from sklearn.model_selection import train_test_split
import os
from adplogger import logger
from datetime import datetime
import time
import sys
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier, export_graphviz
from sklearn import model_selection
from sklearn.ensemble import AdaBoostClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN
import cPickle as pickle
from util import str2Bool
import glob
from util import ut2hum



class SciClassification:
    def __init__(self, modelDir, dataDir, settings):
        self.modelDir = modelDir
        self.dataDir = dataDir
        self.settings = settings

    def detect(self, method, model, data):
        smodel = self.__loadClusterModel(method, model)
        anomalieslist = []
        if not smodel:
            dpredict = 0
        else:
            if data.shape[0]:
                if isinstance(smodel, RandomForestClassifier):
                    print "Detected RandomForest model"
                    print "n_estimators -> %s" % smodel.n_estimators
                    print "Criterion -> %s" % smodel.criterion
                    print "Max_Features -> %s" % smodel.max_features
                    print "Max_Depth -> %s" % smodel.max_depth
                    print "Min_sample_split -> %s " % smodel.min_samples_split
                    print "Min_sample_leaf -> %s " % smodel.min_samples_leaf
                    print "Min_weight_fraction_leaf -> %s " % smodel.min_weight_fraction_leaf
                    print "Max_leaf_nodes -> %s " % smodel.max_leaf_nodes
                    print "Min_impurity_split -> %s " % smodel.min_impurity_split
                    print "Bootstrap -> %s " % smodel.bootstrap
                    print "Oob_score -> %s " % smodel.oob_score
                    print "N_jobs -> %s " % smodel.n_jobs
                    print "Random_state -> %s " % smodel.random_state
                    print "Verbose -> %s " % smodel.verbose
                    print "Class_weight -> %s " % smodel.class_weight
                    try:
                        dpredict = smodel.predict(data)
                        print "RandomForest Prediction Array -> %s" %str(dpredict)
                    except Exception as inst:
                        logger.error('[%s] : [ERROR] Error while fitting randomforest model to event with %s and %s',
                             datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
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
                # TODO change so all normal events have tag 0
                anomalyarray = np.argwhere(dpredict != 0)
                for an in anomalyarray:
                    anomalies = {}
                    anomalies['utc'] = int(data.iloc[an[0]]['key'])
                    anomalies['hutc'] = ut2hum(int(data.iloc[an[0]]['key']))
                    anomalieslist.append(anomalies)
        anomaliesDict = {}
        anomaliesDict['anomalies'] = anomalieslist
        logger.info('[%s] : [INFO] Detected anomalies with model %s using method %s are -> %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), model, method,
                    str(anomaliesDict))
        return anomaliesDict

    def score(self):
        return True

    def compare(self):
        return True

    def crossvalid(self):
        return True

    def naiveBayes(self):
        return True

    def adaBoost(self, settings, data=None, dropna=True):
        df = self.__loadData(settings, data, dropna)
        features = df.columns[:-1]
        X = df[features]
        y = df.iloc[:, -1].values
        seed = 7
        num_trees = 500
        kfold = model_selection.KFold(n_splits=10, random_state=seed)
        print kfold
        model = AdaBoostClassifier(n_estimators=num_trees, random_state=seed)
        results = model_selection.cross_val_score(model, X, y, cv=kfold)
        model.fit(X,y)
        print results.mean()
        print model.score(X, y)
        return True

    def neuralNet(self):
        return True

    def decisionTree(self, settings, data=None, dropna=True):
        if "splitter" not in settings:
            print "Received settings for Decision Tree are %s invalid!" % str(settings)
            logger.error('[%s] : [ERROR] Received settings for Decision Tree %s are invalid',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(settings))
            sys.exit(1)

        try:
            mname = settings['export']
        except:
            mname = 'default'

        df = self.__loadData(settings, data, dropna)
        features = df.columns[:-1]
        X = df[features]
        y = df.iloc[:, -1].values

        # dt = DecisionTreeClassifier(min_samples_split=20, random_state=99)
        dt = DecisionTreeClassifier(criterion=settings["criterion"], splitter=settings["splitter"],
                                    max_features=settings["max_features"], max_depth=settings["max_depth"],
                                    min_samples_split=settings["min_sample_split"],
                                    min_weight_fraction_leaf=settings["min_weight_faction_leaf"], random_state=settings["random_state"])
        if settings['validratio']:
            trainSize = 1.0 - settings['validratio']
            print "Random forest training to validation ratio set to: %s" % str(settings['validratio'])
            logger.info('[%s] : [INFO] Random forest training to validation ratio set to: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(settings['validratio']))
            d_train, d_test, f_train, f_test = self.__dataSplit(X, y, testSize=settings['validratio'], trainSize=trainSize)
            dt.fit(d_train, f_train)
            predict = dt.predict(d_train)
            print "Prediction for Decision Tree Training:"
            print predict

            print "Actual labels of training set:"
            print f_train

            predProb = dt.predict_proba(d_train)
            print "Prediction probabilities for Decision Tree Training:"
            print predProb

            score = dt.score(d_train, f_train)
            print "Decision Tree Training Score: %s" % str(score)
            logger.info('[%s] : [INFO] Decision Tree training score: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(score))

            feature_imp = list(zip(d_train, dt.feature_importances_))
            print "Feature importance Decision Tree Training: "
            print list(zip(d_train, dt.feature_importances_))
            logger.info('[%s] : [INFO] Decision Tree feature importance: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(feature_imp))

            pred_valid = dt.predict(d_test)
            print "Decision Tree Validation set prediction: "
            print pred_valid
            print "Actual values of validation set: "
            print d_test
            score_valid = dt.score(d_test, f_test)
            print "Decision Tree validation set score: %s" % str(score_valid)
            logger.info('[%s] : [INFO] Random forest validation score: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(score_valid))
        else:
            dt.fit(X, y)
            predict = dt.predict(X)
            print "Prediction for Decision Tree Training:"
            print predict

            print "Actual labels of training set:"
            print y

            predProb = dt.predict_proba(X)
            print "Prediction probabilities for Decision Tree Training:"
            print predProb

            score = dt.score(X, y)
            print "Decision Tree Training Score: %s" % str(score)

            print "Feature importance Decision Tree Training: "
            print list(zip(X, dt.feature_importances_))
            if settings['validation'] is None:
                logger.info('[%s] : [INFO] Validation is set to None',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                # return True
            else:
                vfile = os.path.join(self.dataDir, settings['validation'])
                logger.info('[%s] : [INFO] Validation data file is set to %s',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(vfile))
                if not os.path.isfile(vfile):
                    print "Validation file %s not found" % vfile
                    logger.error('[%s] : [ERROR] Validation file %s not found',
                                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(vfile))
                else:
                    df_valid = pd.read_csv(vfile)
                    if dropna:
                        df_valid = df_valid.dropna()
                    features_valid = df_valid.columns[:-1]
                    X_valid = df_valid[features_valid]
                    y_valid = df_valid.iloc[:, -1].values
                    pred_valid = dt.predict(X_valid)
                    print "Decision Tree Validation set prediction: "
                    print pred_valid
                    print "Actual values of validation set: "
                    print y_valid
                    score_valid = dt.score(X_valid, y_valid)
                    print "Random Decision Tree set score: %s" % str(score_valid)
                    # return True
        self.__serializemodel(dt, 'DecisionTree', mname)
        return dt

    def randomForest(self, settings, data=None, dropna=True):
        if "min_weight_faction_leaf" not in settings:
            print "Received settings for RandomForest are %s invalid!" % str(settings)
            logger.error('[%s] : [ERROR] Received settings for RandomForest %s are invalid',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(settings))
            sys.exit(1)

        try:
            mname = settings['export']
        except:
            mname = 'default'
        df = self.__loadData(settings, data, dropna)
        features = df.columns[:-1]
        X = df[features]
        y = df.iloc[:, -1].values

        # clf = RandomForestClassifier(max_depth=5, n_estimators=10, max_features=1, n_jobs=2)
        clf = RandomForestClassifier(n_estimators=settings["n_estimators"], criterion=settings["criterion"],
                                     max_features=settings["max_features"], max_depth=settings["max_depth"],
                                     min_samples_split=settings["min_sample_split"],
                                     min_samples_leaf=settings["min_sample_leaf"],
                                     min_weight_fraction_leaf=settings["min_weight_faction_leaf"],
                                     bootstrap=settings["bootstrap"],
                                     n_jobs=settings["n_jobs"],
                                     random_state=settings["random_state"], verbose=settings["verbose"])

        if settings['validratio']:
            trainSize = 1.0 - settings['validratio']
            print "Random forest training to validation ratio set to: %s" % str(settings['validratio'])
            logger.info('[%s] : [INFO] Random forest training to validation ratio set to: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(settings['validratio']))
            d_train, d_test, f_train, f_test = self.__dataSplit(X, y, testSize=settings['validratio'], trainSize=trainSize)
            clf.fit(d_train, f_train)
            predict = clf.predict(d_train)
            print "Prediction for Random Forest Training:"
            print predict

            print "Actual labels of training set:"
            print f_train

            predProb = clf.predict_proba(d_train)
            print "Prediction probabilities for Random Forest Training:"
            print predProb

            score = clf.score(d_train, f_train)
            print "Random Forest Training Score: %s" % str(score)
            logger.info('[%s] : [INFO] Random forest training score: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(score))

            feature_imp = list(zip(d_train, clf.feature_importances_))
            print "Feature importance Random Forest Training: "
            print list(zip(d_train, clf.feature_importances_))
            logger.info('[%s] : [INFO] Random forest feature importance: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(feature_imp))

            pred_valid = clf.predict(d_test)
            print "Random Forest Validation set prediction: "
            print pred_valid
            print "Actual values of validation set: "
            print d_test
            score_valid = clf.score(d_test, f_test)
            print "Random Forest validation set score: %s" % str(score_valid)
            logger.info('[%s] : [INFO] Random forest validation score: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(score_valid))
        else:
            clf.fit(X, y)
            predict = clf.predict(X)
            print "Prediction for Random Forest Training:"
            print predict

            print "Actual labels of training set:"
            print y

            predProb = clf.predict_proba(X)
            print "Prediction probabilities for Random Forest Training:"
            print predProb

            score = clf.score(X, y)
            print "Random Forest Training Score: %s" % str(score)

            print "Feature importance Random Forest Training: "
            print list(zip(X, clf.feature_importances_))
            if settings['validation'] is None:
                logger.info('[%s] : [INFO] Validation is set to None',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                # return True
            else:
                vfile = os.path.join(self.dataDir, settings['validation'])
                logger.info('[%s] : [INFO] Validation data file is set to %s',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(vfile))
                if not os.path.isfile(vfile):
                    print "Validation file %s not found" % vfile
                    logger.error('[%s] : [ERROR] Validation file %s not found',
                                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(vfile))
                else:
                    df_valid = pd.read_csv(vfile)
                    if dropna:
                        df_valid = df_valid.dropna()
                    features_valid = df_valid.columns[:-1]
                    X_valid = df_valid[features_valid]
                    y_valid = df_valid.iloc[:, -1].values
                    pred_valid = clf.predict(X_valid)
                    print "Random Forest Validation set prediction: "
                    print pred_valid
                    print "Actual values of validation set: "
                    print y_valid
                    score_valid = clf.score(X_valid, y_valid)
                    print "Random Forest validation set score: %s" % str(score_valid)
                    # return True
        self.__serializemodel(clf, 'RandomForest', mname)
        return clf

    def trainingDataGen(self, settings, data=None, dropna=True):
        print "Starting training data generation ...."
        logger.info('[%s] : [INFO] Starting training data generation ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        df = self.__loadData(settings, data, dropna)

        if 'n_estimators' not in settings.keys():
            settings['n_estimators'] = 100
        if 'max_samples' not in settings.keys():
            settings['max_samples'] = 'auto'
        if 'contamination' not in settings.keys():
            settings['contamination'] = 0.1
        if 'bootstrap' not in settings.keys():
            settings['bootstrap'] = True
        if 'max_features' not in settings.keys():
            settings['max_features'] = 1.0
        if 'n_jobs' not in settings.keys():
            settings['n_jobs'] = 1
        if 'random_state' not in settings.keys():
            settings['random_state'] = None
        if 'verbose' not in settings.keys():
            settings['verbose'] = 0

        if settings['random_state'] == 'None':
            settings['random_state'] = None

        if isinstance(settings['bootstrap'], str):
            settings['bootstrap'] = str2Bool(settings['bootstrap'])

        if isinstance(settings['verbose'], str):
            settings['verbose'] = str2Bool(settings['verbose'])

        if settings['max_samples'] != 'auto':
            settings['max_samples'] = int(settings['max_samples'])

        if isinstance(settings['max_features'], str):
            settings["max_features"] = 1.0
        # print type(settings['max_samples'])
        allowedIso = ['n_estimators', 'max_samples', 'contamination', 'bootstrap', 'max_features', 'n_jobs',
                   'random_state', 'verbose']
        for k, v in settings.iteritems():
            if k in allowedIso:
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

        clf.fit(df)
        pred = clf.predict(df)
        # print data.shape
        # print len(pred)
        print "Prediction for IsolationForest:"
        print pred
        anomalies = np.argwhere(pred == -1)
        normal = np.argwhere(pred == 1)

        print "Number of anomalies detected: %s" % str(len(anomalies))

        logger.info('[%s] : [INFO] Number of anomalies detected: %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(len(anomalies)))

        # Generate anomalydataframe
        slist = []
        for an in anomalies:
            slist.append(df.iloc[an[0]])
        anomalyFrame = pd.DataFrame(slist)
        anomalyFrame.set_index('key', inplace=True)
        if settings["checkpoint"]:
            logger.info('[%s] : [INFO] Anomalies checkpointed.',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(len(anomalies)))
            anomalyFrame.to_csv(os.path.join(self.dataDir, 'AnomalyFrame.csv'))

        print "Normalizing data ..."
        X = StandardScaler().fit_transform(anomalyFrame)

        print "Started Anomaly clustering ..."
        if 'eps' not in settings.keys():
            settings['eps'] = 0.9
        if 'min_samples' not in settings.keys():
            settings['min_samples'] = 40
        if 'metric' not in settings.keys():
            settings['metric'] = 'euclidean'
        if 'algorithm' not in settings.keys():
            settings['algorithm'] = 'auto'
        if 'leaf_size' not in settings.keys():
            settings['leaf_size'] = 30
        if 'p' not in settings.keys():
            settings['p'] = 0.2
        if 'n_jobs' not in settings.keys():
            settings['n_jobs'] = 1

        allowedDB = ['eps', 'min_samples', 'metric', 'algorithm', 'leaf_size', 'p', 'n_jobs']
        for k, v in settings.iteritems():
            if k in allowedDB:
                logger.info('[%s] : [INFO] SDBSCAN %s set to %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), k, v)
                print "SDBSCAN %s set to %s" % (k, v)

        # db = DBSCAN(eps=0.9, min_samples=40).fit(X)
        try:
            db = DBSCAN(eps=float(settings['eps']), min_samples=int(settings['min_samples']), metric=settings['metric'],
                        algorithm=settings['algorithm'], leaf_size=int(settings['leaf_size']), p=float(settings['p']),
                        n_jobs=int(settings['n_jobs'])).fit(X)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Cannot instanciate sDBSCAN with %s and %s',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
            print "Error while  instanciating sDBSCAN with %s and %s" % (type(inst), inst.args)
            sys.exit(1)
        print "Finshed  Anomaly clustering."
        labels = db.labels_

        # print len(labels)
        # print anomalyFrame.shape
        # print X[labels == -1]
        # Number of clusters in labels, ignoring noise if present.
        n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)

        print 'Estimated number of clusters: %d' % n_clusters_
        logger.info('[%s] : [INFO] Estimated number of clusters: %d',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), n_clusters_)

        # Add labels to target column
        print "Adding labels to data ..."
        anomalyFrame['Target'] = labels

        # Remove noise from data
        print "Removing noise from data ..."
        data_labeled = anomalyFrame[anomalyFrame["Target"] != -1]
        if settings["checkpoint"]:
            data_labeled.to_csv(os.path.join(self.dataDir, 'AnomalyFrame_Labeled.csv'))
        print "Finished training data generation"
        return data_labeled

    def __loadData(self, settings, data=None, dropna=True):
        if settings["checkpoint"]:
            dfile = os.path.join(self.dataDir, settings['training'])
            logger.info('[%s] : [INFO] Data file is set to %s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(dfile))
            if not os.path.isfile(dfile):
                print "Training file %s not found" % dfile
                logger.error('[%s] : [ERROR] Training file %s not found',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(dfile))
                sys.exit(1)
            else:
                df = pd.read_csv(dfile)
        else:
            df = data
        if dropna:
            df = df.dropna()
        return df

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
        print 'Saved %s model at %s' % (method, fpath)

    def __normalize(self):
        return True

    def __dataSplit(self, X, y, trainSize=None, testSize=None):
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=testSize, train_size=trainSize)
        return X_train, X_test, y_train, y_test