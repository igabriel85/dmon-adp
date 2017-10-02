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
from sklearn.neural_network import MLPClassifier
from sklearn import model_selection
from sklearn.ensemble import AdaBoostClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.grid_search import GridSearchCV
from sklearn.cluster import DBSCAN
import cPickle as pickle
from util import str2Bool
import glob
from util import ut2hum
import itertools


class SciClassification:
    def __init__(self, modelDir, dataDir, checkpoint, export, training, validation, validratio, compare):
        self.modelDir = modelDir
        self.dataDir = dataDir
        self.checkpoint = checkpoint
        self.export = export
        self.training = training
        self.validation = validation
        self.validratio = validratio
        self.compare = compare

    def detect(self, method, model, data):
        smodel = self.__loadClassificationModel(method, model)
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
                elif isinstance(smodel, AdaBoostClassifier):
                    print "Detected AdaBoost model"
                    print "base_estimator -> %s" % smodel.base_estimator
                    print "n_estimators -> %s" % smodel.n_estimators
                    print "Learning_rate -> %s" % smodel.learning_rate
                    print "Algorithm -> %s" % smodel.algorithm
                    print "Random State -> %s" % smodel.random_state
                    try:
                        dpredict = smodel.predict(self.df)
                        print "AdaBoost Prediction Array -> %s" % str(dpredict)
                    except Exception as inst:
                        logger.error('[%s] : [ERROR] Error while fitting AdaBoost model to event with %s and %s',
                             datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args)
                        sys.exit(1)
                elif isinstance(smodel, DecisionTreeClassifier):
                    print "Detected Decision Tree model"
                    print "Criterion -> %s" % smodel.criterion
                    print "Spliter -> %s" % smodel.splitter
                    print "Max_Depth -> %s" % smodel.max_depth
                    print "Min_sample_split -> %s " % smodel.min_samples_split
                    print "Min_sample_leaf -> %s " % smodel.min_samples_leaf
                    print "Min_weight_fraction_leaf -> %s " % smodel.min_weight_fraction_leaf
                    print "Max_Features -> %s" % smodel.max_features
                    print "Random_state -> %s " % smodel.random_state
                    print "Max_leaf_nodes -> %s " % smodel.max_leaf_nodes
                    print "Min_impurity_split -> %s " % smodel.min_impurity_split
                    print "Class_weight -> %s " % smodel.class_weight
                    try:
                        dpredict = smodel.predict(self.df)
                        print "Decision Tree Prediction Array -> %s" % str(dpredict)
                    except Exception as inst:
                        logger.error('[%s] : [ERROR] Error while fitting Decision Tree model to event with %s and %s',
                                     datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst),
                                     inst.args)
                        sys.exit(1)

                elif isinstance(smodel, MLPClassifier):
                    print "Detected Neural Network model"
                    print "Hidden Layer size -> %s" % str(smodel.hidden_layer_sizes)
                    print "Activation -> %s" % smodel.activation
                    print "Solver -> %s" % smodel.solver
                    print "Alpha -> %s" % smodel.alpha
                    print "Batch Size -> %s" % smodel.batch_size
                    print "Learning rate -> %s" % smodel.learning_rate
                    print "Max Iterations -> %s" % smodel.max_iter
                    print "Shuffle -> %s" % smodel.shuffle
                    print "Momentum -> %s" % smodel.momentum
                    print "Epsilon -> %s" % smodel.epsilon
                    try:
                        dpredict = smodel.predict(self.df)
                        print "MLP Prediction Array -> %s" % str(dpredict)
                    except Exception as inst:
                        logger.error('[%s] : [ERROR] Error while fitting MLP model to event with %s and %s',
                                     datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst),
                                     inst.args)
                        sys.exit(1)
                else:
                    logger.error('[%s] : [ERROR] Unsuported model loaded: %s!',
                             datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(smodel))
                    sys.exit(1)
            else:
                dpredict = 0
                logger.warning('[%s] : [WARN] Dataframe empty with shape (%s,%s)',
                             datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(data.shape[0]),
                             str(data.shape[1]))
                print "Empty dataframe received with shape (%s,%s)" % (str(data.shape[0]),
                             str(data.shape[1]))
            print "dpredict type is %s" % (type(dpredict))
            if type(dpredict) is not int:
                data['AType'] = dpredict
                for index, row in data.iterrows():
                    anomalies = {}
                    if row['AType'] != 0:
                        print index
                        print data.get_value(index, 'AType')
                        anomalies['utc'] = int(index)
                        anomalies['hutc'] = ut2hum(int(index))
                        anomalies['anomaly_type'] = data.get_value(index, 'AType')
                        anomalieslist.append(anomalies)
        anomaliesDict = {}
        anomaliesDict['anomalies'] = anomalieslist
        logger.info('[%s] : [INFO] Detected anomalies with model %s using method %s are -> %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), model, method,
                    str(anomaliesDict))
        return anomaliesDict

    def score(self, model, X, y):
        return model.score(X, y)

    def compare(self, modelList, X, y):
        scores = []
        for model in modelList:
            scores.append(model.score(X,y))
        logger.info('[%s] : [INFO] Best performing model score is -> %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), max(scores))
        # for a, b in itertools.combinations(modelList, 2):
        #     a.score(X, y)
        #     b.score(X, y)
        return modelList.index(max(scores))

    def crossvalid(self, model, X, y, kfold):
        return model_selection.cross_val_score(model, X, y, cv=kfold)

    def naiveBayes(self):
        return True

    def adaBoost(self, settings, data=None, dropna=True):
        if "n_estimators" not in settings:
            print "Received settings for Ada Boost are %s invalid!" % str(settings)
            logger.error('[%s] : [ERROR] Received settings for Decision Tree %s are invalid',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(settings))
            sys.exit(1)
        dtallowedSettings = ["n_estimators", "learning_rate"]
        for k, v in settings.iteritems():
            if k in dtallowedSettings:
                logger.info('[%s] : [INFO] Ada Boost %s set to %s',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), k, v)
                print "Ada Boost %s set to %s" % (k, v)

        if not isinstance(self.export, str):
            mname = 'default'
        else:
            mname = self.export
        df = self.__loadData(data, dropna)
        features = df.columns[:-1]
        X = df[features]
        y = df.iloc[:, -1].values
        seed = 7
        # num_trees = 500
        kfold = model_selection.KFold(n_splits=10, random_state=seed)
        print kfold
        ad = AdaBoostClassifier(n_estimators=settings['n_estimators'], learning_rate=settings['learning_rate'],
                                   random_state=seed)
        if self.validratio:
            trainSize = 1.0 - self.validratio
            print "Decision Tree training to validation ratio set to: %s" % str(self.validratio)
            logger.info('[%s] : [INFO] Ada Boost training to validation ratio set to: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(self.validratio))
            d_train, d_test, f_train, f_test = self.__dataSplit(X, y, testSize=self.validratio, trainSize=trainSize)
            ad.fit(d_train, f_train)
            predict = ad.predict(d_train)
            print "Prediction for Ada Boost Training:"
            print predict

            print "Actual labels of training set:"
            print f_train

            predProb = ad.predict_proba(d_train)
            print "Prediction probabilities for Ada Boost Training:"
            print predProb

            score = ad.score(d_train, f_train)
            print "Ada Boost Training Score: %s" % str(score)
            logger.info('[%s] : [INFO] Ada Boost training score: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(score))

            feature_imp = list(zip(d_train, ad.feature_importances_))
            print "Feature importance Ada Boost Training: "
            print list(zip(d_train, ad.feature_importances_))
            logger.info('[%s] : [INFO] Ada Boost feature importance: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(feature_imp))

            pred_valid = ad.predict(d_test)
            print "Ada Boost Validation set prediction: "
            print pred_valid
            print "Actual values of validation set: "
            print d_test
            score_valid = ad.score(d_test, f_test)
            print "Ada Boost validation set score: %s" % str(score_valid)
            logger.info('[%s] : [INFO] Ada Boost validation score: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(score_valid))
        else:
            ad.fit(X, y)
            predict = ad.predict(X)
            print "Prediction for Ada Boost Training:"
            print predict

            print "Actual labels of training set:"
            print y

            predProb = ad.predict_proba(X)
            print "Prediction probabilities for Ada Boost Training:"
            print predProb

            score = ad.score(X, y)
            print "Ada Boost Training Score: %s" % str(score)

            fimp = list(zip(X, ad.feature_importances_))
            print "Feature importance Ada Boost Training: "
            print fimp
            dfimp = dict(fimp)
            dfimp = pd.DataFrame(dfimp.items(), columns=['Metric', 'Importance'])
            sdfimp = dfimp.sort('Importance', ascending=False)
            dfimpCsv = 'Feature_Importance_%s.csv' % mname
            sdfimp.to_csv(os.path.join(self.modelDir, dfimpCsv))
            if self.validation is None:
                logger.info('[%s] : [INFO] Validation is set to None',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                # return True
            else:
                vfile = os.path.join(self.dataDir, self.validation)
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
                    pred_valid = ad.predict(X_valid)
                    print "Ada Boost Validation set prediction: "
                    print pred_valid
                    print "Actual values of validation set: "
                    print y_valid
                    score_valid = ad.score(X_valid, y_valid)
                    print "Ada Boost set score: %s" % str(score_valid)
                    # return True
        self.__serializemodel(ad, 'DecisionTree', mname)
        return ad

    def neuralNet(self, settings, data=None, dropna=True):
        if "activation" not in settings:
            print "Received settings for Neural Networks are %s invalid!" % str(settings)
            logger.error('[%s] : [ERROR] Received settings for Neural Networks %s are invalid',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(settings))
            sys.exit(1)

        rfallowedSettings = ["max_iter", "activation", "solver", "batch_size", "learning_rate",
                             "momentum", "alpha"]

        for k, v in settings.iteritems():
            if k in rfallowedSettings:
                logger.info('[%s] : [INFO] Neural Network %s set to %s',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), k, v)
                print "Neural Network %s set to %s" % (k, v)

        if not isinstance(self.export, str):
            mname = 'default'
        else:
            mname = self.export

        df = self.__loadData(data, dropna)
        features = df.columns[:-1]
        X = df[features]
        y = df.iloc[:, -1].values

        mlp = MLPClassifier(hidden_layer_sizes=(50, 20), max_iter=settings['max_iter'],
                            activation=settings['activation'],
                            solver=settings['solver'], batch_size=settings['batch_size'],
                            learning_rate=settings['learning_rate'], momentum=settings['momentum'],
                            alpha=settings['alpha'])

        if self.validratio:
            trainSize = 1.0 - self.validratio
            print "Neural Network training to validation ratio set to: %s" % str(self.validratio)
            logger.info('[%s] : [INFO] Neural Netowork training to validation ratio set to: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(self.validratio))
            d_train, d_test, f_train, f_test = self.__dataSplit(X, y, testSize=self.validratio, trainSize=trainSize)
            mlp.fit(d_train, f_train)
            predict = mlp.predict(d_train)
            print "Prediction for Neural Network Training:"
            print predict

            print "Actual labels of training set:"
            print f_train

            predProb = mlp.predict_proba(d_train)
            print "Prediction probabilities for Neural Network Training:"
            print predProb

            score = mlp.score(d_train, f_train)
            print "Neural Network Training Score: %s" % str(score)
            logger.info('[%s] : [INFO] Neural Network training score: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(score))
            pred_valid = mlp.predict(d_test)
            print "Neural Network Validation set prediction: "
            print pred_valid
            print "Actual values of validation set: "
            print d_test
            score_valid = mlp.score(d_test, f_test)
            print "Neural Network validation set score: %s" % str(score_valid)
            logger.info('[%s] : [INFO] Random forest validation score: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(score_valid))
        else:
            mlp.fit(X, y)
            predict = mlp.predict(X)
            print "Prediction for Neural Network Training:"
            print predict

            print "Actual labels of training set:"
            print y

            predProb = mlp.predict_proba(X)
            print "Prediction probabilities for Neural Network Training:"
            print predProb

            score = mlp.score(X, y)
            print "Random Forest Training Score: %s" % str(score)

            if self.validation is None:
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
                    pred_valid = mlp.predict(X_valid)
                    print "Neural Network Validation set prediction: "
                    print pred_valid
                    print "Actual values of validation set: "
                    print y_valid
                    score_valid = mlp.score(X_valid, y_valid)
                    print "Neural Network validation set score: %s" % str(score_valid)
                    # return True
        self.__serializemodel(mlp, 'RandomForest', mname)
        return mlp

    def decisionTree(self, settings, data=None, dropna=True):
        if "splitter" not in settings:
            print "Received settings for Decision Tree are %s invalid!" % str(settings)
            logger.error('[%s] : [ERROR] Received settings for Decision Tree %s are invalid',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(settings))
            sys.exit(1)

        if settings['random_state'] == 'None':
            settings['random_state'] = None
        else:
            settings['random_state'] = int(settings['random_state'])

        if settings['max_depth'] == 'None':
            max_depth = None
        else:
            max_depth = int(settings['max_depth'])

        if settings['max_features'] == 'auto':
            max_features = settings['max_features']
        else:
            max_features = int(settings['max_features'])

        dtallowedSettings = ["criterion", "splitter", "max_features", "max_depth",
                             "min_weight_faction_leaf", "random_state"]
        for k, v in settings.iteritems():
            if k in dtallowedSettings:
                logger.info('[%s] : [INFO] DecisionTree %s set to %s',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), k, v)
                print "DecisionTree %s set to %s" % (k, v)

        if not isinstance(self.export, str):
            mname = 'default'
        else:
            mname = self.export

        df = self.__loadData(data, dropna)
        features = df.columns[:-1]
        X = df[features]
        y = df.iloc[:, -1].values

        # dt = DecisionTreeClassifier(min_samples_split=20, random_state=99)
        dt = DecisionTreeClassifier(criterion=settings["criterion"], splitter=settings["splitter"],
                                    max_features=max_features, max_depth=max_depth,
                                    min_samples_split=float(settings["min_sample_split"]),
                                    min_weight_fraction_leaf=float(settings["min_weight_faction_leaf"]), random_state=settings["random_state"])
        if self.validratio:
            trainSize = 1.0 - self.validratio
            print "Decision Tree training to validation ratio set to: %s" % str(self.validratio)
            logger.info('[%s] : [INFO] Random forest training to validation ratio set to: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(self.validratio))
            d_train, d_test, f_train, f_test = self.__dataSplit(X, y, testSize=self.validratio, trainSize=trainSize)
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

            fimp = list(zip(X, dt.feature_importances_))
            print "Feature importance Random Forest Training: "
            print fimp
            dfimp = dict(fimp)
            dfimp = pd.DataFrame(dfimp.items(), columns=['Metric', 'Importance'])
            sdfimp = dfimp.sort('Importance', ascending=False)
            dfimpCsv = 'Feature_Importance_%s.csv' % mname
            sdfimp.to_csv(os.path.join(self.modelDir, dfimpCsv))
            if self.validation is None:
                logger.info('[%s] : [INFO] Validation is set to None',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                # return True
            else:
                vfile = os.path.join(self.dataDir, self.validation)
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

        if settings['random_state'] == 'None':
            settings['random_state'] = None
        else:
            settings['random_state'] = int(settings['random_state'])

        if settings['max_depth'] == 'None':
            max_depth = None
        else:
            max_depth = int(settings['max_depth'])

        if isinstance(settings['bootstrap'], str):
            settings['iso_bootstrap'] = str2Bool(settings['bootstrap'])

        rfallowedSettings = ["n_estimators", "criterion", "max_features", "max_depth", "min_sample_split",
                             "min_sample_leaf", "min_weight_faction_leaf", "min_impurity_split", "bootstrap", "n_jobs",
                             "random_state", "verbose"]
        for k, v in settings.iteritems():
            if k in rfallowedSettings:
                logger.info('[%s] : [INFO] RandomForest %s set to %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), k, v)
                print "RandomForest %s set to %s" % (k, v)

        if not isinstance(self.export, str):
            mname = 'default'
        else:
            mname = self.export

        df = self.__loadData(data, dropna)
        features = df.columns[:-1]
        X = df[features]
        y = df.iloc[:, -1].values

        # clf = RandomForestClassifier(max_depth=5, n_estimators=10, max_features=1, n_jobs=2)
        clf = RandomForestClassifier(n_estimators=int(settings["n_estimators"]), criterion=settings["criterion"],
                                     max_features=settings["max_features"], max_depth=max_depth,
                                     min_samples_split=int(settings["min_sample_split"]),
                                     min_samples_leaf=int(settings["min_sample_leaf"]),
                                     min_weight_fraction_leaf=int(settings["min_weight_faction_leaf"]),
                                     bootstrap=settings["bootstrap"],
                                     n_jobs=int(settings["n_jobs"]),
                                     random_state=settings["random_state"], verbose=int(settings["verbose"]))

        if self.validratio:
            trainSize = 1.0 - self.validratio
            print "Random forest training to validation ratio set to: %s" % str(self.validratio)
            logger.info('[%s] : [INFO] Random forest training to validation ratio set to: %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(self.validratio))
            d_train, d_test, f_train, f_test = self.__dataSplit(X, y, testSize=self.validratio, trainSize=trainSize)
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

            fimp = list(zip(X, clf.feature_importances_))
            print "Feature importance Random Forest Training: "
            print fimp
            dfimp = dict(fimp)
            dfimp = pd.DataFrame(dfimp.items(), columns=['Metric', 'Importance'])
            sdfimp = dfimp.sort('Importance', ascending=False)
            dfimpCsv = 'Feature_Importance_%s.csv' % mname
            sdfimp.to_csv(os.path.join(self.modelDir, dfimpCsv))
            if self.validation is None:
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

    def trainingDataGen(self, settings, data=None, dropna=True, onlyAno=True):
        print "Starting training data generation ...."
        logger.info('[%s] : [INFO] Starting training data generation ...',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        df = self.__loadData(data, dropna)
        print df.index.name
        if df.index.name is None:
            df.set_index('key', inplace=True)
        logger.info('[%s] : [INFO] Input Dataframe shape: %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(df.shape))

        if 'iso_n_estimators' not in settings.keys():
            settings['iso_n_estimators'] = 100
        if 'iso_max_samples' not in settings.keys():
            settings['iso_max_samples'] = 'auto'
        if 'iso_contamination' not in settings.keys():
            settings['iso_contamination'] = 0.1
        if 'iso_bootstrap' not in settings.keys():
            settings['iso_bootstrap'] = True
        if 'iso_max_features' not in settings.keys():
            settings['iso_max_features'] = 1.0
        if 'iso_n_jobs' not in settings.keys():
            settings['iso_n_jobs'] = 1
        if 'iso_random_state' not in settings.keys():
            settings['iso_random_state'] = None
        if 'iso_verbose' not in settings.keys():
            settings['iso_verbose'] = 0

        if settings['iso_random_state'] == 'None':
            settings['iso_random_state'] = None

        if isinstance(settings['iso_bootstrap'], str):
            settings['iso_bootstrap'] = str2Bool(settings['bootstrap'])

        if isinstance(settings['iso_verbose'], str):
            settings['iso_verbose'] = str2Bool(settings['verbose'])

        if settings['iso_max_samples'] != 'auto':
            settings['iso_max_samples'] = int(settings['max_samples'])

        if isinstance(settings['iso_max_features'], str):
            settings["iso_max_features"] = 1.0
        # print type(settings['max_samples'])
        allowedIso = ['iso_n_estimators', 'iso_max_samples', 'iso_contamination', 'iso_bootstrap', 'iso_max_features', 'iso_n_jobs',
                   'iso_random_state', 'iso_verbose']
        for k, v in settings.iteritems():
            if k in allowedIso:
                logger.info('[%s] : [INFO] IsolationForest %s set to %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), k, v)
                print "IsolationForest %s set to %s" % (k, v)

        try:
            clf = IsolationForest(n_estimators=int(settings['iso_n_estimators']), max_samples=settings['iso_max_samples'], contamination=float(settings['iso_contamination']), bootstrap=settings['bootstrap'],
                        max_features=float(settings['iso_max_features']), n_jobs=int(settings['iso_n_jobs']), random_state=settings['iso_random_state'], verbose=settings['iso_verbose'])
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

        # Generate anomalydataframe/ anomalies only
        slist = []
        for an in anomalies:
            slist.append(df.iloc[an[0]])
        anomalyFrame = pd.DataFrame(slist)
        # if df.index.name.index is None:
        #     anomalyFrame.set_index('key', inplace=True)

        logger.info('[%s] : [INFO] Anomaly Dataframe shape: %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(anomalyFrame.shape))

        if self.checkpoint:
            logger.info('[%s] : [INFO] Anomalies checkpointed.',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(len(anomalies)))
            anomalyFrame.to_csv(os.path.join(self.dataDir, 'AnomalyFrame.csv'))

        print "Normalizing data ..."
        X = StandardScaler().fit_transform(anomalyFrame)

        print "Started Anomaly clustering ..."
        if 'db_eps' not in settings.keys():
            settings['db_eps'] = 0.9
        if 'db_min_samples' not in settings.keys():
            settings['db_min_samples'] = 40
        if 'db_metric' not in settings.keys():
            settings['db_metric'] = 'euclidean'
        if 'db_algorithm' not in settings.keys():
            settings['db_algorithm'] = 'auto'
        if 'db_leaf_size' not in settings.keys():
            settings['db_leaf_size'] = 30
        if 'db_p' not in settings.keys():
            settings['db_p'] = 0.2
        if 'db_n_jobs' not in settings.keys():
            settings['db_n_jobs'] = 1

        allowedDB = ['db_eps', 'db_min_samples', 'db_metric', 'db_algorithm', 'db_leaf_size', 'db_p', 'db_n_jobs']
        for k, v in settings.iteritems():
            if k in allowedDB:
                logger.info('[%s] : [INFO] SDBSCAN %s set to %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), k, v)
                print "SDBSCAN %s set to %s" % (k, v)

        # db = DBSCAN(eps=0.9, min_samples=40).fit(X)
        try:
            db = DBSCAN(eps=float(settings['db_eps']), min_samples=int(settings['db_min_samples']), metric=settings['db_metric'],
                        algorithm=settings['db_algorithm'], leaf_size=int(settings['db_leaf_size']), p=float(settings['db_p']),
                        n_jobs=int(settings['db_n_jobs'])).fit(X)
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

        if onlyAno:
            # Remove noise from data
            print "Removing noise from data ..."
            data_labeled = anomalyFrame[anomalyFrame["Target"] != -1]
            if self.checkpoint:
                data_labeled.to_csv(os.path.join(self.dataDir, 'AnomalyFrame_Labeled.csv'))
            print "Finished training data generation"
            return data_labeled
        else:
            bval = np.amax(labels)
            print bval
            # replace noise with new label
            labels[labels == -1] = bval + 1
            # add one to all elements in array so that 0 is free for normal events
            nlabels = labels + 1
            anomalyFrame['Target2'] = nlabels
            # print data_labeled[['Target', 'Target2']]
            # initialize empty column
            df['TargetF'] = np.nan
            print df.shape
            print anomalyFrame.shape
            # add clustered anomalies to original dataframe
            for k in anomalyFrame.index.values:
                try:
                    df.set_value(k, 'TargetF', anomalyFrame.loc[k, 'Target2'])
                except Exception as inst:
                    print inst.args
                    print type(inst)
                    print k
                    sys.exit()
            # Mark all normal instances as 0
            df = df.fillna(0)
            if df.isnull().values.any():
                logger.error('[%s] : [ERROR] Found null values in anomaly dataframe after processing!',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                print 'Found null values in anomaly dataframe after processing!'
                sys.exit(1)
            if self.checkpoint:
                df.to_csv(os.path.join(self.dataDir, 'AnomalyFrame_Labeled_Complete.csv'))
            print "Finished training data generation"
            return df

    def __loadData(self, data=None, dropna=True):
        if not self.checkpoint:
            dfile = os.path.join(self.dataDir, self.training)
            logger.info('[%s] : [INFO] Data file is set to %s', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(dfile))
            if not os.path.isfile(dfile):
                print "Training file %s not found" % dfile
                logger.error('[%s] : [ERROR] Training file %s not found',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(dfile))
                sys.exit(1)
            else:
                df = pd.read_csv(dfile)
        else:
            if not isinstance(data, pd.core.frame.DataFrame):
                print "Data is of type %s and not dataframe, exiting!" % type(data)
                logger.error('[%s] : [ERROR] Data is of type %s and not dataframe, exiting!',
                             datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(type(data)))
                sys.exit(1)
            df = data
        if df.index.name is None:
            df.set_index('key', inplace=True)
        if dropna:
            df = df.dropna()
        return df

    def __gridSearch(self, est, X, y):
        if isinstance(est, RandomForestClassifier):
            param_grid = {
                'n_estimators': [200, 300, 400, 500, 800, 1000],
                'max_features': ['auto', 'sqrt', 'log2'],
                'max_depth': [5, 15, 25]
            }

        CV_rfc = GridSearchCV(estimator=est, param_grid=param_grid, cv=5)
        CV_rfc.fit(X, y)
        logger.info('[%s] : [INFO] Best parameters are: %s',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), CV_rfc.best_params_)
        print 'Best parameters are: %s'  % CV_rfc.best_params_
        return CV_rfc.best_params_

    def __loadClassificationModel(self, method, model):
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

    def __normalize(self, data):
        normalized_data = StandardScaler().fit_transform(data)
        return normalized_data

    def __dataSplit(self, X, y, trainSize=None, testSize=None):
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=testSize, train_size=trainSize)
        return X_train, X_test, y_train, y_test