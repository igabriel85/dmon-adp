from sklearn.model_selection import train_test_split
import os
from adplogger import logger
from datetime import datetime
import time
import sys
import pandas as pd
from sklearn.ensemble import RandomForestClassifier


class SciClassification:
    def __init__(self, modelDir, dataDir, settings):
        self.modelDir = modelDir
        self.dataDir = dataDir
        self.settings = settings

    def detect(self, method, model, data):
        return True

    def score(self):
        return True

    def compare(self):
        return True

    def naiveBayes(self):
        return True

    def adaBoost(self):
        return True

    def neuralNet(self):
        return True

    def decisionTree(self, trainingData, validationData, settings):
        return True

    def randomForest(self, settings, data=None, dropna=True):
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
        features = df.columns[:-1]
        X = df[features]
        y = df.iloc[:, -1].values

        clf = RandomForestClassifier(max_depth=5, n_estimators=10, max_features=1, n_jobs=2)
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

            print "Actual ffflabels of training set:"
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
                return True
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
                    return True
        return True

    def __loadClusterModel(self, method, model):
        return True

    def __serializemodel(self, model, method, mname):
        return True

    def __normalize(self):
        return True

    def __dataSplit(self, X, y, trainSize=None, testSize=None):
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=testSize, train_size=trainSize)
        return X_train, X_test, y_train, y_test