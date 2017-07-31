import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
import os
import pandas as pd
import scipy
from scipy import stats
import sys
from sklearn.feature_extraction import DictVectorizer
from sklearn.ensemble import RandomForestClassifier
rng = np.random.RandomState(42)


def ohEncoding(data, cols=None, replace=False):
    if cols is None:
        cols = []
        for el, v in data.dtypes.iteritems():
            if v == 'object':
                cols.append(el)
        print "Categorical features not set, detected as categorical: %s" % str(cols)
    vec = DictVectorizer()
    mkdict = lambda row: dict((col, row[col]) for col in cols)
    vecData = pd.DataFrame(vec.fit_transform(data[cols].apply(mkdict, axis=1)).toarray())
    vecData.columns = vec.get_feature_names()
    vecData.index = data.index
    if replace is True:
        data = data.drop(cols, axis=1)
        data = data.join(vecData)
    return data, vecData, vec


dataDir = "/Users/Gabriel/Dropbox/ACI"
data = pd.read_csv("/Users/Gabriel/Dropbox/ACI/TXN_ALERTS.csv")
print data.shape


#DD_CRD_LAST_ISS data cand a fost emis
#DD_CRD_EMBOSS data cand a fost folosita ultima oara metoda emboss
dropCol = ['DD_DDAY', 'DD_CRD_LAST_ISS', 'DD_CRD_EMBOSS', 'DD_TRAN_DAT_TIM', 'DD_APDATE']
data = data.drop(dropCol, axis=1)
dropL = ['SD_RETL_ID', 'SD_PROD_IND', 'SD_TERM_NAME_LOC', 'SD_TERM_CITY_OLD', 'SD_TERM_ST', 'SD_TERM_CNTRY', 'SD_CR_DB_IND', 'SD_CASH_IND', 'SD_CRD_PLASTIC_TYP', 'SD_TERM_CITY', 'SD_TRAN_RSN_CDE', 'SD_TERM_ID', 'Alert']
# print len(data['SD_TERM_ID'].unique())
#
# data = data.drop(dropL, axis=1)

print "Shape of data before encoding: "
print data.shape

data, vec, vec2 = ohEncoding(data, cols=dropL, replace=True)
# print data


print "Shape after enoding"
print data.shape
# data = data.dropna()
# print "Drop n/a"
# print data.shape
data = data.fillna(0)
# data.to_csv(os.path.join(dataDir, 'TXN_OHE.csv'), index=False)


# fit the model
# print np.isnan(data)
# data = data.drop('alert')
y = data['Alert'].values
# features = data.columns[:-1]
# X = data[features]
# y = data.iloc[:, -1].values

X = data.drop(['Alert'], axis=1)
clf = RandomForestClassifier(max_depth=50, n_estimators=1000, max_features=100, n_jobs=-1)

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
feature_imp = list(zip(X, clf.feature_importances_))
print "Score of Classifier"
print score
print "Feature Improtance"
print feature_imp
dfimp = dict(feature_imp)
dfimp = pd.DataFrame(dfimp.items(), columns=['Metric', 'Importance'])
sdfimp = dfimp.sort('Importance', ascending=False)
dfimpCsv = 'Feature_Importance_ACI.csv'
sdfimp.to_csv(os.path.join(dataDir, dfimpCsv))

sys.exit()
print "Starting Unsupervised"
clf = IsolationForest(max_samples='auto', verbose=1, n_jobs=-1, contamination=0.7)
clf.fit(data)
pred = clf.predict(data)

print "Predicition array"
print pred
# print data.shape
# print pred
anomalies = np.argwhere(pred == -1)
normal = np.argwhere(pred == 1)
print "Number of anomalies detected"
print len(anomalies)
print "Key of anomalies"
print anomalies
# print type(anomalies)
# print normal

# for an in anomalies:
#     # print int(data.iloc[an[0]]['key'])
#     print data.iloc[an[0]].to_dict()
#     # test2.append(data.iloc[an[0]].to_dict(), ignore_index=True)
#     # print data.iloc[an[0]]
#     # print type(data.iloc[an[0]])
#     # test2.add(data.iloc[an[0]].to_frame())

# test2.to_csv(os.path.join(dataDir, 'ano.csv'))


#Generate anomalydataframe
# slist = []
# for an in anomalies:
#     slist.append(data.iloc[an[0]])
# test = pd.DataFrame(slist)
# # test.set_index('key', inplace=True)
# # test.to_csv(os.path.join(dataDir, 'tt.csv'))
# test.to_csv(os.path.join(dataDir, 'ACI_Anomalies.csv'))
# #
# #
# print "test"
# data['Target'] = pred
# # data.set_index('key')
# data.to_csv(os.path.join(dataDir, 'ACI_Complete_labeled.csv'))



# # for s in slist:
# #     test.append(slist, ignore_index=False)
# test.set_index('key', inplace=True)
# print test.index
# # print 'done'
# print type(test)
#
# print len(anomalies)
#







# for anomaly in anomalies:
#     chkValues = []
#     for event in normal:
#         chkValues.append(data.iloc[event[0]])
#     chkValues.append(data.iloc[anomaly[0]])
#     chkDF = pd.DataFrame(chkValues)
#     # chkDF.set_index('key', inplace=True)
#     # print chkDF.index
#     # print chkDF
#
#     print "&" *100
#     # cause = chkDF[chkDF.diff()!=0.0].stack()
#     cause = chkDF.diff()
#     cause.fillna(0.0, inplace=True)
#     print type(cause)
#     dcause = cause.to_dict()
#     print dcause
#     print data.iloc[anomaly[0]].to_dict()
#     # print cause.to_dict()
#     print "&" * 100













# print pred.w
# for el in pred:
#     if el == -1:
#         print "found anomaly"
#         print type(el)

# y_pred_train = clf.predict(X_train)
# y_pred_test = clf.predict(X_test)
# y_pred_outliers = clf.predict(X_outliers)
# print y_pred_train
# print y_pred_test
# print y_pred_outliers

# plot the line, the samples, and the nearest vectors to the plane
# xx, yy = np.meshgrid(np.linspace(-5, 5, 50), np.linspace(-5, 5, 50))
# Z = clf.decision_function(np.c_[xx.ravel(), yy.ravel()])
# Z = Z.reshape(xx.shape)

# plt.title("IsolationForest")
# plt.contourf(xx, yy, Z, cmap=plt.cm.Blues_r)
#
# b1 = plt.scatter(X_train[:, 0], X_train[:, 1], c='white')
# b2 = plt.scatter(X_test[:, 0], X_test[:, 1], c='green')
# c = plt.scatter(X_outliers[:, 0], X_outliers[:, 1], c='red')
# plt.axis('tight')
# plt.xlim((-5, 5))
# plt.ylim((-5, 5))
# plt.legend([b1, b2, c],
#            ["training observations",
#             "new regular observations", "new abnormal observations"],
#            loc="upper left")
# plt.show()