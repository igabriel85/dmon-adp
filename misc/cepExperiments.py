import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
from sklearn.feature_extraction import DictVectorizer
import os
import pandas as pd
import scipy
from scipy import stats
import sys

dataDir = os.path.join(os.path.dirname(os.path.abspath('')), 'data')

data = pd.read_csv(os.path.join(dataDir, 'cep.csv'))
data.set_index('key', inplace=True)


print data.shape
# add aterget column and initialize to 0
data['Anomaly'] = np.nan
data = data.fillna(0)
for k, v in data.iterrows():
    print k
    if v['ms'] > 26000 and v['component'] == 'AIS_SENTENCE_LISTENER':
        v['Anomaly'] = 1
        data.set_value(k, 'Anomaly', 1)
        print "Anomaly Type 1"
    if v['ms'] > 37000 and v['component'] == 'SIMPLE_ANCHOR_IN':
        v['Anomaly'] = 2
        data.set_value(k, 'Anomaly', 2)
        print "Anomaly Type 2"
    if v['ms'] > 2000 and v['component'] == 'SIMPLE_DOCK_STOP':
        v['Anomaly'] = 3
        data.set_value(k, 'Anomaly', 3)
        print "Anomaly Type 3"
    if v['ms'] > 2000 and v['component'] == 'STOP_OVER_OUT':
        v["Anomaly"] = 4
        data.set_value(k, 'Anomaly', 4)
        print "Anomaly Type 4"


print data['Anomaly'].values
# data.set_index('key', inplace=True)
data.to_csv(os.path.join(dataDir, 'CEP_Complete_Labeled_Extended.csv'))


    # try:
    #     df.set_value(k, 'TargetF', anomalyFrame.loc[k, 'Target2'])
    # except Exception as inst:
    #     print inst.args
    #     print type(inst)
    #     print k
    #     sys.exit()


# def ohEncoding(data, cols, replace=False):
#     if cols is None:
#         cols = []
#         for el, v in data.dtypes.iteritems():
#             if v == 'object':
#                 if el == 'key':
#                     pass
#                 else:
#                     cols.append(el)
#         print "Categorical features not set, detected as categorical: %s" % str(cols)
#     vec = DictVectorizer()
#     mkdict = lambda row: dict((col, row[col]) for col in cols)
#     vecData = pd.DataFrame(vec.fit_transform(data[cols].apply(mkdict, axis=1)).toarray())
#     vecData.columns = vec.get_feature_names()
#     vecData.index = data.index
#     if replace is True:
#         data = data.drop(cols, axis=1)
#         data = data.join(vecData)
#     return data, vecData, vec
#
# data, t, v = ohEncoding(data, cols=None, replace=True)
#
# # fit the model
# clf = IsolationForest(max_samples='auto', verbose=1, n_jobs=-1, contamination=0.01)
# clf.fit(data)
# pred = clf.predict(data)
# print type(pred)
# # print data.shape
# # print len(pred)
# print pred
# anomalies = np.argwhere(pred == -1)
# normal = np.argwhere(pred == 1)
# print anomalies
# print type(anomalies)
# # print normal
#
# # for an in anomalies:
# #     print int(data.iloc[an[0]]['key'])
# #     print data.iloc[an[0]].to_dict()
# #     # test2.append(data.iloc[an[0]].to_dict(), ignore_index=True)
# #     # print data.iloc[an[0]]
# #     # print type(data.iloc[an[0]])
# #     # test2.add(data.iloc[an[0]].to_frame())
#
# # test2.to_csv(os.path.join(dataDir, 'ano.csv'))
#
# ##############################################################################################################################
# #Generate anomalydataframe
# slist = []
# for an in anomalies:
#     slist.append(data.iloc[an[0]])
# test = pd.DataFrame(slist)
# # test.set_index('key', inplace=True)
# # test.to_csv(os.path.join(dataDir, 'tt.csv'))
# test.to_csv(os.path.join(dataDir, 'CEP_Anomalies.csv'))
#
#
# data['Target'] = pred
# # data.set_index('key', inplace=True)
# data.to_csv(os.path.join(dataDir, 'CEP_Complete_labeled.csv'))