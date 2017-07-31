import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
from sklearn.feature_extraction import DictVectorizer
import os
import pandas as pd
import scipy
from scipy import stats
import sys



dataDir = '/Users/Gabriel/Documents/workspaces/diceWorkspace/dmon-adp/data'
expDir = '/Users/Gabriel/Documents/workspaces/diceWorkspace/dmon-adp/experiments'
data = os.path.join(dataDir, 'CEP_Complete_Labeled_Extended.csv')
# data = os.path.join(dataDir, 'Storm_anomalies_Clustered.csv')


df = pd.read_csv(data)
df.set_index('key', inplace=True)
dropList = ['host', 'ship', 'method']
print "Droped columns are: %s" % dropList
df = df.drop(dropList, axis=1)
print "Index Name: %s" % df.index.name

# encode dataframe
col = []
for el, v in df.dtypes.iteritems():
    # print el
    if v == 'object':
        col.append(el)


def ohEncoding(data, cols, replace=False):
    vec = DictVectorizer()
    mkdict = lambda row: dict((col, row[col]) for col in cols)
    vecData = pd.DataFrame(vec.fit_transform(data[cols].apply(mkdict, axis=1)).toarray())
    vecData.columns = vec.get_feature_names()
    vecData.index = data.index
    if replace is True:
        data = data.drop(cols, axis=1)
        data = data.join(vecData)
    return data, vecData, vec

df, t, v = ohEncoding(df, col, replace=True)

print "Shape after encoding"
print type(df.shape)

df_unlabeled = df.drop("Anomaly", axis=1)
print "Shape of the dataframe without anomaly column: "
print df_unlabeled.shape

clf = IsolationForest(max_samples=6444, verbose=1, n_jobs=-1, contamination=0.255555
                      , bootstrap=True, max_features=9)
clf.fit(df_unlabeled)
pred = clf.predict(df_unlabeled)
# print type(pred)
# print data.shape
# print len(pred)
# print pred
anomalies = np.argwhere(pred == -1)
normal = np.argwhere(pred == 1)
# print anomalies
# print type(anomalies)

df['ISO1'] = pred

# iterate over rows
nLabAno = 0
nDetAno = 0
nFalsePositives = 0
nPositives = 0
for index, row in df.iterrows():
    if row['ISO1'] != 1.0:
        nDetAno += 1
        if row['Anomaly'] != 0.0:
            nPositives += 1
        else:
            nFalsePositives += 1
    if row['Anomaly'] != 0.0:
        nLabAno += 1

print "Labeled anomalies: %s" %str(nLabAno)
print "Detected anomalies: %s" %str(nDetAno)
print "False Positives: %s" %str(nFalsePositives)
print "Good anomalies: %s" %str(nPositives)

def percentage(part, whole):
  return 100 * float(part)/float(whole)

lano = percentage(nLabAno, df.shape[0])
dano = percentage(nDetAno, df.shape[0])
acc = percentage(nPositives, nDetAno)
print "Percentage Labeled Anomalies: %s" %str(lano)
print "Percentage of Detected Anomalies: %s" %str(dano)
print "Accuracy: %s" %str(acc)
# df.to_csv(os.path.join(expDir, 'Storm_ISOEXP_labeled.csv'))