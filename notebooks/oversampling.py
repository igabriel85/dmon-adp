import os
import pandas as pd
import numpy as np
from imblearn.over_sampling import SMOTE
import time
import cPickle as pickle

dataDir = '/home/gabriel/Research/dmon-adp/data'
expDir = '/home/gabriel/Research/dmon-adp/data'
print "Loading data ..."
data = os.path.join(dataDir, 'creditcard.csv')
df = pd.read_csv(data)
print"Data loaded with shape: {}".format(df.shape)
np.set_printoptions(precision=16)
_, counts = np.unique(df['Class'], return_counts=True)
print "Percentage of fraudulent transactions : {}%".format(float(counts[1])/float(df.shape[0])*100)
y = df.Class.values
X = df.drop(['Class'], axis=1)
print "Running SMOTE ..."
sm = SMOTE(kind='regular')
X_sm, y_sm = sm.fit_sample(X, y)
print "Shape of upsampled data is {}".format(X_sm.shape)
_, ncounts = np.unique(y_sm, return_counts=True)
print "Precentage of upsampled fraudulent tranzactions is {}".format(float(ncounts[1])/float(X_sm.shape[0])*100)
print "Saving oversampled data .."
X_f = open(os.path.join(dataDir, 'X.dat'), 'wb')
y_f = open(os.path.join(dataDir,'y.dat'), 'wb')
pickle.dump(X_sm, X_f, pickle.HIGHEST_PROTOCOL)
pickle.dump(y_sm, y_f, pickle.HIGHEST_PROTOCOL)