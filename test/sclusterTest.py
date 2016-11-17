from dmonscikit import dmonscilearncluster
import os
import pandas as pd


dataDir = os.path.join(os.path.dirname(os.path.abspath('')), 'data')
modelDir = os.path.join(os.path.dirname(os.path.abspath('')), 'models')
data = os.path.join(dataDir, 'Final_Merge.csv')
data_df = pd.read_csv(data)
print data_df

dbscan = dmonscilearncluster.SciCluster(modelDir=modelDir)

settings = {'eps': 0.9, 'min_samples': 10, 'metric': 'euclidean', 'algorithm': 'auto', 'leaf_size': 30, 'p': 0.2, 'n_jobs':1}
# mname = os.path.join(dataDir, 'sdbscan_test.pkl')
mname = 'test'
dbscan.sdbscanTrain(settings=settings, mname=mname, data=data_df)
#
isolationFrst = dmonscilearncluster.SciCluster(modelDir=modelDir)
settings2 = {'n_estimators': 100, 'max_samples': 100, 'contamination': 0.01, 'bootstrap': False,
                        'max_features': 1.0, 'n_jobs': -1, 'random_state': None, 'verbose': 0}
#
# mname = 'test'
#
isolationFrst.isolationForest(settings2, mname, data=data_df)
print isolationFrst.detect('isoforest', 'test', data_df)
print isolationFrst.detect('sdbscan', 'test', data_df)

