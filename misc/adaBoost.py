# AdaBoost Classification
import pandas as pd
import os
from sklearn import model_selection
from sklearn.ensemble import AdaBoostClassifier

dataDir = os.path.join(os.path.dirname(os.path.abspath('')), 'data')

data = pd.read_csv(os.path.join(dataDir, 'Storm_anomalies_Clustered.csv'))

#drop missing values
data = data.dropna()

# View the top rows
print data.head





# Create a list of the feature column's names, last one is always the target

# df = df.drop('column_name', 1)
features = data.columns[:-1]

# y = data['Winner_Cluster'].values
y = data.iloc[:, -1].values

print y

seed = 7
num_trees = 5
kfold = model_selection.KFold(n_splits=10, random_state=seed)
print kfold
model = AdaBoostClassifier(n_estimators=num_trees, random_state=seed)
results = model_selection.cross_val_score(model, data[features], y, cv=kfold)
print results.mean()