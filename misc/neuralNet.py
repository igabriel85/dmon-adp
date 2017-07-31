from sklearn.neural_network import MLPClassifier
import pandas as pd
import os
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix



dataDir = os.path.join(os.path.dirname(os.path.abspath('')), 'data')

data = pd.read_csv(os.path.join(dataDir, 'Storm_anomalies_Clustered.csv'))

#drop missing values
data = data.dropna()

# View the top 5 rows
print data.head

# Create a list of the feature column's names, last one is always the target

# df = df.drop('column_name', 1)
features = data.columns[:-1]


print type(features)
for f in features:
    if f == "Winner_Cluster":
        print "FOUND"

# train['species'] contains the actual species names. Before we can use it,
# we need to convert each species name into a digit. So, in this case there
# are three species, which have been coded as 0, 1, or 2.

# Might need this for ADT!!! TODO
# y = pd.factorize(train['species'])[0]
# print y

# y = data['Winner_Cluster'].values
y = data.iloc[:, -1].values
print y


# split training and validation sets
X_train, X_test, y_train, y_test = train_test_split(data[features], y)

# normalization
scaler = StandardScaler()

# Fit training data
scaler.fit(X_train)
# Now apply the transformations to the data:
X_train = scaler.transform(X_train)
X_test = scaler.transform(X_test)

print X_train

mlp = MLPClassifier(hidden_layer_sizes=(13,13), max_iter=5000, activation='tanh', solver='sgd', batch_size='auto', learning_rate='adaptive', momentum=0.9, alpha=0.0001)

mlpModelTrain = mlp.fit(X_train,y_train)

print mlpModelTrain

predictions = mlp.predict(X_test)

print predictions



print confusion_matrix(y_test,predictions)

print classification_report(y_test,predictions)