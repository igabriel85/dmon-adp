

import os
import subprocess
import sys

import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeClassifier, export_graphviz

dataDir = os.path.join(os.path.dirname(os.path.abspath('')), 'data')


def get_iris_data(dataDir, drop=True):
    """Get the iris data, from local csv or pandas repo."""
    fileName = os.path.join(dataDir, "Storm_anomalies_Clustered.csv")
    print fileName
    if os.path.exists(fileName):
        print"-- training data found locally"
        try:
            df = pd.read_csv(fileName)
        except:
            print "-- Unable to load training data"
            sys.exit(0)
    else:
        print "Training file %s not found " % fileName
        sys.exit(0)
    if drop:
        df = df.dropna()
    return df


def encode_target(df, target_column):
    """Add column to df with integers for the target.

    Args
    ----
    df -- pandas DataFrame.
    target_column -- column to map to int, producing
                     new Target column.

    Returns
    -------
    df_mod -- modified DataFrame.
    targets -- list of target names.
    """
    df_mod = df.copy()
    targets = df_mod[target_column].unique()
    map_to_int = {name: n for n, name in enumerate(targets)}
    df_mod["Target"] = df_mod[target_column].replace(map_to_int)

    return df_mod, targets


def visualize_tree(tree, feature_names):
    """Create tree png using graphviz.

    Args
    ----
    tree -- scikit-learn DecsisionTree.
    feature_names -- list of feature names.
    """
    with open("dt.dot", 'w') as f:
        export_graphviz(tree, out_file=f,
                        feature_names=feature_names)

    command = ["dot", "-Tpng", "dt.dot", "-o", "dt.png"]
    try:
        subprocess.check_call(command)
    except:
        exit("Could not run dot, ie graphviz, to "
             "produce visualization")
print dataDir


data = get_iris_data(dataDir)

print data.head()

print data.tail()

# return unique target features
print "* anomaly types: %s" % str(data["Winner_Cluster"].unique())

features = data.columns[:-1]
print features

y = data['Winner_Cluster'].values
print y

dt = DecisionTreeClassifier(min_samples_split=20, random_state=99)
dt.fit(data[features], y)
predict = dt.predict(data[features])
print predict

score = dt.score(data[features], y)
print score

print list(zip(data[features], dt.feature_importances_))

# visualize_tree(dt, features)
