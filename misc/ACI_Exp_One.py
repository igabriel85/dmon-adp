import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
from sklearn import model_selection
from sklearn.ensemble import AdaBoostClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.grid_search import GridSearchCV
from sklearn.tree import DecisionTreeClassifier, export_graphviz
from sklearn.feature_extraction import DictVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.model_selection import learning_curve
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import validation_curve
from sklearn.model_selection import KFold
import subprocess
import os
import pandas as pd
import time
import scipy
from scipy import stats
import sys

###############
colover = True # setting custom categorical columns
forceFill = True # force to fill missing n/a with 0
target = 'Alert' # target column header inf not set using last col as targer
paramOptim = False # Use gridsearch optimtization
###############


def plot_learning_curve(estimator, title, X, y, ylim=None, cv=None,
                        n_jobs=1, train_sizes=np.linspace(.1, 1.0, 5)):
    """
    Generate a simple plot of the test and traning learning curve.

    Parameters
    ----------
    estimator : object type that implements the "fit" and "predict" methods
        An object of that type which is cloned for each validation.

    title : string
        Title for the chart.

    X : array-like, shape (n_samples, n_features)
        Training vector, where n_samples is the number of samples and
        n_features is the number of features.

    y : array-like, shape (n_samples) or (n_samples, n_features), optional
        Target relative to X for classification or regression;
        None for unsupervised learning.

    ylim : tuple, shape (ymin, ymax), optional
        Defines minimum and maximum yvalues plotted.

    cv : integer, cross-validation generator, optional
        If an integer is passed, it is the number of folds (defaults to 3).
        Specific cross-validation objects can be passed, see
        sklearn.cross_validation module for the list of possible objects

    n_jobs : integer, optional
        Number of jobs to run in parallel (default 1).
    """
    plt.figure()
    plt.title(title)
    if ylim is not None:
        plt.ylim(*ylim)
    plt.xlabel("Training examples")
    plt.ylabel("Score")
    train_sizes, train_scores, test_scores = learning_curve(
        estimator, X, y, cv=cv, n_jobs=n_jobs, train_sizes=train_sizes)
    train_scores_mean = np.mean(train_scores, axis=1)
    train_scores_std = np.std(train_scores, axis=1)
    test_scores_mean = np.mean(test_scores, axis=1)
    test_scores_std = np.std(test_scores, axis=1)
    plt.grid()

    plt.fill_between(train_sizes, train_scores_mean - train_scores_std,
                     train_scores_mean + train_scores_std, alpha=0.1,
                     color="r")
    plt.fill_between(train_sizes, test_scores_mean - test_scores_std,
                     test_scores_mean + test_scores_std, alpha=0.1, color="g")
    plt.plot(train_sizes, train_scores_mean, 'o-', color="r",
             label="Training score")
    plt.plot(train_sizes, test_scores_mean, 'o-', color="g",
             label="Cross-validation score")

    plt.legend(loc="best")
    return plt


def validation_crv(estimator, X, y, title, n_jobs=1):
    param_range = np.logspace(-6, -1, 5)
    train_scores, test_scores = validation_curve(
        estimator, X, y, param_name="max_features", param_range=param_range,
        cv=10, scoring="accuracy", n_jobs=n_jobs)
    train_scores_mean = np.mean(train_scores, axis=1)
    train_scores_std = np.std(train_scores, axis=1)
    test_scores_mean = np.mean(test_scores, axis=1)
    test_scores_std = np.std(test_scores, axis=1)

    plt.title(title)
    plt.xlabel("$\gamma$")
    plt.ylabel("Score")
    plt.ylim(0.0, 1.1)
    lw = 2
    plt.semilogx(param_range, train_scores_mean, label="Training score",
                 color="darkorange", lw=lw)
    plt.fill_between(param_range, train_scores_mean - train_scores_std,
                     train_scores_mean + train_scores_std, alpha=0.2,
                     color="darkorange", lw=lw)
    plt.semilogx(param_range, test_scores_mean, label="Cross-validation score",
                 color="navy", lw=lw)
    plt.fill_between(param_range, test_scores_mean - test_scores_std,
                     test_scores_mean + test_scores_std, alpha=0.2,
                     color="navy", lw=lw)
    plt.legend(loc="best")
    return plt
print "#" *150
print "Getting Data ..."

dataDir = "/Users/Gabriel/Dropbox/ACI"
expDir = '/Users/Gabriel/Documents/workspaces/diceWorkspace/dmon-adp/experiments'
data = os.path.join(dataDir, 'TXN_ALERTS.csv')
seed = 7


df = pd.read_csv(data)
# df.set_index('key', inplace=True)
print "Shape after loading is:"
print df.shape

dropList = ['DD_DDAY', 'DD_CRD_LAST_ISS', 'DD_CRD_EMBOSS', 'DD_TRAN_DAT_TIM', 'DD_APDATE']
print "Droped columns are: %s" % dropList
df = df.drop(dropList, axis=1)
print "Index Name: %s" % df.index.name


print "Dataframe shape (row, col): %s" % str(df.shape)

# encode dataframe
col = []
for el, v in df.dtypes.iteritems():
    # print el
    if v == 'object':
        col.append(el)

print "Categorical columns detected: %s" % str(col)
if colover == True:
    col = ['SD_RETL_ID', 'SD_PROD_IND', 'SD_TERM_NAME_LOC', 'SD_TERM_CITY_OLD', 'SD_TERM_ST', 'SD_TERM_CNTRY', 'SD_CR_DB_IND', 'SD_CASH_IND', 'SD_CRD_PLASTIC_TYP', 'SD_TERM_CITY', 'SD_TRAN_RSN_CDE', 'SD_TERM_ID', 'Alert']
    print "Categorical column overide detected, using: %s" % str(col)
else:
    print "Categorical columns used: %s" % str(col)


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

print "Shape of the dataframe after encoding: "
print df.shape
if forceFill == True:
    df = df.fillna(0)

features = df.columns[:-1]

print "Detected Features are: %s" % features

X = df[features]
# Target always last column of dataframe

if target:
    y = df[target].values
    # Remove Target column from training set
    X = df.drop([target], axis=1)
else:
    y = df.iloc[:, -1].values
    # Remove Target column from training set
    X = df.iloc[:, :-1]

print "Target column set to:"
print y


#Split up dataset  for Neural Netowrk training
X_train, X_test, y_train, y_test = train_test_split(X, y)

# Normalization
print "Normalizing data ...."
scaler = StandardScaler()

scaler.fit(X_train)
# Now apply the transformations to the data:
X_train = scaler.transform(X_train)
# y_train = scaler.transform(y_train)

print "#" *150
print "Starting baseline..."

start_time_b = time.time()
rfb = RandomForestClassifier()
rfb.fit(X, y)
score_RF = rfb.score(X, y)
print "Training Score Random Forest: %s" % score_RF
elapsed_time_g = time.time() - start_time_b
print "Training Baseline for Random Forest took: %s" % str(elapsed_time_g)

print "Example Random Forest validation curve for number of estimators done"
test = validation_crv(rfb, X, y, "Test", n_jobs=1)
test.savefig(os.path.join(expDir, 'Rforest_valid_curve.png'))


start_time_b = time.time()
dtb = DecisionTreeClassifier()
dtb.fit(X, y)
score_DT = dtb.score(X, y)
print "Training Score Decision Tree: %s" % score_DT
elapsed_time_g = time.time() - start_time_b
print "Training Baseline Decision Tree took: %s" % str(elapsed_time_g)

start_time_b = time.time()
adb = AdaBoostClassifier()
adb.fit(X, y)
score_AD = adb.score(X, y)
print "Training Score AdaBoost: %s" % score_AD
elapsed_time_g = time.time() - start_time_b
print "Training Baseline AdaBoost took: %s" % str(elapsed_time_g)

start_time_b = time.time()
mlp_g = MLPClassifier()
mlp_g.fit(X_train, y_train)
score_MLP = mlp_g.score(X_train, y_train)

print "Training Score Neural Network: %s" % score_MLP
elapsed_time_g = time.time() - start_time_b
print "Training Baseline Neural Network took: %s" % str(elapsed_time_g)

print "#" *150
print "Starting Random Forest Experiments..."

# if isinstance(rfc, RandomForestClassifier):
#     print "test"
if paramOptim:
    rfc = RandomForestClassifier(n_jobs=-1, max_features='sqrt', n_estimators=50, oob_score=True)
    param_grid = {
        'n_estimators': [200, 700],
        'max_features': ['auto', 'sqrt', 'log2'],
        'max_depth': [5, 15, 25]
    }
    start_time_g = time.time()
    CV_rfc = GridSearchCV(estimator=rfc, param_grid=param_grid, cv=5)
    CV_rfc.fit(X, y)
    print CV_rfc.best_params_
    bestParam = CV_rfc.best_params_
    print "Best params for Random Forest: %s" % str(bestParam)
    elapsed_time_g = time.time() - start_time_g
    print "Grid Search for Random Forest took: %s" % str(elapsed_time_g)
else:
    bestParam = {}
    bestParam['max_depth'] = 100
    bestParam['n_estimators'] = 500
    bestParam['max_features'] = 400

# fix random seed for reproducibility
start_time = time.time()
clfKV = RandomForestClassifier(max_depth=bestParam['max_depth'], n_estimators=bestParam['n_estimators'],
                               max_features=bestParam['max_features'], n_jobs=-1)
kfold = KFold(n_splits=10, shuffle=True, random_state=seed)

results = cross_val_score(clfKV, X, y, cv=kfold)
print("Baseline: %.2f%% (%.2f%%)" % (results.mean() * 100, results.std() * 100))
elapsed_time = time.time() - start_time
print "Cross validation took: %s" % str(elapsed_time)

title = "Learning Curves (Random Forest)"
plot_learning_curve(clfKV, title, X, y, ylim=(0.2, 1.01), cv=kfold, n_jobs=4)

print "Saving Lerning Curve (Random Forest)"
plt.savefig(os.path.join(expDir, 'Rforest.png'))

start_time = time.time()
clf = RandomForestClassifier(max_depth=bestParam['max_depth'], n_estimators=bestParam['n_estimators'],
                             max_features=bestParam['max_features'], n_jobs=-1)
clf.fit(X, y)

# Apply the classifier we trained to the test data (which, remember, it has never seen before)
predict = clf.predict(X)
# print predict

# View the predicted probabilities of the first 10 observations
predProb = clf.predict_proba(X)
# print predProb

score = clf.score(X, y)
print "Training Score Random Forest: %s" % score

# # Create confusion matrix
# print pd.crosstab(test['species'], preds, rownames=['Actual Species'], colnames=['Predicted Species'])
#
# View a list of the features and their importance scores

fimp = list(zip(X, clf.feature_importances_))
print "Feature importance Random Forest Training: "
print fimp
elapsed_time = time.time() - start_time
print "Training Random Forest Took: %s" % str(elapsed_time)
dfimp = dict(fimp)
dfimp = pd.DataFrame(dfimp.items(), columns=['Metric', 'Importance'])
sdfimp = dfimp.sort('Importance', ascending=False)
dfimpCsv = 'Feature_Importance_RF_%s.csv' % 'ACI'
sdfimp.to_csv(os.path.join(expDir, dfimpCsv))

print "#" *150
print "Starting Decision Tree Experiments"

if paramOptim:
    dt_g = DecisionTreeClassifier()
    param_grid_dt = {
        'criterion': ['gini', 'entropy'],
        'splitter': ['best', 'random'],
        'max_features': ['auto', 'sqrt', 'log2'],
        'max_depth': [5, 15, 25, 50, 100],
        'min_samples_split': [2, 5, 10]
    }

    start_time_d = time.time()
    CV_dt = GridSearchCV(estimator=dt_g, param_grid=param_grid_dt, cv=5)
    CV_dt.fit(X, y)
    print CV_dt.best_params_
    bestParam_dt = CV_dt.best_params_
    print "Best params for Decision Tree: %s" % str(bestParam_dt)
    elapsed_time_dt = time.time() - start_time_d
    print "Grid Search for Decision Tree took: %s" % str(elapsed_time_dt)
else:
    bestParam_dt ={}
    bestParam_dt["criterion"] = 'gini'
    bestParam_dt["splitter"] = 'best'
    bestParam_dt['max_features'] = 'auto'
    bestParam_dt['max_depth'] = 100
    bestParam_dt['min_samples_split'] = 10
    print "Set params for Decision Tree: %s " % str(bestParam_dt)

# fix random seed for reproducibility
start_time = time.time()
clfKV_dt = DecisionTreeClassifier(criterion=bestParam_dt["criterion"], splitter=bestParam_dt["splitter"], max_features=bestParam_dt['max_features'],
                            max_depth=bestParam_dt['max_depth'], min_samples_split=bestParam_dt['min_samples_split'])

kfold = KFold(n_splits=10, shuffle=True, random_state=seed)

results = cross_val_score(clfKV_dt, X, y, cv=kfold)
print("Baseline: %.2f%% (%.2f%%)" % (results.mean() * 100, results.std() * 100))
elapsed_time = time.time() - start_time
print "Cross validation took: %s" % str(elapsed_time)

start_time = time.time()
dt = DecisionTreeClassifier(criterion=bestParam_dt["criterion"], splitter=bestParam_dt["splitter"], max_features=bestParam_dt['max_features'],
                            max_depth=bestParam_dt['max_depth'], min_samples_split=bestParam_dt['min_samples_split'])

dt.fit(X, y)
predict_dt = dt.predict(X)
print "Prediction for Decision Tree Training:"
print predict_dt

predProb_dt = dt.predict_proba(X)
print "Prediction probabilities for Decision Tree Training:"
print predProb_dt

score_dt = dt.score(X, y)
print "Decision Tree Training Score: %s" % str(score_dt)

fimp_dt = list(zip(X, dt.feature_importances_))
print "Feature importance Decision Tree Training: "
print fimp_dt
elapsed_time = time.time() - start_time
print "Training Decision Tree Took: %s" % str(elapsed_time)
dfimp_dt = dict(fimp_dt)
dfimp_dt = pd.DataFrame(dfimp_dt.items(), columns=['Metric', 'Importance'])
sdfimp_dt = dfimp_dt.sort('Importance', ascending=False)
dfimpCsv_dt = 'Feature_Importance_DT_%s.csv' % 'ACI'
sdfimp_dt.to_csv(os.path.join(expDir, dfimpCsv_dt))


def visualize_tree(tree, feature_names):
    """Create tree png using graphviz.

    Args
    ----
    tree -- scikit-learn DecsisionTree.
    feature_names -- list of feature names.
    """
    file_dt = os.path.join(expDir, 'dt.dot')
    with open(file_dt, 'w') as f:
        export_graphviz(tree, out_file=f,
                        feature_names=feature_names)

    command = ["dot", "-Tpng", "dt.dot", "-o", "dt_ACI.png"]
    try:
        subprocess.check_call(command)
    except:
        exit("Could not run dot, ie graphviz, to "
             "produce visualization")

visualize_tree(dt, features)

print "#" *150
print "Starting AdaBoost Experiment"
if paramOptim:
    ad_g = AdaBoostClassifier()
    param_grid_ad = {
        'n_estimators': [10, 20, 100, 200, 700],
        'learning_rate': [1, 5, 10, 20]
    }

    start_time_d = time.time()
    CV_ad = GridSearchCV(estimator=ad_g, param_grid=param_grid_ad, cv=5)
    CV_ad.fit(X, y)
    print CV_ad.best_params_
    bestParam_ad = CV_ad.best_params_
    print "Best params for AdaBoost: %s" % str(bestParam_ad)
    elapsed_time_dt = time.time() - start_time_d
    print "Grid Search for AdaBoost took: %s" % str(elapsed_time_dt)
else:
    bestParam_ad = {}
    bestParam_ad['n_estimators'] = 200
    bestParam_ad['learning_rate'] = 20
    print "Set parameters for AdaBoost are: %s" % str(bestParam_ad)

start_time_d = time.time()
num_trees = 5
kfold = model_selection.KFold(n_splits=10, random_state=seed)
print kfold
model = AdaBoostClassifier(n_estimators=bestParam_ad['n_estimators'], learning_rate=bestParam_ad['learning_rate'])
results = cross_val_score(model, X, y, cv=kfold)
print("Baseline: %.2f%% (%.2f%%)" % (results.mean() * 100, results.std() * 100))
elapsed_time = time.time() - start_time_d
print "Cross validation took: %s" % str(elapsed_time)

start_time_d = time.time()
ad = AdaBoostClassifier(n_estimators=bestParam_ad['n_estimators'], learning_rate=bestParam_ad['learning_rate'])

ad.fit(X, y)
predict_ad = ad.predict(X)
print "Prediction for AdaBoost Training:"
print predict_ad

predProb_ad = ad.predict_proba(X)
print "Prediction probabilities for AdaBoost Training:"
print predProb_ad

score_ad = ad.score(X, y)
print "AdaBoost Training Score: %s" % str(score_ad)

fimp_ad = list(zip(X, ad.feature_importances_))
print "Feature importance AdaBoost Training: "
print fimp_ad
elapsed_time = time.time() - start_time_d
print "Training AdaBoost Took: %s" % str(elapsed_time)
dfimp_ad = dict(fimp_ad)
dfimp_ad = pd.DataFrame(dfimp_ad.items(), columns=['Metric', 'Importance'])
sdfimp_ad = dfimp_ad.sort('Importance', ascending=False)
dfimpCsv_ad = 'Feature_Importance_AD_%s.csv' % 'ACI'
sdfimp_ad.to_csv(os.path.join(expDir, dfimpCsv_ad))


print "#" *150
print "Starting Neural Network Experiment"

if paramOptim:
    mlp_g = MLPClassifier()
    param_grid_mlp = {
        'activation': ['identity', 'logistic', 'tanh', 'relu'],
        'solver': ['lbfgs', 'sgd', 'adam'],
        'learning_rate': ['constant', 'invscaling', 'adaptive'],
        'momentum': [0.9, 0.5, 0.1],
        'alpha': [0.0001, 0.00001, 0.001, 0.01, 0.1]
    }

    param_grid_mlp_bk = {
        'max_iter': [200, 300, 500, 1000, 2000, 5000, 10000],
        'activation': ['identity', 'logistic', 'tanh', 'relu'],
        'solver': ['lbfgs', 'sgd', 'adam'],
        'batch_size': ['auto', 50, 100, 200, 500],
        'learning_rate': ['constant', 'invscaling', 'adaptive'],
        'momentum': [1, 0.9, 0.5, 0.3, 0.1],
        'alpha': [0.0001, 0.00001, 0.001, 0.01, 0.1]

    }


    start_time_d = time.time()
    CV_mlp = GridSearchCV(estimator=mlp_g, param_grid=param_grid_mlp, cv=5)
    CV_mlp.fit(X_train, y_train)
    print CV_mlp.best_params_
    bestParam_mlp = CV_mlp.best_params_
    print "Best params for Neural Network: %s" % str(bestParam_mlp)
    elapsed_time_mlp = time.time() - start_time_d
    print "Grid Search for Neural Network: %s" % str(elapsed_time_mlp)
else:
    bestParam_mlp = {}
    bestParam_mlp['activation'] = 'relu'
    bestParam_mlp['solver'] = 'adam'
    bestParam_mlp['learning_rate'] = 'adaptive'
    bestParam_mlp['momentum'] = 0.9
    bestParam_mlp['alpha'] = 0.0001
    print "Parameters set for Neural Network: %s" % str(bestParam_mlp)

#bypass best params
bestParam_mlp['max_iter'] = 200
bestParam_mlp['batch_size'] = 'auto'

start_time = time.time()
mlpKV = MLPClassifier(hidden_layer_sizes=(20,20), max_iter=bestParam_mlp['max_iter'], activation=bestParam_mlp['activation'],
                      solver=bestParam_mlp['solver'], batch_size=bestParam_mlp['batch_size'],
                      learning_rate=bestParam_mlp['learning_rate'], momentum=bestParam_mlp['momentum'], alpha=bestParam_mlp['alpha'])
kfold = KFold(n_splits=10, shuffle=True, random_state=seed)

results = cross_val_score(mlpKV, X_train, y_train, cv=kfold)
print("Baseline: %.2f%% (%.2f%%)" % (results.mean() * 100, results.std() * 100))
elapsed_time = time.time() - start_time
print "Cross validation took: %s" % str(elapsed_time)

start_time = time.time()
mlp = MLPClassifier(hidden_layer_sizes=(20,20), max_iter=bestParam_mlp['max_iter'], activation=bestParam_mlp['activation'],
                      solver=bestParam_mlp['solver'], batch_size=bestParam_mlp['batch_size'],
                      learning_rate=bestParam_mlp['learning_rate'], momentum=bestParam_mlp['momentum'], alpha=bestParam_mlp['alpha'])
mlp.fit(X_train, y_train)

# Apply the classifier we trained to the test data (which, remember, it has never seen before)
predict = mlp.predict(X_train)
# print predict

# View the predicted probabilities of the first 10 observations
predProb = mlp.predict_proba(X_train)
# print predProb

score = mlp.score(X_train, y_train)
print "Training Score Neural Network: %s" % score
elapsed_time = time.time() - start_time
print "Training Neural Network Took: %s" % str(elapsed_time)

# # Create confusion matrix
# print pd.crosstab(test['species'], preds, rownames=['Actual Species'], colnames=['Predicted Species'])
#
# View a list of the features and their importance scores


# fimp = list(zip(X_train, mlp.feature_importances_))
# print "Feature importance Neural Network Training: "
# print fimp
# elapsed_time = time.time() - start_time
# print "Training Neural Network  Took: %s" % str(elapsed_time)
# dfimp = dict(fimp)
# dfimp = pd.DataFrame(dfimp.items(), columns=['Metric', 'Importance'])
# sdfimp = dfimp.sort('Importance', ascending=False)
# dfimpCsv = 'Feature_Importance_MLP_%s.csv' % 'CEP'
# sdfimp.to_csv(os.path.join(expDir, dfimpCsv))