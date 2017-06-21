from dmonscikit import dmonscilearnclassification
import os

dataDir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
print dataDir
modelDir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'models'))
print modelDir
settings = settings = {
        "esendpoint": None,
        "esInstanceEndpoint": 9200,
        "dmonPort": 5001,
        "index": "logstash-*",
        "from": None, # timestamp
        "to": None, # timestamp
        "query": None,
        "nodes": None,
        "qsize": None,
        "qinterval": None,
        "train": None, # Bool default None
        "type": None,
        "load": None,
        "file": None,
        "method": None,
        "validate": None, # Bool default None
        "export": None,
        "detect": None, # Bool default None
        "cfilter": None,
        "rfilter": None,
        "dfilter": None,
        "sload": None,
        "smemory": None,
        "snetwork": None,
        "heap": None,
        "checkpoint": True,
        "delay": None,
        "interval": None,
        "resetindex": None,
        "training": "Storm_anomalies_Clustered.csv",
        "validation": "Storm_anomalies_Clustered.csv",
        "validratio": 0.3,
        "compare": False,
        "target": None,
        "n_estimators": 10,
        "criterion": "gini",
        "max_features": "auto",
        "max_depth": None,
        "min_sample_split": 2,
        "min_sample_leaf": 1,
        "min_weight_faction_leaf": 0,
        "bootstrap": True,
        "n_jobs": 1,
        "random_state": None,
        "verbose": 0,
        "iso_n_estimators": 100,
        "iso_max_samples": 'auto',
        "iso_contamination": 0.1,
        "iso_bootstrap": True,
        "iso_max_features": 1.0,
        "iso_n_jobs": 1,
        "iso_random_state": None,
        "iso_verbose": 0,
        "db_eps": 0.8,
        "min_samples": 10,
        "db_metric": 'euclidean',
        "db_algorithm": 'auto',
        "db_leaf_size": 30,
        "db_p": 0.2,
        "db_n_jobs": 1
}



classifRF = dmonscilearnclassification.SciClassification(modelDir, dataDir, checkpoint=True,
                                                         export='Test', training=settings['training'],
                                                         validation=settings['validation'], validratio=settings['validratio'], compare=True)

# RandomForest
settings["n_estimators"] = 10
settings["criterion"] = "gini"
settings["max_features"] = "auto"
settings["max_depth"] = 5
settings["min_sample_split"] = 2
settings["min_sample_leaf"] = 1
settings["min_weight_faction_leaf"] = 0
# settings["min_impurity_split"] =
settings["bootstrap"] = True
settings["n_jobs"] = 2
settings["random_state"] = "None"
settings["verbose"] = 0

classifRF.randomForest(settings)


# Decision Tree
# settings["criterion"] = "gini"
# settings["splitter"] = "best"
# settings["max_features"] = "auto"
# settings["max_depth"] = 5
# settings["min_weight_faction_leaf"] = 0
# settings["random_state"] = None
# classifRF.decisionTree(settings)


# print classifRF.adaBoost(settings)


# Generate Semiautomatic Training data

# data = classifRF.trainingDataGen(settings, onlyAno=False)

# Decision Tree
# settings["criterion"] = "gini"
# settings["splitter"] = "best"
# settings["max_features"] = "auto"
# settings["max_depth"] = 5
# settings["min_samples_split"] = 2
# settings["min_weight_faction_leaf"] = 0
# settings["random_state"] = None
# settings['checkpoint'] = False
# classifRF.decisionTree(settings, data=data)
