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
        "training":"Storm_anomalies_Clustered.csv",
        "validation":"Storm_anomalies_Clustered.csv",
        "validratio":0.3,
        "compare": False,
        "target": None
    }

classifRF = dmonscilearnclassification.SciClassification(modelDir, dataDir, settings)

classifRF.randomForest(settings)