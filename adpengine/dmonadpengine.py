import os
from dmonconnector import *
settings = {'load': '<model_name>', 'index': 'logstash-*', 'qsize': 'qs', 'from': '1444444444', 'detect': False, 'qinterval': '10s', 'dmonPort': 5001, 'validate': False, 'esendpoint': '85.120.206.27', 'snetwork': 'net_threashold', 'smemory': 'mem_threashold', 'to': '1455555555', 'export': '<model_name>', 'train': True, 'esInstanceEndpoint': 9200, 'MethodSettings': {'set1': 'none', 'set2': 'none', 'set3': 'none'}, 'file': None, 'sload': 'load_threashold', 'query': 'es_query>', 'model': '<model_name>', 'method': 'method_name'}


class AdpEngine:
    def __init__(self, settingsDict):
        self.esendpoint = settingsDict['esendpoint']
        self.esInstanceEndpoint = settingsDict['esInstanceEndpoint']
        self.dmonPort = settingsDict['dmonPort']
        self.index = settingsDict['index']
        self.tfrom = settingsDict['from']
        self.to = settingsDict['to']
        self.query = settingsDict['query']
        self.qsize = settingsDict['qsize']
        self.qinterval = settingsDict['qinterval']
        self.train = settingsDict['train']
        self.model = settingsDict['model']
        self.load = settingsDict['load']
        self.method = settingsDict['method']
        self.validate = settingsDict['validate']
        self.export = settingsDict['export']
        self.detect = settingsDict['detect']
        self.sload = settingsDict['sload']
        self.smemory = settingsDict['smemory']
        self.snetwork = settingsDict['snetwork']
        self.methodSettings = settingsDict['MethodSettings']
        self.dataDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
        self.modelsDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
        self.dmonConnector = Connector(self.esendpoint)
        self.qConstructor = QueryConstructor()
        self.dformat = DataFormatter(self.dataDir)

    def initConnector(self):
        print "Establishing connection with dmon ....."
        resInfo = self.dmonConnector.info()
        print "General es dmon info -> %s" %resInfo


        # nodeList = dmonConnector.getNodeList()
        # interval = dmonConnector.getInterval()

        # if int(qinterval[:-1]) < interval['System']:
        #     logger.warning('[%s] : [WARN] System Interval smaller than set interval!',
        #                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        return "connector init"

    def getData(self):
        return "Data"

    def runmethod(self):
        return "select and run methods"

    def loadModel(self):
        return "model"

    def reportAnomaly(self):
        return "anomaly"

    def printTest(self):
        print "Endpoint -> %s" %self.esendpoint
        print "Method settings -> %s" %self.methodSettings




test = AdpEngine(settings)

print test.initConnector()