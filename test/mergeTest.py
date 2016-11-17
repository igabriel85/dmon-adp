from dataformatter import *
from adpengine import AdpEngine


dataDir = os.path.join(os.path.dirname(os.path.abspath('')), 'data')
modelsDir = os.path.join(os.path.dirname(os.path.abspath('')), 'models')
settings = {'load': 'test1', 'qsize': '0', 'dfilter': 'AMLaunchDelayAvgTime', 'export': 'test1', 'file': None, 'rfilter': '', 'query': 'yarn:cluster, nn, nm, dfs, dn, mr;system', 'index': 'logstash-*', 'detect': 'false', 'from': '1479105362284', 'checkpoint': 'false', 'to': '1479119769978', 'sload': 'shortterm:gd:2.0;midterm:ld:0.1;longterm:gd:1.0', 'nodes': 0, 'type': 'clustering', 'method': 'skm', 'snetwork': 'tx:gd:34344;rx:ld:323434', 'resetindex': 'false', 'interval': '15m', 'train': 'false', 'esInstanceEndpoint': 9200, 'heap': '512m', 'validate': False, 'qinterval': '10s', 'dmonPort': 5001, 'esendpoint': '85.120.206.27', 'smemory': 'cached:gd:231313;buffered:ld:312123;used:ld:12313;free:gd:23123', 'delay': '2m', 'MethodSettings': {'s': '10', 'n': '10'}, 'cfilter': None}
# file = os.path.join(dataDir, 'System.csv')
# df = pd.read_csv(file)
# df.set_index('key', inplace=True)
# # print df
# t = df.ix[1477561800000]
# print t
# print df.filter(items=[1477561800000], axis=0)
# test = DataFormatter(dataDir)

testEngine = AdpEngine(settingsDict=settings, dataDir=dataDir, modelsDir=modelsDir)
train = os.path.join(dataDir, 'Final_Merge.csv')
# detect = os.path.join(dataDir, 'cTest.csv')

df_train = pd.read_csv(train)
# df_detect = pd.read_csv(detect)
# hr = df_train.columns.values
# hd = df_detect.columns.values
# print list(set(hd) - set(hr))

df_ttrain =  df_train
df_train =  testEngine.filterData(df_train)

print df_ttrain.columns.values
print df_train.columns.values
print list(set(df_ttrain)-set(df_train))