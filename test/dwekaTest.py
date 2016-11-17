from dmonweka import *
from dataformatter import DataFormatter
from util import ut2hum
import os

dataDir = os.path.join(os.path.dirname(os.path.abspath('')), 'data')
modelDir = os.path.join(os.path.dirname(os.path.abspath('')), 'models')

print dataDir
print modelDir

dformat = DataFormatter(dataDir)
test = dweka(dataDir, modelDir)

#dformat.dict2arff(os.path.join(dataDir, 'ctest2.csv'), os.path.join(dataDir, 'cTest.arff'))

print type(test)
options = ["-N", "10", "-S", "10"]
dataf = os.path.join(dataDir, "cTest.csv")

test.simpleKMeansTrain(dataf, options, 'test', temp=False)

anomalies = test.runclustermodel("skm", "test", dataf, temp=False)

for e in anomalies:
    print ut2hum(e)
    # print int(e/1000)
#
# file = os.path.join(dataDir, 'System.arff')
# file1 = os.path.join(dataDir, 'System.csv')

#
