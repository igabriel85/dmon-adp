from dataformatter import DataFormatter
from pyQueryConstructor import QueryConstructor
import os
from dmonconnector import Connector

if __name__ == '__main__':
    dataDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    modelDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')

    #Standard query values
    # qte = 1475842980000
    # qlte = 1475845200000
    qgte = 1481751128740
    qlte = 1481752928740

    qsize = 0
    qinterval = "60s"
    dmonEndpoint = '85.120.206.95'

    spouts = 1
    bolts = 2

    dmonConnector = Connector(dmonEndpoint)
    qConstructor = QueryConstructor()
    dformat = DataFormatter(dataDir)

    storm, storm_file = qConstructor.stormString()
    print "Query strinb -> %s" % storm
    qstorm = qConstructor.stormQuery(storm, qgte, qlte, qsize, qinterval, bolts=bolts, spouts=spouts)
    print "Query -> %s" % qstorm
    gstorm = dmonConnector.aggQuery(qstorm)

    print "Response:"
    print gstorm
    dformat.dict2csv(gstorm, qstorm, storm_file)