from dataformatter import DataFormatter
from pyQueryConstructor import QueryConstructor
import os
from dmonconnector import Connector
from adpconfig import readConf


def stormDump(dmonEndpoint, qgte, qlte, qsize, qinterval):
    '''
    :param dmonEndpoint: -> DMON endpoint
    :param qgte: -> greater than timestamp
    :param qlte: -> less than timestamp
    :param qsize: -> query size
    :param qinterval: -> query interval
    :return:
    '''
    dmonConnector = Connector(dmonEndpoint)
    stormTopology = dmonConnector.getStormTopology()

    bolts = stormTopology['bolts']
    spouts = stormTopology['spouts']
    print "Detected %s bolts" % str(bolts)
    print "Detected %s spouts" % str(spouts)

    qConstructor = QueryConstructor()
    dformat = DataFormatter(dataDir)

    storm, storm_file = qConstructor.stormString()
    print "Query string -> %s" % storm
    qstorm = qConstructor.stormQuery(storm, qgte, qlte, qsize, qinterval, bolts=bolts, spouts=spouts)
    print "Query -> %s" % qstorm
    gstorm = dmonConnector.aggQuery(qstorm)

    print "Response:"
    print gstorm
    dformat.dict2csv(gstorm, qstorm, storm_file)

if __name__ == '__main__':
    dataDir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))

    file_conf = 'dmonadp.ini'
    readCnf = readConf(file_conf)
    print os.path.abspath(file_conf)

    print 'DMON endpoint set to -> %s ' % str(readCnf['Connector']['esendpoint'])
    print 'Query from -> %s' % str(readCnf['Connector']['from'])
    print 'Query to -> %s' % str(readCnf['Connector']['to'])
    print 'Query size -> %s' % str(readCnf['Connector']['qsize'])
    print 'Query interval -> %s' % str(readCnf['Connector']['qinterval'])

    stormDump(dmonEndpoint=readCnf['Connector']['esendpoint'], qgte=readCnf['Connector']['from'],
              qlte=readCnf['Connector']['to'], qsize=readCnf['Connector']['qsize'],
              qinterval=readCnf['Connector']['qinterval'])




