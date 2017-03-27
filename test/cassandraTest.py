from dataformatter import DataFormatter
from pyQueryConstructor import QueryConstructor
import os
from dmonconnector import Connector
from adpconfig import readConf


def cassandraDump(dmonEndpoint, qgte, qlte, qsize, qinterval):
    '''
    :param dmonEndpoint: -> DMON endpoint
    :param qgte: -> greater than timestamp
    :param qlte: -> less than timestamp
    :param qsize: -> query size
    :param qinterval: -> query interval
    :return:
    '''
    dmonConnector = Connector(dmonEndpoint)
    qConstructor = QueryConstructor()
    dformat = DataFormatter(dataDir)

    roles = dmonConnector.roles()
    cassandra_hosts = []
    for el in roles['Nodes']:
        for k, v in el.iteritems():
            if 'cassandra' in v:
                print 'Found cassandra role at %s' % k
                cassandra_hosts.append(k)

    for host in cassandra_hosts:
        print "-" * 50
        cassandra, cassandra_file = qConstructor.cassandraCounterString(host=host)
        print "Query string -> %s" % cassandra
        qcassandra = qConstructor.cassandraQuery(cassandra, qgte, qlte, qsize, qinterval)
        print "Query -> %s" % qcassandra
        gcassandra = dmonConnector.aggQuery(qcassandra)

        print "Response:"
        print gcassandra
        dformat.dict2csv(gcassandra, qcassandra, cassandra_file)

        cassandragauge, cassandragauge_file = qConstructor.cassandraGaugeString(host=host)

        qcassandragauge = qConstructor.cassandraQuery(cassandragauge, qgte, qlte, qsize, qinterval)
        print "Query -> %s" % qcassandragauge
        gcassandragauge = dmonConnector.aggQuery(qcassandragauge)

        print "Response:"
        print gcassandragauge
        dformat.dict2csv(gcassandragauge, qcassandragauge, cassandragauge_file)
        print "-" * 50

if __name__ == '__main__':
    dataDir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))


    file_conf = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dmonadp.ini'))
    readCnf = readConf(file_conf)
    print readCnf
    print 'DMON endpoint set to -> %s ' % str(readCnf['Connector']['esendpoint'])
    print 'Query from -> %s' % str(readCnf['Connector']['from'])
    print 'Query to -> %s' % str(readCnf['Connector']['to'])
    print 'Query size -> %s' % str(readCnf['Connector']['qsize'])
    print 'Query interval -> %s' % str(readCnf['Connector']['qinterval'])

    #Standard query values
    qgte = 1490603893725
    qlte = 1490604793725
    #
    # qsize = 0
    # qinterval = "30s"
    # dmonEndpoint = '109.231.122.173'
    # host = "e812ae-cassandra-worker-00163e0e0889"

    cassandraDump(dmonEndpoint=readCnf['Connector']['esendpoint'], qgte=int(readCnf['Connector']['from']),
              qlte=int(readCnf['Connector']['to']), qsize=int(readCnf['Connector']['qsize']),
              qinterval=readCnf['Connector']['qinterval'])