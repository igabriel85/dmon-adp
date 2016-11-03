"""
Copyright 2016, Institute e-Austria, Timisoara, Romania
    http://www.ieat.ro/
Developers:
 * Gabriel Iuhasz, iuhasz.gabriel@info.uvt.ro

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at:
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import sys, getopt
import os.path
from dmonconnector import Connector
from adpconfig import readConf
from adplogger import logger
from datetime import datetime
import time


def main(argv):
    settings = {
        "esendpoint": None,
        "esInstanceEndpoint": 9200,
        "dmonPort": 5001,
        "index": "logstash-*",
        "from": None, # timestamp
        "to": None, # timestamp
        "query": None,
        "qsize": None,
        "qinterval": None,
        "train": None, # Bool default None
        "model":None,
        "load":None,
        "file": None,
        "method": None,
        "validate": None, # Bool default None
        "export": None,
        "detect": None, # Bool default None
        "sload": None,
        "smemory": None,
        "snetwork": None
    }

    # Only for testing
    settings['train'] = True
    settings['validate'] = False
    settings['detect'] = False

    try:
        opts, args = getopt.getopt(argv, "he:tf:m:vx:d:lq:", ["endpoint=", "file=", "method=", "export=", "detect=", "query="])  # todo:expand comand line options
    except getopt.GetoptError:
        logger.warning('[%s] : [WARN] Invalid argument received exiting', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        print "dmonadp.py -f <filelocation>, -t -m <method> -v -x <modelname>"
        sys.exit(0)
    for opt, arg in opts:
        if opt == '-h':
            print 'Help message!'
            sys.exit(0)
        elif opt in ("-e", "--endpoint"):
            settings['esendpoint'] = arg
        elif opt in ("-t"):
            settings["train"] = True
        elif opt in ("-f", "--file"):
            settings["file"] = arg
        elif opt in ("-m", "--method"):
            settings["method"] = arg
        elif opt in ("-v"):
            settings["validate"] = True
        elif opt in ("-x", "--export"):
            settings["export"] = arg
        elif opt in ("-d", "--detect"):
            settings["detect"] = arg
        elif opt in ("-l", "--list-models"):
            print "Return current saved models based on Method"  # TODO
        elif opt in ("-q", "--query"):
            settings["query"] = arg

    print "#" * 100
    print "Starting DICE Anomaly detection framework"
    print "Initializing ..."
    print "Trying to read configuration file ..."

    file_conf = ''
    if settings["file"] is None:
        file_conf = 'dmonadp.ini'
        logger.info('[%s] : [INFO] Settings file set to %s',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), file_conf)
    else:
        if os.path.isfile(settings["file"]):
            file_conf = settings["file"]
            logger.info('[%s] : [INFO] Settings file set to %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), file_conf)
        else:
            logger.error('[%s] : [ERROR] Settings file not found at locations %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["file"])
            sys.exit(1)

    readCnf = readConf(file_conf)
    print "Reading configuration file ..."
    try:
        print "Index name -> %s" %readCnf['Connector']['indexname']
    except:
        logger.warning('[%s] : [WARN] Index not set in conf setting to default value %s',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['index'])
    if settings['esendpoint'] is None:
        try:
            print "Monitoring Endpoint -> %s" %readCnf['Connector']['esendpoint']
            settings['esendpoint'] = readCnf['Connector']['esendpoint']
            logger.info('[%s] : [INFO] Enpoint set to %s',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['esendpoint'])
        except:
            logger.error('[%s] : [ERROR] Enpoint not set in conf or commandline!',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            sys.exit(1)
    else:
        print "Monitoring Endpoint -> %s" % settings['esendpoint']
        logger.info('[%s] : [INFO] Enpoint set to %s',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['esendpoint'])
    if settings["from"] is None:
        try:
            print "From timestamp -> %s" % readCnf['Connector']['from']
            settings["from"] = readCnf['Connector']['from']
            logger.info('[%s] : [INFO] From timestamp set to %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                        settings["from"])
        except:
            logger.error('[%s] : [ERROR] From timestamp not set in conf or commandline!',
                         datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            sys.exit(1)
    else:
        print "From timestamp -> %s" % settings["from"]
        logger.info('[%s] : [INFO] From timestamp set to %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['from'])

    if settings["to"] is None:
        try:
            print "To timestamp -> %s" %readCnf['Connector']['to']
            settings["to"] = readCnf['Connector']['to']
            logger.info('[%s] : [INFO] To timestamp set to %s',
                                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                                settings["to"])
        except:
            logger.error('[%s] : [ERROR] To timestamp not set in conf or commandline!',
                                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            sys.exit(1)
    else:
        print "To timestamp -> %s" %settings["to"]
        logger.info('[%s] : [INFO] To timestamp set to %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['to'])

    if settings['query'] is None:
        try:
            print "Query -> %s" %readCnf['Connector']['query']
            settings['query'] = readCnf['Connector']['query']
            logger.info('[%s] : [INFO] Query set to %s',
                                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                                settings['query'])
        except:
            logger.error('[%s] : [ERROR] Query not set in conf or commandline!',
                                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            sys.exit(1)
    else:
        print "Query -> %s" % settings['query']
        logger.info('[%s] : [INFO] Query set to %s',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['query'])

    if settings["qsize"] is None:
        try:
            print "Query size -> %s" %readCnf['Connector']['qsize']
            settings["qsize"] = readCnf['Connector']['qsize']
            logger.info('[%s] : [INFO] Query size set to %s',
                                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                                settings['qsize'])
        except:
            logger.warning('[%s] : [WARN] Query size not set in conf or commandline setting to default',
                                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            settings["qsize"] = 'default'
            print "Query size -> %s" % settings["qsize"]
    else:
        print "Query size -> %s" %settings['qsize']
        logger.info('[%s] : [INFO] Query size set to %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["qsize"])

    if settings["qinterval"] is None:
        try:
            print "Query interval -> %s" %readCnf['Connector']['qinterval']
            settings["qinterval"] = readCnf['Connector']['qinterval']
            logger.info('[%s] : [INFO] Query interval set to %s',
                                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                                settings['qinterval'])
        except:
            logger.warning('[%s] : [WARN] Query interval not set in conf or commandline setting to default',
                                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            settings["qsize"] = "default"
    else:
        print "Query interval -> %s" % settings["qinterval"]
        logger.info('[%s] : [INFO] Query interval set to %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["qinterval"])

    if settings["train"] is None:
        try:
            print "Mode Training -> %s" %readCnf['Mode']['training']
            settings["train"] = readCnf['Mode']['training']
            logger.info('[%s] : [INFO] Train is set to %s from conf',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['train'])
        except:
            logger.error('[%s] : [ERROR] Train is not set in conf or comandline!',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            sys.exit(1)
    else:
        print "Mode Training -> %s" %settings['train']
        logger.info('[%s] : [INFO] Train is set to %s from comandline',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['train'])

    if settings["validate"] is None:
        try:
            print "Mode Validate -> %s" % readCnf['Mode']['validate']
            settings["validate"] = readCnf['Mode']['validate']
            logger.info('[%s] : [INFO] Validate is set to %s from conf',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['validate'])
        except:
            logger.error('[%s] : [ERROR] Validate is not set in conf or comandline!',
                                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            sys.exit(1)
    else:
        print "Mode Validate -> %s" % settings['validate']
        logger.info('[%s] : [INFO] Validate is set to %s from comandline',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['validate'])

    if settings["detect"] is None:
        try:
            print "Mode Detect -> %s" %readCnf['Mode']['detect']
            settings["detect"] = readCnf['Mode']['detect']
            logger.info('[%s] : [INFO] Detect is set to %s from conf',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['detect'])
        except:
            logger.error('[%s] : [ERROR] Detect is not set in conf or comandline!',
                                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            sys.exit(1)
    else:
        print "Mode Detect -> %s" % settings['detect']
        logger.info('[%s] : [INFO] Detect is set to %s from comandline',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['detect'])

    if settings["method"] is None:
        try:
            print "Detect Method -> %s" %readCnf['Detect']['method']
            settings["method"] = readCnf['Detect']['method']
            logger.info('[%s] : [INFO] Method is set to %s from conf',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["method"])
        except:
            logger.error('[%s] : [ERROR] Method is not set in conf or comandline!',
                                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            sys.exit(1)
    else:
        print "Detect Method -> %s" % settings["method"]
        logger.info('[%s] : [INFO] Method is set to %s from comandline',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["method"])

    if settings["model"] is None:
        try:
            print "Detect Model -> %s" %readCnf['Detect']['model']
            settings["model"] = readCnf['Detect']['model']
            logger.info('[%s] : [INFO] Model is set to %s from conf',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["model"])
        except:
            logger.error('[%s] : [ERROR] Model is not set in conf or comandline!',
                                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            sys.exit(1)
    else:
        print "Detect Model -> %s" %settings['model']
        logger.info('[%s] : [INFO] Model is set to %s from comandline',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["model"])

    if settings["export"] is None:
        try:
            print "Detect Export -> %s" %readCnf['Detect']['export']
            settings["export"] = readCnf['Detect']['export']
            logger.info('[%s] : [INFO] Export is set to %s from conf',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["export"])
        except:
            logger.error('[%s] : [ERROR] Export is not set in conf or comandline!',
                                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            sys.exit(1)
    else:
        print "Detect Export -> %s" % settings["export"]
        logger.info('[%s] : [INFO] Model is set to %s from comandline',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["export"])

    if settings["load"] is None:
        try:
            print "Detect Load -> %s" %readCnf['Detect']['load']
            settings["load"] = readCnf['Detect']['load']
            logger.info('[%s] : [INFO] Load is set to %s from conf',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["load"])
        except:
            logger.error('[%s] : [ERROR] Load is not set in conf or comandline!',
                                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            sys.exit(1)
    else:
        print "Detect Load -> %s" % settings['load']
        logger.info('[%s] : [INFO] Load is set to %s from comandline',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["load"])

    try:
        print "Settings for method %s: " % settings['method']
        settings['MethodSettings'] = {}
        for name, value in readCnf['MethodSettings'].iteritems():
            print "%s -> %s" % (name, value)
            settings['MethodSettings'][name] = value
    except:
        print "No Method settings detected, using defaults for %s!" %settings["method"]
        settings['MethodSettings'] = 'default'
        logger.warning('[%s] : [WARN] No Method settings detected, using defaults for %s!',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["method"])

    try:
        print "Point Memory -> %s" %readCnf['Point']['memory']
        settings["smemory"] = readCnf['Point']['memory']
        logger.info('[%s] : [INFO] System memory is set to %s',
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["smemory"])
    except:
        print "Point Memory not set using default!"
        settings["smemory"] = "default"
        logger.warning('[%s] : [WARN] System memory is not set, using default!',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

    try:
        print "Point CPU Load -> %s" %readCnf['Point']['load']
        settings["sload"] = readCnf['Point']['load']
        logger.info('[%s] : [INFO] System load is  set to %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["sload"])
    except:
        print "Point Load is not set, using default!"
        settings["sload"] = "default"
        logger.warning('[%s] : [WARN] System load is not set, using default!',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

    try:
        print "Point Network -> %s" %readCnf['Point']['network']
        settings["snetwork"] = readCnf['Point']['network']
        logger.info('[%s] : [INFO] System netowrk is  set to %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["snetwork"])
    except:
        print "Point Network is not set, using default!"
        settings["snetwork"] = "default"
        logger.warning('[%s] : [WARN] System netowrk is not set, using default!',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))



    #if settings["esendpoint"] == None:

    #dmonC = Connector('85.120.206.27')

    #print dmonC
    print "Conf file -> %s" %readCnf
    print "Settings  -> %s" %settings
    print "#" * 100


if __name__ == "__main__":
    main(sys.argv[1:])
