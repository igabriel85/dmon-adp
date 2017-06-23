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
from adpengine import dmonadpengine
from util import getModelList, parseDelay
import time
from dmonweka import *
from dataformatter import DataFormatter
import tempfile



def main(argv):
    dataDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    modelsDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
    queryDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'queries')

    settings = {
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
        "checkpoint": None,
        "delay": None,
        "interval": None,
        "resetindex": None,
        "training":None,
        "validation":None,
        "validratio":0.0,
        "compare": False,
        "anomalyOnly": False,
        "categorical": None
    }

    # Only for testing

    settings['validate'] = False

    try:
        opts, args = getopt.getopt(argv, "he:tf:m:vx:d:lq:", ["endpoint=", "file=", "method=", "export=", "detect=", "query="])  # todo:expand comand line options
    except getopt.GetoptError:
        logger.warning('[%s] : [WARN] Invalid argument received exiting', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        print "dmonadp.py -f <filelocation>, -t -m <method> -v -x <modelname>"
        sys.exit(0)
    for opt, arg in opts:
        if opt == '-h':
            print "#" * 100
            print 'DICE Anomaly detection Tool'
            print 'Utilisation:'
            print '-f -> condifuration file location'
            print '-t -> activate training mode'
            print '-m -> methods'
            print '   -> allowed methods: skm, em, dbscan, sdbscan, isoforest'
            print '-x -> export model name'
            print '-v -> validation'
            print '-q -> query string for anomaly detection'
            print "#" * 100
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
            print "Current saved models are:\n"
            print getModelList()
            sys.exit(0)
        elif opt in ("-q", "--query"):
            settings["query"] = arg

    print "#" * 100
    print "Starting DICE Anomaly detection framework"
    print "Initializing ..."
    print "Trying to read configuration file ..."
    print queryDir

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

    if settings["nodes"] is None:
        try:
            if not readCnf['Connector']['nodes']:
                readCnf['Connector']['nodes'] = 0
            print "Desired Nodes -> %s" % readCnf['Connector']['nodes']
            settings["nodes"] = readCnf['Connector']['nodes']
            logger.info('[%s] : [INFO] Desired nodes set to %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                    settings['nodes'])
        except:
            logger.warning('[%s] : [WARN] No nodes selected from config file or comandline querying all',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            settings["nodes"] = 0
    else:
        print "Desired Nodes -> %s" % settings["nodes"]
        logger.info('[%s] : [INFO] Desired nodes set to %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["nodes"])

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

    if settings["type"] is None:
        try:
            print "Detect Type -> %s" %readCnf['Detect']['type']
            settings["type"] = readCnf['Detect']['type']
            logger.info('[%s] : [INFO] Type is set to %s from conf',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["type"])
        except:
            logger.error('[%s] : [ERROR] Type is not set in conf or comandline!',
                                 datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            sys.exit(1)
    else:
        print "Detect Type -> %s" %settings['type']
        logger.info('[%s] : [INFO] Type is set to %s from comandline',
                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["type"])

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
        settings['MethodSettings'] = {}   #todo read settings from commandline ?
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

    try:
        print "Heap size set to -> %s" %readCnf['Misc']['heap']
        settings['heap'] = readCnf['Misc']['heap']
        logger.info('[%s] : [INFO] Heap size set to %s',
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['heap'])
    except:
        print "Heap size not defined using default"
        settings['heap'] = '512m'
        logger.info('[%s] : [INFO] Heap size set to default %s',
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['heap'])

    try:
        if readCnf['Filter']['columns']:
            print "Filter columns -> %s" %readCnf['Filter']['columns']
            settings["cfilter"] = readCnf['Filter']['columns']
        else:
            print "Filter columns -> %s" % settings["cfilter"]
    except:
        print "Filter columns -> %s" % settings["cfilter"]
    finally:
        logger.info('[%s] : [INFO] Filter column set to %s',
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['cfilter'])

    try:
        print "Filter rows -> %s" %readCnf['Filter']['rows']
        settings["rfilter"] = readCnf['Filter']['rows']
    except:
        print "Filter rows -> %s" %settings["rfilter"]
    finally:
        logger.info('[%s] : [INFO] Filter rows set to %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['rfilter'])

    try:
        if readCnf['Filter']['dcolumns']:
            print "Filter drop columns -> %s" % readCnf['Filter']['dcolumns']
            settings["dfilter"] = readCnf['Filter']['dcolumns']
        else:
            print "Filter drop columns -> %s" % settings["dfilter"]
    except:
        print "Filter drop columns -> %s" % settings["dfilter"]
    finally:
        logger.info('[%s] : [INFO] Filter drop column set to %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['dfilter'])

    if settings["checkpoint"] is None:
        try:
            print "Checkpointing is set to %s" %readCnf['Misc']['checkpoint']
            settings["checkpoint"] = readCnf['Misc']['checkpoint']
            logger.info('[%s] : [INFO] Checkpointing is  set to %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['checkpoint'])
        except:
            print "Checkpointing not set using default"
            settings["checkpoint"] = "True"
            logger.info('[%s] : [INFO] Checkpointing is  set to True',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    else:
        print "Checkpointing is set to %s" %settings["checkpoint"]
        logger.info('[%s] : [INFO] Checkpointing is  set to %s',
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['checkpoint'])

    if settings["delay"] is None:
        try:
            print "Delay is set to %s" %readCnf['Misc']['delay']
            settings["delay"] = readCnf['Misc']['delay']
            logger.info('[%s] : [INFO] Delay is  set to %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['delay'])
        except:
            print "Delay is not set, setting default"
            settings["delay"] = "2m"
            logger.info('[%s] : [INFO] Delay is  set to %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['delay'])
    else:
        print "Delay is set to %s" % settings["delay"]
        logger.info('[%s] : [INFO] Delay is  set to %s',
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['delay'])

    if settings["interval"] is None:
        try:
            print "Interval is set to %s" % readCnf['Misc']['interval']
            settings["interval"] = readCnf['Misc']['interval']
            logger.info('[%s] : [INFO] Interval is  set to %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['interval'])
        except:
            print "Interval is not set, setting default"
            settings["interval"] = "15m"
            logger.info('[%s] : [INFO] Interval is  set to %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['interval'])
    else:
        print "Interval is set to %s" % settings["interval"]
        logger.info('[%s] : [INFO] Interval is  set to %s',
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['interval'])

    if settings["resetindex"] is None:
        try:
            print "Reset index set to %s" % readCnf['Misc']['resetindex']
            settings["resetindex"] = readCnf['Misc']['resetindex']
        except:
            print "Reset index not set, skipping."
            settings["resetindex"] = False
    else:
        print "Reset index set to %s" % settings["resetindex"]
    logger.info('[%s] : [INFO] Reset index set to %s',
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['resetindex'])

    try:
        print "DMon Port is set to %s" % readCnf['Connector']['dmonport']
        settings['dmonPort'] = readCnf['Connector']['dmonport']
    except:
        print "DMon Port is set to default %s" % str(5001)
    logger.info('[%s] : [INFO] DMon Port is set to %s"',
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(settings['dmonPort']))

    try:
        print "Classification Training set is %s" % readCnf['Detect']['training']
        settings['training'] = readCnf['Detect']['training']
    except:
        print "Classification training set is default"
    logger.info('[%s] : [INFO] Classification Training set is %s',
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(settings['training']))

    try:
        print "Classification Validation set is %s" % readCnf['Detect']['validation']
        settings['validation'] = readCnf['Detect']['validation']
    except:
        print "Classification Validation set is default"
    logger.info('[%s] : [INFO] Classification Validation set is %s',
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(settings['validation']))

    try:
        print "Classification validation ratio is set to %d" % int(readCnf['Detect']['validratio'])
        if float(readCnf['Detect']['validratio']) > 1.0:
            print "Validation ratio is out of range, must be between 1.0 and 0.1"
            settings['validratio'] = 0.0
        settings['validratio'] = float(readCnf['Detect']['validratio'])
    except:
        print "Classification Validation ratio is default"
    logger.info('[%s] : [INFO] Classification Validation ratio is %s',
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(settings['validratio']))

    try:
        print "Classification comparison is set to %s" % readCnf['Detect']['compare']
        settings['compare'] = readCnf['Detect']['compare']
    except:
        print "Classification comarison is default"
    logger.info('[%s] : [INFO] Classification comparison is %s',
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings['compare'])

    try:
        print "Classification data generation using only anomalies set to %s" % readCnf['Detect']['anomalyOnly']
        settings['anomalyOnly'] = readCnf['Detect']['anomalyOnly']
    except:
        print "Classification data generation using only anomalies set to False"
    logger.info('[%s] : [INFO] Classification data generation using only anomalies set to %s',
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), str(settings['anomalyOnly']))

    if settings["categorical"] is None:
        try:
            if not readCnf['Connector']['categorical']:
                readCnf['Connector']['categorical'] = 0
            print "Categorical Features -> %s" % readCnf['Connector']['categorical']
            if readCnf['Connector']['categorical'] == '0':
                settings["categorical"] = None
            else:
                settings["categorical"] = readCnf['Connector']['categorical']
            logger.info('[%s] : [INFO] Categorical Features ->  %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                    settings['categorical'])
        except:
            logger.warning('[%s] : [WARN] No Categorical Features selected from config file or comandline! Skipping encoding',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            settings["categorical"] = 0
    else:
        print "Categorical Features -> %s" % settings["categorical"]
        logger.info('[%s] : [INFO] Categorical Features ->  %s',
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), settings["categorical"])

    #if settings["esendpoint"] == None:

    #dmonC = Connector('85.120.206.27')

    #print dmonC
    print "Conf file -> %s" %readCnf
    print "Settings  -> %s" %settings


    # Begin testing settings
    # settings = {'load': 'test1', 'qsize': '0', 'dfilter': None, 'export': 'test1', 'file': None, 'rfilter': None, 'query': 'cassandra', 'index': 'lscassandra', 'detect': 'false', 'from': '1481569200000', 'checkpoint': 'false', 'to': '1481580000000', 'sload': 'shortterm:gd:2.0;midterm:ld:0.1;longterm:gd:1.0', 'nodes': 0, 'type': 'clustering', 'method': 'isoforest', 'snetwork': 'tx:gd:34344;rx:ld:323434', 'resetindex': 'false', 'interval': '15m', 'train': 'true', 'esInstanceEndpoint': 9200, 'heap': '512m', 'validate': False, 'qinterval': '20s', 'dmonPort': '5001', 'esendpoint': '85.120.206.27', 'smemory': 'cached:gd:231313;buffered:ld:312123;used:ld:12313;free:gd:23123', 'delay': '2m', 'MethodSettings': {'max_samples': '100', 'n_jobs': '1', 'verbose': '0', 'bootstrap': 'False', 'n_estimators': '100', 'random_state': 'None', 'contamination': '0.01', 'max_features': '1.0'}, 'cfilter': None}
    # End testing settings

    engine = dmonadpengine.AdpEngine(settings, dataDir=dataDir, modelsDir=modelsDir, queryDir=queryDir)
    #engine.printTest()
    engine.initConnector()
    #
    #
    # engine.run(engine)
    engine.runProcess(engine)
    # systemReturn, yarnReturn, reducemetrics, mapmetrics, sparkReturn, stormReturn, cassandraReturn = engine.getData()
    # dformat = DataFormatter(dataDir)
    # test = dweka(dataDir, modelsDir)
    # options = ["-N", "10", "-S", "10"]
    #
    # # dataf = dformat.savetomemory(yarnReturn)
    # # dataf = dformat.df2cStringIO(yarnReturn)
    # dataf = tempfile.NamedTemporaryFile(suffix='.csv')
    # print dataf.name
    # # print dataf.file
    # # print dataf.star
    #
    # dformat.df2csv(yarnReturn, dataf.name)
    # test.simpleKMeansTrain(dataf.name, options)
    # anomalies = test.runclustermodel("skm", dataf.name)
    # for e in anomalies:
    #     # print ut2hum(e)
    #     print e / 1000
    # dataf.close()


    # print type(dataf.getvalue())
    # dataf = "cTest.csv"

    #
    # filtered_df = engine.filterData(yarnReturn)
    # filtered_df.to_csv(os.path.join(dataDir, 'ctest2.csv'), index=False)
    # test = systemReturn.set_index('key')
    # print test.to_dict()
    # engine.trainMethod()
    # engine.detectAnomalies(30)
    # engine.printTest()
    print "Exiting DICE Anomaly detection framework"
    print "\n"
    print "#" * 100


if __name__ == "__main__":
    main(sys.argv[1:])
