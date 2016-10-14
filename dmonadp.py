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
import datetime
import time


def main(argv):
    settings = {
        "esendpoint": None,
        "train": False,
        "file": None,
        "method": None,
        "validate": False,
        "export": None,
        "detect": None
    }
    try:
        opts, args = getopt.getopt(argv, "he:tf:m:vx:d:", ["endpoint=", "file=", "method=", "export=", "detect="])
    except getopt.GetoptError:
        logger.warning('[%s] : [WARN] Invalid argument received exiting', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        print "dmonadp.py -f <filelocation>, -t -m <method> -v -x <modelname>"
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'Help message!'
            sys.exit()
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

    print "#" * 20
    print "Starting DICE Anomaly detection framework"
    print "Initializing ..."
    print "Trying to read configuration file ..."
    if settings["file"] == None:
        if os.path.isfile('dmonadp.ini'):
            readCnf = readConf('dmonadp.ini')
            print "Reading configuration file ..."
            print "Index name -> %s" %readCnf['Connector']['indexname']
            print "Monitoring Endpoint -> %s" %readCnf['Connector']['esendpoint']
            print "Validate -> %s" %readCnf['Connector']['validate']
            print "Method -> %s" %readCnf['Connector']['method']
            print "Settings for method %s: " %readCnf['Connector']['method']
            for name, value in readCnf['MethodSettings'].iteritems():
                print "%s -> %s" %(name, value)
            print readCnf
    else:
        if os.path.isfile(settings["file"]):
            print "Found config file found !"
            logger.info('[%s] : [INFO] Config file found', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        else:
            logger.warning('[%s] : [WARN] Config file not found', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            print "Config File not found !"


    #if settings["esendpoint"] == None:


    dmonC = Connector('85.120.206.27')

    print dmonC
    print settings
    print "#" * 20


if __name__ == "__main__":
    main(sys.argv[1:])
