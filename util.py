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

from weka.core.converters import Loader, Saver
from os import listdir
from os.path import isfile, join
import os
import csv
import pandas as pd


modelDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')


def convertCsvtoArff(indata, outdata):
    '''
    :param indata: -> input csv file
    :param outdata: -> output file
    :return:
    '''
    loader = Loader(classname="weka.core.converters.CSVLoader")
    data = loader.load_file(indata)
    saver = Saver(classname="weka.core.converters.ArffSaver")
    saver.save_file(data, outdata)


def queryParser(query):
    '''
    :param query: -> query of the form  {"Query": "yarn:resourcemanager, clustre, jvm_NM;system"}
    :return: -> dictionary of the form {'system': 0, 'yarn': ['resourcemanager', 'clustre', 'jvm_NM']}
    '''
    type = {}
    for r in query.split(';'):
        if r.split(':')[0] == 'yarn':
            try:
                type['yarn'] = r.split(':')[1].split(', ')
            except:
                type['yarn'] = 0
        if r.split(':')[0] == 'spark':
            try:
                type['spark'] = r.split(':')[1].split(', ')
            except:
                type['spark'] = 0
        if r.split(':')[0] == 'storm':
            try:
                type['storm'] = r.split(':')[1].split(', ')
            except:
                type['storm'] = 0
        if r.split(':')[0] == 'system':
            try:
                type['system'] = r.split(':')[1].split(', ')
            except:
                type['system'] = 0
    return type


def nodesParse(nodes):
    if not nodes:
        return 0
    return nodes.split(';')


def getModelList():
    onlyfiles = [f for f in listdir(modelDir) if isfile(join(modelDir, f))]
    return onlyfiles


def csvheaders2colNames(csvfile, adname, df=False):
    '''
    :param csvfile: -> input csv or dataframe
    :param adname: -> string to add to column names
    :param df: -> if set to false csvfile is used if not df is used
    :return:
    '''
    colNames = {}
    if not df:
        with open(csvfile, 'rb') as f:
            reader = csv.reader(f)
            i = reader.next()
        i.pop
        for e in i:
            if e == 'key':
                pass
            else:
                colNames[e] = '%s_%s' %(e, adname)
    else:
        for e in csvfile.columns.values:
            if e =='key':
                pass
            else:
                colNames[e] = '%s_%s' % (e, adname)
    return colNames


def str2Bool(st):
    '''
    :param st: -> string to test
    :return: -> if true then returns 1 else 0
    '''
    if st in ['True', 'true', '1']:
        return 1
    elif st in ['False', 'false', '0']:
        return 0
    else:
        return 0


# testcsv = "/Users/Gabriel/Documents/workspaces/diceWorkspace/dmon-adp/data/JVM_NM_dice.cdh.slave1.csv"
#
# print csvheaders2colNames(testcsv, 'slave1')




# print getModelList()
# query = "yarn:resourcemanager, clustre, jvm_NM;system"
# query2 = {"Query": "yarn;system;spark"}
# test = queryParser(query)
# print test
# print queryParser(query2)



