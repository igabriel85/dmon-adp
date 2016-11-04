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



# query = "yarn:resourcemanager, clustre, jvm_NM;system"
# query2 = {"Query": "yarn;system;spark"}
# test = queryParser(query)
# print test
# print queryParser(query2)


