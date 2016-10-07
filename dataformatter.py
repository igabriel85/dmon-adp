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


class DataFormatter():

    def __init__(self):
        pass

    rawdataset = {}

    def getJson(self):
        return 'load Json'

    def filter(self):
        return 'filter dataset'

    def dict2csv(self):
        return "transform to csv"

    def dict2arff(self):
        return "transform to arff"

    def normalize(self):
        return "normalized dataset"