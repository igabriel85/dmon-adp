import multiprocessing
import time


def test(times, processID):
    print "Starting engine Point process %s" % processID
    time.sleep(times)
    print "Exit process %s" % processID


class AdpPointProcess():
    def __init__(self, engine, processID):
        self.processID = processID
        self.engine = engine

    def run(self):
        print "Starting engine point process %s" % self.processID
        p = multiprocessing.Process(target=self.engine.detectPointAnomalies)
        return p


class AdpTrainProcess():
    def __init__(self, engine, processID):
        self.processID = processID
        self.engine = engine

    def run(self):
        # time.sleep(10)
        print "Starting engine Train process %s" % self.processID
        p = multiprocessing.Process(target=self.engine.trainMethod)
        return p


class AdpDetectProcess():
    def __init__(self, engine, processID):
        self.processID = processID
        self.engine = engine

    def run(self):
        p = multiprocessing.Process(target=self.engine.detectAnomalies)
        return p


# jobs = []
# testProcess = AdpPointProcess('engine', 'PointProcess')
# testProcess2 = AdpTrainProcess('engine', 'TrainProcess')
# testProcess3 = AdpDetectProcess('engine', 'Detect')
# initProcess = testProcess.run()
# jobs.append(initProcess)
# initProcess2 = testProcess2.run()
# jobs.append(initProcess2)
# initProcess3 = testProcess3.run()
# jobs.append(initProcess3)
#
# initProcess.start()
# initProcess2.start()
# initProcess3.start()
#
# for j in jobs:
#     j.join()
#     print '%s.exitcode = %s' % (j.name, j.exitcode)
