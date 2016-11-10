import threading
import time


class AdpPointThread(threading.Thread):
    def __init__(self, engine, threadID):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.engine = engine

    def run(self):
        print "Starting engine point thread %s" % self.threadID
        self.engine.detectPointAnomalies()


class AdpTrainThread(threading.Thread):
    def __init__(self, engine, threadID):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.engine = engine

    def run(self):
        print "Starting engine train thread %s" % self.threadID
        self.engine.trainMethod()


class AdpDetectThread(threading.Thread):
    def __init__(self, engine, threadID):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.engine = engine

    def run(self):
        print "Starting engine detect thread %s" % self.threadID
        self.engine.detectAnomalies()
