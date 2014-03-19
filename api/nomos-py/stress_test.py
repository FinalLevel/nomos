#!/usr/bin/env python
#coding=utf-8

import random
import threading
from nomos import Nomos
from threading import Thread
from time import sleep
from datetime import datetime, timedelta
import pickle
from array import *
from optparse import OptionParser

class TestClass(object):
    @classmethod
    def __init__(self):
        self._test = "Bla bla bla";
        self._array = array('i', [ 1,3,4 ])
        self._array.insert(100, 4)

testClass = TestClass()
data = pickle.dumps(testClass)

class TestStats(object):
    @classmethod
    def __init__(self):
        self._connections = 0
        self._puts = 0
        self._gets = 0
        self._getsTime = timedelta()
        self._errors = 0
        self._lock = threading.Lock()
    def addConnection(self):
        self._lock.acquire()
        self._connections += 1
        self._lock.release()

    def addPut(self):
        self._lock.acquire()
        self._puts += 1
        self._lock.release()

    def addGet(self, time_delta):
        self._lock.acquire()
        self._gets += 1
        self._getsTime += time_delta
        self._lock.release()

    def addError(self):
        self._lock.acquire()
        self._errors += 1
        self._lock.release()

    def printStat(self):
        print "Stress test stat: connections: " + str(self._connections) + ",puts: " + str(self._puts) + ",gets: " \
              + str(self._gets) +" (" + str(self._getsTime) + "), errors: " + str(self._errors)


def nomos_stress_test(requests, putQueries, getQueries):
    for r in range(requests):
        nomosClient = Nomos("192.168.81.128")
        testStats.addConnection()
        keys = []
        for putQ in range(putQueries):
            key = random.randint(1, 1000000)
            keys.append(key)
            testStats.addPut()
            if not nomosClient.put(1,2,key,3600, data):
                print "Put error on step " + str(putQ)
                testStats.addError()
                return


        for getQ in range(getQueries):
            for k in keys:
                start_time = datetime.now()
                if not nomosClient.get(1, 2, key):
                    print "Get error on step " + str(getQ)
                    testStats.addError()
                    return
                testStats.addGet(datetime.now() - start_time)



if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-c", "--concurrency", dest="concurrency",
                      help="set thread number", default=10)
    parser.add_option("-r", "--requests",
                      dest="requests", default=10,
                      help="set request number")

    parser.add_option("-p", "--puts",
                      dest="puts", default=10,
                      help="set put queries number per request")

    parser.add_option("-g", "--gets",
                      dest="gets", default=10,
                      help="set get queries number per put")

    (options, args) = parser.parse_args()
    testStats = TestStats()
    start_time = datetime.now()
    threadList = []
    for i in range(int(options.concurrency)):
        thread = Thread(target = nomos_stress_test, args = (int(options.requests), int(options.puts),
                                                            int(options.gets)))
        thread.start()
        threadList.append(thread)
    for thread in threadList:
        thread.join()
    testStats.printStat()
    print "Total: " + str(datetime.now() - start_time)
