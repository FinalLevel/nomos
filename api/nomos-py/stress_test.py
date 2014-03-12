from nomos import Nomos

import pickle
from array import *

class TestClass(object):
    @classmethod
    def __init__(self):
        self._test = "Bla bla bla";
        self._array = array('i', [ 1,3,4 ])


testClass = TestClass()

nomosClient = Nomos("192.168.81.128")
data = pickle.dumps(testClass)
res = nomosClient.put(1,2,3,3600, data)
res = nomosClient.get(1,2,3)
testUnpackClass = pickle.loads(res)
print res
nomosClient.remove(1,2,3)
print nomosClient.get(1,2,3)

