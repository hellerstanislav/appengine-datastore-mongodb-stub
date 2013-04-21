
import unittest

from pymongo.errors import AutoReconnect

from google.appengine.datastore import datastore_stub_util
from google.appengine.datastore.datastore_mongodb_stub import DatastoreMongoDBStub
from google.appengine.ext import ndb
from google.appengine.ext import testbed


def atexit_deactivate_stub(f):
    def wrapper(*args, **kwargs):
        f(*args, **kwargs)
        self = args[0]
        self.testbed.init_datastore_v3_stub(enable=False)
    return wrapper

class TestPatchedTestbedInitialization(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.testbed = testbed.Testbed()
        cls.testbed.activate()

    @classmethod
    def tearDownClass(cls):
        cls.testbed.deactivate()

    def test_simple_init(self):
        # test if it initialized really mongodb stub
        self.testbed.init_datastore_v3_stub(use_mongodb=True)
        stub = self.testbed.get_stub('datastore_v3')
        self.assertIsInstance(stub, DatastoreMongoDBStub)
        # after deactivation should be stub None
        self.testbed.init_datastore_v3_stub(enable=False)
        self.assertIsNone(self.testbed.get_stub('datastore_v3'))

    def test_invalid_init(self):
        with self.assertRaises(ValueError):
            self.testbed.init_datastore_v3_stub(use_mongodb=True, use_sqlite=True)

    def test_init_consistency_policy(self):
        cp = datastore_stub_util.PseudoRandomHRConsistencyPolicy()
        self.testbed.init_datastore_v3_stub(use_mongodb=True,
                                            consistency_policy=cp)
        stub = self.testbed.get_stub('datastore_v3')
        self.assertEquals(cp, stub._consistency_policy)
        # deactivate stub
        self.testbed.init_datastore_v3_stub(enable=False)

    def test_init_host_port(self):
        with self.assertRaises(AutoReconnect):
            self.testbed.init_datastore_v3_stub(use_mongodb=True,
                                                mongodb_host='some.host',
                                                mongodb_port=55555)



class TestPatchedTestbedUsage(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        t = testbed.Testbed()
        t.activate()
        t.init_datastore_v3_stub(use_mongodb=True)
        t.init_memcache_stub()
        cls.testbed = t

    @classmethod
    def tearDownClass(cls):
        # clear datastore
        stub = cls.testbed.get_stub('datastore_v3')
        stub.Clear()
        # deactivate testbed
        cls.testbed.deactivate()

    def _get_pymongo_db_from_stub(self):
        # ugly, but works fine
        stub = self.testbed.get_stub('datastore_v3')
        return stub._mongods._db

    def test_insert_and_query(self):
        class A(ndb.Model):
            s = ndb.StringProperty()
            i = ndb.IntegerProperty()
        a = A(s='a',i=3)
        k = a.put()
        # check if it is really in the mongodb
        db = self._get_pymongo_db_from_stub()
        c = db.a.find().count()
        self.assertEquals(c, 1)
        k.delete()

