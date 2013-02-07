================================================
MongoDB Datastore Stub for Google App Engine SDK
================================================
AppEngine Datastore Stub using [MongoDB](http://www.mongodb.org/) database as a backend.
This is a replacement for App Engine's default DatastoreFileStub and DatastoreSQLiteStub.
Slightly inspired by Mike Dirolf's and Tobias Rodabel's
[Mongo Appengine Connector](https://github.com/mdirolf/mongo-appengine-connector) (thx).

Now adapted to SDK 1.7.4 with features of ndb like structured properties, query projection etc.

Optimized for highest performance (safe=False for pymongo < 2.4, write concern w=0 for pymongo >= 2.4).

Usage in tests
==============
```python
import unittest
import os

from google.appengine.api import apiproxy_stub_map
from google.appengine.api.memcache import memcache_stub

from datastore_mongodb_stub import DatastoreMongoDBStub

class MyTests(unittest.TestCase)
    def __init__(self, *args, **kwargs):
        """
        Set up SDK testing environment
        """
        super(MyTests, self).__init__(*args, **kwargs)
        self.app_id = 'test'
        os.environ['APPLICATION_ID'] = self.app_id
        apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
        # memcache stub
        cache_stub = memcache_stub.MemcacheServiceStub()
        apiproxy_stub_map.apiproxy.RegisterStub('memcache', cache_stub)
        # datastore stub
        self.datastore_stub = DatastoreMongoDBStub(self.app_id,
                                                   require_indexes=False,
                                                   service_name='datastore_v3',
                                                   consistency_policy=None,
                                                   mongodb_host='localhost',
                                                   mongodb_port=27017)
        # we can now edit pymongo.MongoClient's write_concern to use journaling
        # this option is only for pymongo version >= 2.4
        self.datastore_stub._datastore.write_concern['j'] = True
        apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', self.datastore_stub)
```

Maybe someday here will be patch for testbed if needed.

Notes
=====
* Tested only on ndb (google.appengine.ext.ndb).
* Missing tests for threaded environment.
* Missing tests for expando models and polymodel.
* Query projection on multiple repeated properties not supported yet.
* Index treating not supported yet.
* Transactions unsupported.
* Problem with native ordering - sometimes it happens to upper layer (ndb) giving entities
  to put into datastore in wrong order. Then some tests fail because default ordering should
  be by insert time, which is wrong in this case.
* Consistency policy is very hard to simulate. Not supported yet. :(

