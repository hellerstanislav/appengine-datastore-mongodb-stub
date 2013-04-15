=========================================================
MongoDB Datastore Stub for Google App Engine SDK (Python)
=========================================================
AppEngine Datastore Stub using [MongoDB](http://www.mongodb.org/) database as a backend.
This is a replacement for App Engine's default DatastoreFileStub and DatastoreSQLiteStub.
Slightly inspired by Mike Dirolf's and Tobias Rodabel's
[Mongo Appengine Connector](https://github.com/mdirolf/mongo-appengine-connector) (thx).

**Features:**
* provides much faster queries than DatastoreFileStub (10-100 times faster filtering, 5x faster ordering, depends on dataset size)
* provides much faster inserts than DatastoreSqliteStub (2-3 times faster on large datasets)
* compatibility with SDK 1.7.6
* support for **devappserver2**
* supported features of ndb like structured properties, query projection etc.
* optimized for highest performance (safe=False for pymongo < 2.4, write concern w=0 for pymongo >= 2.4).
* tested against behaviour of DatastoreFileStub.

**Dependencies:**
* python 2.7 (tested on 2.7.3 [GCC 4.7.0 20120507 (Red Hat 4.7.0-5)])
* pymongo >= 2.0 (for versions < 2.0 not tested)
* mongodb >= 2.0


Install
=======
This way you can install the stub directly into your SDK. Warning: some of the files of the SDK
will be overwriten (patched). Since SDK 1.7.6 introduces devappserver2, which supports DatastoreSqliteStub
only, there's no --use_sqlite option in dev_appserver. Patching will overwrite hard-wired sqlite
stub into mongodb stub. (For more info, see patch file).


1. Extract downloaded zip archive (or clone this repo). Enter the appengine-datastore-mongodb-stub dir.
```bash
$ unzip appengine-datastore-mongodb-stub.zip
$ cd appengine-datastore-mongodb-stub
```

2. Install the stub.
```bash
$ sh install.sh /PATH/TO/YOUR/APPENGINE/SDK
```

Usage
=====
**devappserver2:**

You may start your dev_appserver now. This way your data will be stored in mongodb databse named
after your app ID. For example:
```bash
$ python ./google_appengine/dev_appserver.py $PROJECT_DIR
```

**old_dev_appserver:**

If you are still using an old development server, you may start it now. Using `--use_mongodb` flag
your data will be stored in mongodb databse named after your app ID. For example:
```bash
$ python ./google_appengine/old_dev_appserver.py --use_mongodb $PROJECT_DIR
```

Usage in tests
==============
```python
import unittest
import os

from google.appengine.api import apiproxy_stub_map
from google.appengine.api.memcache import memcache_stub

from datastore_mongodb_stub import DatastoreMongoDBStub

APP_ID = 'test'

class MyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Set up SDK testing environment
        """
        os.environ['APPLICATION_ID'] = APP_ID
        apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
        # memcache stub
        cache_stub = memcache_stub.MemcacheServiceStub()
        apiproxy_stub_map.apiproxy.RegisterStub('memcache', cache_stub)
        # datastore stub
        datastore_stub = DatastoreMongoDBStub(APP_ID,
                                              require_indexes=False,
                                              mongodb_host='localhost',
                                              mongodb_port=27017)
        # we can now edit pymongo.MongoClient's write_concern to use journaling
        # this option is only for pymongo version >= 2.4
        datastore_stub._mongods.write_concern['j'] = True
        apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', datastore_stub)
```

Maybe someday here will be patch for testbed if needed.

Notes
=====
* Tested only on ndb (google.appengine.ext.ndb).
* Missing tests for threaded environment.
* Query projection on multiple repeated properties not supported.

