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
* compatibility with SDK 1.7.7
* support for **devappserver2**
* supported features of ndb like structured properties, query projection etc.
* optimized for highest performance (safe=False for pymongo < 2.4, write concern w=0 for pymongo >= 2.4).
* tested against behaviour of DatastoreFileStub.

**Dependencies:**
* python 2.7 (tested on 2.7.3 [GCC 4.7.0 20120507 (Red Hat 4.7.0-5)])
* pymongo >= 2.0 (for versions < 2.0 not tested)
* mongodb >= 2.0


Install (Linux)
===============
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


Install (Windows)
=================
For installation on Windows you will the `patch` utility. Make sure you have MongoDB running and pymongo installed.

1. Install `patch` utility (http://gnuwin32.sourceforge.net/packages/patch.htm), preferably version 2.5.9.7.
2. Run `cmd` (win command line)
  * Windows < Vista: run as usually
  * **Windows Vista,7,8**: Start -> type `cmd` and `Ctrl+Shift+Enter` to run everything as Administrator
3. Add `patch` to windows `PATH`, most commonly the path to patch is `"C:\Program Files\GnuWin32\bin\"`.
   If you are running on 64bit OS, the path is expected to be `"C:\Program Files (x86)\GnuWin32\bin\"`:
```dos
> PATH=%PATH%;"C:\Program Files (x86)\GnuWin32\bin\"
```

4. Download this repository as zip archive (or clone this repo). Extract it and enter the appengine-datastore-mongodb-stub
   directory.
5. Run install.bat with first param as path to google_appengine SDK, most commonly
`"C:\Program Files\Google\google_appengine\"` or on 64bit `"C:\Program Files (x86)\Google\google_appengine\"`:
```dos
> install.bat "C:\Program Files (x86)\Google\google_appengine\"
```
If the output is similar to this, you have successfully installed Datastore MongoDB Stub into the SDK:
```dos
Copying datastore mongodb stub into SDK...
        1 file(s) copied.
        1 file(s) copied.
Patching dev_appserver...
patching file appengine/tools/api_server.py
patching file appengine/tools/dev_appserver.py
patching file appengine/tools/dev_appserver_main.py
patching file appengine/tools/devappserver2/api_server.py
patching file appengine/tools/devappserver2/devappserver2.py
patching file appengine/ext/testbed/__init__.py
"Done."
```


### FAQ & Common Errors
* If you notice message `Access denied` while patching the SDK, you are not running the `cmd` as Administrator.
  Make sure you type `cmd` into the "Run" field in Start and press `Ctrl+Shift+Enter` to run as Administrator.
* If you see this traceback in your GAE SDK Launcher's Log: 

```python
Traceback (most recent call last):
  File "C:\Program Files (x86)\Google\google_appengine\dev_appserver.py", line 193, in <module>
    _run_file(__file__, globals())
  File "C:\Program Files (x86)\Google\google_appengine\dev_appserver.py", line 189, in _run_file
    execfile(script_path, globals_)
  File "C:\Program Files (x86)\Google\google_appengine\google\appengine\tools\devappserver2\devappserver2.py", line 31, in <module>
    from google.appengine.datastore.datastore_mongodb_stub import MongoDatastore
  File "C:\Program Files (x86)\Google\google_appengine\google\appengine\datastore\datastore_mongodb_stub.py", line 46, in <module>
    from pymongo import ASCENDING, DESCENDING
ImportError: No module named pymongo
2013-04-29 16:11:23 (Process exited with code 1)
```
* ..you probably did not install `pymongo` library.

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
Since SDK 1.7.7, you can use patched testbed - there is new option `use_mongodb` in
`Testbed.init_datastore_v3_stub`.
```python
import unittest

from google.appengine.ext import testbed

class MyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.testbed = testbed.Testbed()
        cls.testbed.activate()
        cls.testbed.init_datastore_v3_stub(use_mongodb=True)

    @classmethod
    def tearDownClass(cls):
        cls.testbed.deactivate()

    # ...a bunch of tests...
```
You can use `setUp` and `tearDown` methods as well, but be aware that when initializing
the mongodb stub it creates new Connection. Setting the connection before every test could
be a bit slow.

Or you can fully customize the initialization using low-level API:
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


Notes
=====
* Tested only on ndb (google.appengine.ext.ndb).
* Missing tests for threaded environment.
* Query projection on multiple repeated properties not supported.

