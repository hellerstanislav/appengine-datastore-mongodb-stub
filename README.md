=========================================
MongoDB Datastore Stub for App Engine SDK
=========================================
AppEngine Datastore Stub using MongoDB database as a backend. This is a replacement for
App Engine's default DatastoreFileStub and DatastoreSQLiteStub. Slightly inspired by
Mike Dirolf's and Tobias Rodabel's Mongo Appengine Connector (thx).

Now adapted to SDK 1.7.4 with features of ndb like structured properties etc.

Notes
=====
* Missing tests for threaded environment.
* Missing tests for expando models and polymodel.
* Query projection not supported yet.
* Index treating not supported yet.
* Transactions unsupported.
* Problem with natvie ordering - sometimes it happens to upper layer (ndb) giving entities
  to put into datastore in wrong order. Then some tests fail because default ordering should
  be by insert time, which is wrong in this case.
