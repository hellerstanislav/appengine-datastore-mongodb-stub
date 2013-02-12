#!/usr/bin/env python
#
# Copyright 2007 Google Inc., 2013 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
MongoDB-based stub for the Python datastore API.

Entities are stored in an MongoDB database as documents, properties store their
type if needed. DatastoreMongoDBStub manages an meta-collection named _schema,
where typed schema of the entity groups is stored.

The whole implementation is targeted not to provide safe and durable datastore,
but for highest possible performance. If you are looking for something more
conservative, use DatastoreSqliteStub.

Transactions aren't supported.

Author: Stanislav Heller, heller.stanislav@gmail.com
"""

import collections
import datetime
import itertools
import random
import sys
import string
import time
import re
import warnings

from google.appengine.api import apiproxy_stub, datastore_types, datastore, users
from google.appengine.datastore import entity_pb, datastore_pb
from google.appengine.datastore.datastore_stub_util import _MAXIMUM_RESULTS, \
     _MAX_QUERY_OFFSET, LoadEntity
from google.appengine.ext.blobstore import BlobKey
from google.appengine.runtime import apiproxy_errors

from pymongo import ASCENDING, DESCENDING
try:
    # pymongo >= 2.4
    from pymongo import MongoClient
    PYM_2_4 = True
except ImportError:
    from pymongo import Connection
    PYM_2_4 = False
try:
    from pymongo.binary import Binary
except ImportError:
    from pymongo import Binary


STRUCTURED_PROPERTY_DELIMITER = "#!#"


def parse_isoformat(datestring):
    """Try to parse date in ISO8061 format.

    Args:
      datestring: string containing ISO-formatted date.

    Returns:
      Converted datetime object - datetime.datetime instance.
    """
    try:
        return datetime.datetime.strptime(datestring, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        return datetime.datetime.strptime(datestring, "%Y-%m-%dT%H:%M:%S")



class _Key(object):
    """
    Wrapper class for handling both datastore and mongodb keys.

    Implements conversion between these types and some additional
    functionalities.
    """
    def __init__(self, key, app_id):
       self._app_id = app_id
       self._new = False
       if isinstance(key, basestring):
           # mongo _id
           self.path_chain = key.split("-")
           for i in xrange(1, len(self.path_chain), 2):
               self.path_chain[i] = int(self.path_chain[i])
           
       elif isinstance(key, entity_pb.Reference):
           # protobuf
           path = key.path().element_list()
           # if new key (entity is not stored into datastore), generate
           # new random monotonically increasing id.
           if path[-1].id() == 0 and not path[-1].has_name():
                path[-1].set_id(self._gen_id())
                self._new = True
           self.path_chain = [x for x in itertools.chain(*[(a.type(), a.id()) for a in path])]

    def _gen_id(self):
        return int(time.time() * 10000000)
        #return random.randint(1, sys.maxint)

    def new(self):
        """Check if the key is stored into datastore.
        
        Returns:
          True if the key is new (not store into datastore).
        """
        return self._new

    def to_datastore_key(self):
        """Convert the key into datastore format.

        Returns:
          Converted key as datastore_types.Key
        """
        return datastore_types.Key.from_path(*self.path_chain, _app=self._app_id)

    def to_mongo_key(self):
        """Convert this key into mongodb format.

        Mongodb format is basically path connected with `-`, eg. Product-1-Image-4.

        Returns:
          Converted key as string.
        """
        return "-".join(map(str, self.path_chain))

    def collection(self):
        """Get collection name which the key belongs to.

        Returns:
          Name of the collection as string.
        """
        return self.path_chain[-2].lower()

    def kind(self):
        """Get kind of the key.

        Returns:
          String representing the kind.
        """
        return self.path_chain[-2]

    def __str__(self):
        return "_Key(%s)" % self.to_mongo_key()




class _Document(object):
    """
    Wrapper around google.appengine.datastore.Entity.

    Cares about translating data types from protobuf into mongo
    and from mongo into protobuf.
    """
    # transformation functions from datastore types into format in
    # which they are stored in mongodb.
    ENCODER = {
        datetime.datetime: lambda self, x: {"t":"datetime", "v":x.isoformat()},
        datastore_types.Blob: lambda self, x: {"t":"blob", "v": Binary(x)},
        datastore_types.GeoPt: lambda self, x: {"t":"geo", "v":{'x':x.lon, 'y':x.lat}},
        datastore_types.Key: lambda self, x: {"t":"key",
                        "v":_Key(x._ToPb(), self._app_id).to_mongo_key()},
        datastore_types.BlobKey: lambda self, x: {"t":"blobkey", "v":str(x)},
        datastore_types.Text: lambda self, x: {"t":"text", "v":x},
        datastore_types.EmbeddedEntity: lambda self, x: {"t":"local", "v":Binary(x)},
    }
    # transformation functions from types in mongo into datastore types.
    DECODER = {
        "datetime": lambda self, x: parse_isoformat(x),
        "geo": lambda self, x: datastore_types.GeoPt(lat=x['y'], lon=x['x']),
        "key": lambda self, x: _Key(x, self._app_id).to_datastore_key(),
        "blobkey": lambda self, x: BlobKey(x),
        "blob": lambda self, x: datastore_types.Blob(str(x)),
        "text": lambda self, x: datastore_types.Text(x),
        "local": lambda self, x: datastore_types.EmbeddedEntity(str(x)),
        "user": lambda self, x: users.User(**x)
    }

    def __init__(self, app_id):
        self._app_id = app_id

    def _encode_user(self, user):
        d = {}
        if user.email(): d['email'] = user.email()
        if user.federated_identity():
            d['federated_identity'] = user.federated_identity()
        if user.federated_provider():
            d['federated_provider'] = user.federated_provider()
        return {"t":"user", "v":d}

    def _encode_value(self, val):
        """Translate datstore value into mongodb value.

        Args:
          val: value to be encoded.

        Returns:
          Either directly the value or dict containing value and its type.
        """
        if val.__class__ in self.ENCODER:
            return self.ENCODER[val.__class__](self, val)
        elif val.__class__ is users.User:
            return self._encode_user(val)
        elif isinstance(val, list):
            return [self._encode_value(x) for x in val]
        return val

    def _decode_value(self, val):
        """Translate mongodb value into datastore value.

        Args:
          val: value to be decoded.

        Returns:
          Value in datastore format.
        """
        if isinstance(val, dict):
            return self.DECODER[val["t"]](self, val["v"])
        elif isinstance(val, list):
            return [self._decode_value(x) for x in val]
        return val

    def _parse_pb(self, entity):
        """Parse datastore entity and store result into self.

        Args:
          entity: entity (entity_pb.EntityProto) to be parsed.
        """
        self._mongo_doc = {}
        self._mongo_doc['_id'] = self.key.to_mongo_key()
        d = datastore.Entity._FromPb(entity)
        for k, v in d.iteritems():
            # in order to store structured property, we need to translate
            # dot notation into something, what mongodb accepts
            attr = k.replace(".", STRUCTURED_PROPERTY_DELIMITER)
            self._mongo_doc[attr] = self._encode_value(v)

    def _parse_mongo(self, doc):
        """Parse mongodb document and store result into self.

        Args:
          doc: mongodb document to be parsed.
        """
        key = self.key.to_datastore_key()
        entity = datastore.Entity(kind=key.kind(), parent=key.parent(), name=key.name())
        for k, v in doc.iteritems():
            # do not set mongodb id
            if k == '_id': continue
            # transform attributes of structured properties into dotted format
            attr = k.replace(STRUCTURED_PROPERTY_DELIMITER, ".")
            entity[attr] = self._decode_value(v)
        # transfrom entity into EntityProto
        self._entity = entity._ToPb()
        if not key.name():
            self._entity.key().path().element_list()[-1].set_id(key.id())

    def is_new(self):
        """Check if the document is stored into database

        Returns:
          True if this is new document (not stored into datastore).
        """
        return self.key.new()

    def to_mongo(self):
        """Get MongoDB format of this document (entity).

        Returns:
          Dict contaning the entity in format in which it is stored in mongodb.
        """
        return self._mongo_doc

    def to_pb(self):
        """Get datastore format of this document (entity).

        Returns:
          Entity in entity_pb.EntityProto format.
        """
        return self._entity

    def get_collection(self):
        """Get name of collection for this document.

        Returns:
          Name of the collection as string.
        """
        return self.key.collection()

    @classmethod
    def from_pb(cls, entity, app_id):
        """Parses entity in protocol buffer format.

        Args:
          entity: entity_pb.EntityProto to be parsed.
          app_id: string contaning the application ID.

        Returns:
          Instance of _Document class.
        """
        d = cls(app_id)
        clone = entity_pb.EntityProto()
        clone.CopyFrom(entity)
        d.key = _Key(clone.key(), app_id)
        d._parse_pb(clone)
        d._entity = clone
        return d

    @classmethod
    def from_mongo(cls, doc, app_id):
        """Parses entity in mongodb format.

        Args:
          doc: dict contaning entity to be parsed.
          app_id: string contaning the application ID.

        Returns:
          Instance of _Document class.
        """
        d = cls(app_id)
        d.key = _Key(doc['_id'], app_id)
        d._parse_mongo(doc)
        d._mongo_doc = doc
        return d

    def get_schema(self):
        """Returns schema of this document.

        This method is specifically used by MongoSchemaManager.

        Returns:
          Dictionary representing schema of this entity.
        """
        schema = {"_id": self.get_collection(), "_kind": self.key.kind()}
        d = datastore.Entity._FromPb(self._entity)
        for k, v in d.iteritems():
            k = k.replace(".", STRUCTURED_PROPERTY_DELIMITER)
            type_ = v.__class__.__name__
            if isinstance(v, list):
                type_ += ":" + v[0].__class__.__name__
            schema[k] = type_
        return schema

    def __str__(self):
        return "_Document(%s)" % str(self._mongo_doc)



class _BaseCursor(object):
    """
    Base class for all cursor wrappers.
    """
    def __init__(self, query):
        self._app_id = query.app()
        self._skipped_results = 0
        # just random string should be safe without need for thread locks
        self.__id = "".join(random.sample(string.printable,25))

    @property
    def cursor(self):
        """Returns pymongo's cursor."""
        return self.__cursor

    @property
    def id(self):
        """Returns cursor id, which is used in the stub."""
        return self.__id

    @property
    def skipped_results(self):
        return self._skipped_results
 
    def compile(self):
        """Performs last formatting of the cursor before emitting data.

        This method is noop for pseudokind cursors.

        Returns:
          Always self.
        """
        return self


class _IteratorCursor(_BaseCursor):
    """
    Iterable cursor wrapper around pymongo.Cursor.

    Returns results in format of entity_pb.EntityProto objects. The cursor
    returns partial entities when projection on repeated property is applied.
    """
    # maps mongodb operators to appropriate python functions.
    _MONGO_FILTER_MAP = {"$lt": lambda x,y: x<y,
                         "$lte": lambda x,y: x<=y,
                         "$gte": lambda x,y: x>=y,
                         "$gt": lambda x,y: x>y}

    # maps datastore query operators to mongodb ones.
    _DATASTORE_FILTER_MAP = {
        datastore_pb.Query_Filter.LESS_THAN: '$lt',
        datastore_pb.Query_Filter.LESS_THAN_OR_EQUAL: '$lte',
        datastore_pb.Query_Filter.GREATER_THAN: '$gt',
        datastore_pb.Query_Filter.GREATER_THAN_OR_EQUAL: '$gte',
    }

    def __init__(self, query, db):
        """Constructor.

        Initializes pymongo cursor inside this wrapper.

        Args:
          query: datastore query (datastore_pb.Query) for which the cursor
                 is created.
          db: pymongo.database.Database instance.
        """
        super(_IteratorCursor, self).__init__(query)
        self.__limit = 0
        self.__offset = 0
        self._projected_props = set(query.property_name_list())
        self._projected_mongo_props = set([x.replace(".", \
                STRUCTURED_PROPERTY_DELIMITER) for x in query.property_name_list()])
        # storage for results of projection queries on repeated properties
        self._projection_splitted = []

        # parse query
        proj = self._projection(query)
        self._filters = self._get_filters(query)
        self._ancestor_query(query)
        order = self._ordering(query)
        coll_name = query.kind().lower()

        # get cursor
        if proj:
            self.__cursor = db[coll_name].find(self._filters, proj)
        else:
            self.__cursor = db[coll_name].find(self._filters)
        if order:
            self.__cursor.sort(order)
        if query.has_offset():
            self.offset(query.offset())
        if query.has_limit():
            self.limit(query.limit())


    def _projection(self, query):
        """Get projection mongodb-like dictionary

        Args:
          query: datastore query (datastore_pb.Query).

        Returns:
          Projection specification for pymongo's Cursor.
        """
        proj = {}
        for prop_name in query.property_name_list():
            proj[prop_name.replace(".", STRUCTURED_PROPERTY_DELIMITER)] = 1
        return proj

    def _get_filters(self, query):
        """Get filter specification for mongo query.

        Args:
          query: datastore query (datastore_pb.Query).

        Returns:
          Dict of filters for pymongo's Cursor.
        """
        filters = {}
        for f in query.filter_list():
            prop = f.property(0).name().decode('utf-8')
            val = datastore_types.FromPropertyPb(f.property_list()[0])
            if prop == "__key__":
                prop = "_id"
            # transform filter value of nonequality filter
            if f.op() != datastore_pb.Query_Filter.EQUAL:
                val = {self._DATASTORE_FILTER_MAP[f.op()] : val}
            # if there are more filters on the same property -> AND
            if prop in filters:
                v1 = filters[prop]
                del filters[prop]
                filters["$and"] = [{prop:v1}, {prop:val}]
            else:
                filters[prop] = val
        return filters

    def _ancestor_query(self, query):
        """Handle ancestor queries. Adds new filter to _id attribute."""
        if query.has_ancestor():
            k = _Key(query.ancestor(), self._app_id).to_mongo_key()
            self._filters["_id"] = re.compile("%s" % k)

    def _ordering(self, query):
        """Get sort orders in mongodb format.

        Args:
          query: datastore query (datastore_pb.Query).

        Returns:
          List of order dicts for pymongo.Cursor.
        """
        ordering = []
        for order in query.order_list():
            key = order.property().decode('utf-8')
            direction = ASCENDING
            if order.direction() is datastore_pb.Query_Order.DESCENDING:
                direction = DESCENDING

            # translate key attribute
            if key == "__key__":
                key = "_id"
            # translate structured property attributes
            key = key.replace(".", STRUCTURED_PROPERTY_DELIMITER)

            ordering.append((key, direction))

            # add $exists filter to get the same behaviour as google datastore:
            # if the attribute does not exist in the entity, datastore omits it,
            # but mognodb returns it..
            if key in self._filters:
                self._filters[key]["$exists"] = True
            else:
                self._filters[key] = {"$exists" : True}

            # ensure that there are not returned results with unorderable
            # property types
            type_for_key = "%s.t" % key
            # FIXME: add more unorderable property types!
            self._filters[type_for_key] = {"$nin" : ["blob", "text", "local"]}
        return ordering

    def offset(self, o):
        """Apply offset to this cursor."""
        assert o >= 0
        self.__offset = o
        # HACK: ndb is probably requesting count()
        if o >= 2147483647:
            o = 0
        self.__cursor.skip(o)
        return self

    def limit(self, l):
        """Apply limit to this cursor."""
        assert l >= 0
        self.__limit = l
        self.__cursor.limit(l)
        return self

    def compile(self):
        """Compiles this cursor.

        In reality it counts skipped results for ndb's count() calls.
        """
        o = min(self.__offset, self.__cursor.count(True))
        self._skipped_results = min(o, _MAX_QUERY_OFFSET)
        return self

    def _prepare_properties(self, entity):
        """Prepares properties in case of projection query.

        Args:
          entity: entity_pb.EntityProto to be prepared.
        """
        return LoadEntity(entity, keys_only=False,  # TODO: keys only???
                          property_names=self._projected_props)

    def __iter__(self): return self

    def _get_filter_fnc(self, filter_spec):
        """Get filtering function for projected properties.

        In case of projection query, returned function filters
        results from datastore on app layer.

        Args:
          filter_spec: filter specification in pymongo's format.

        Returns:
          Function (lambda) performing the filter.
        """
        if isinstance(filter_spec, dict):
            # inequality operator
            op, spec = filter_spec.items()[0]
            try:
                f = self._MONGO_FILTER_MAP[op]
                return lambda x: f(x, spec)
            except KeyError:
                return None
        else:
            # equals
            return lambda x: x == spec

    def _filter_projected_values(self, prop, values):
        """Filters values from repeated property in case of projection query.

        Args:
          prop: name of repeated property
          values: list of the property's values.

        Returns:
          Filtered results.
        """
        # is there some filter defined on this property?
        filter_fnc = []
        if prop in self._filters:
            filter_fnc.append(self._get_filter_fnc(self._filters[prop]))
        if "$and" in self._filters and not filter_fnc:
            for f in self._filters["$and"]:
                filtered_prop, fdict = f.items()[0]
                if filtered_prop != prop: continue
                filter_fnc.append(self._get_filter_fnc(fdict))
        # get rid of None's
        filter_fnc = filter(None, filter_fnc)
        if not filter_fnc:
            return set(values)
        # do filtering
        result = []
        for v in set(values):
            if all([fnc(v) for fnc in filter_fnc]):
                result.append(v)
        return result

    def _split_projected(self, e):
        """Splits values of repeated property if it is projected.

        Inserts all partial entities into _projection_splitted list.

        Args:
          e: entity which could be potentionally splitted.

        Returns:
          True if the the splitting was done, False otherwise.
        """
        projected = set(e.keys()) & self._projected_mongo_props
        if not projected:
            return False
        repeated = filter(lambda t: isinstance(t[1], list), # XXX list only?
                          [(attr, e[attr]) for attr in projected])
        if not repeated:
            return False
        if len(repeated) > 1:
            warnings.warn("Projection queries on more than one multivalued "\
                          "properties not supported yet.", FutureWarning)
            return False
        # split result on repeated property
        prop, values = repeated[0]
        for value in self._filter_projected_values(prop, values):
            e_new = e.copy()
            e_new[prop] = [value]
            e_proto = _Document.from_mongo(e_new, self._app_id).to_pb()
            self._projection_splitted.insert(0, self._prepare_properties(e_proto))
        return True

    def next(self):
        # If query has defined projection on repeated property, we fetch
        # entity and split it into multiple partial entities which we
        # return sequentially by calling this method.
        if self._projection_splitted:
            return self._projection_splitted.pop()

        e = self.__cursor.next()
        if self._split_projected(e):
            return self._projection_splitted.pop()
        else:
            # not splitted, just return this result
            entity = _Document.from_mongo(e, self._app_id).to_pb()
            return self._prepare_properties(entity)

    def fetch_results(self, count):
        """Fetch entities from cursor and return list of them.

        Args:
          count: method returns at most *count* results.

        Returns:
          List of fetched entities. These can be full entities or partial ones.
        """
        if count == 0:
            count = 1
        entities = []
        for _ in xrange(count):
            try:
                entity = self.next()
                entities.append(entity)
            except StopIteration: break
        return entities



class _PseudoKindCursor(_BaseCursor):
    """
    Special cursor for queries to pseudo kinds.

    These are for example __kind__, __property__ etc.
    """
    def __init__(self, query, db, schema):
        """
        Constructor. See class doc for more info.

        @param query: query for which we need to create cursor.
        @type query: datastore_pb.Query
        @param db: database of the application
        @type db: pymongo.database.Database instance
        @param schema: schema manager. Needed for __kind__ queries.
        @type schema: MongoSchemaManager instance
        """
        super(_PseudoKindCursor, self).__init__(query)
        self._schema = schema
        if query.kind() == "__kind__":
            self._is_kind = True
            self._kinds = []
            for kind in self._schema.get_kinds():
                self._kinds.append(self._to_pseudo_entity(query, "__kind__", kind))
            self.__cursor = None

    def _to_pseudo_entity(self, query, *path):
        pseudo_pb = entity_pb.EntityProto()
        pseudo_pb.mutable_entity_group()
        pseudo_pk = pseudo_pb.mutable_key()
        pseudo_pk.set_app(query.app())
        if query.has_name_space():
             pseudo_pk.set_name_space(query.name_space())

        for i in xrange(0, len(path), 2):
            pseudo_pe = pseudo_pk.mutable_path().add_element()
            pseudo_pe.set_type(path[i])
        if isinstance(path[i + 1], basestring):
            pseudo_pe.set_name(path[i + 1])
        else:
            pseudo_pe.set_id(path[i + 1])

        return pseudo_pb

    def __iter__(self): return self

    def next(self):
        if self._is_kind:
            try:
                return self._kinds.pop()
            except IndexError:
                raise StopIteration()
        else:
            raise RuntimeError("Wrong type of _PseudoKindCursor query.")

    def fetch_results(self, count):
        return [x for x in self]


class MongoSchemaManager(object):
    """
    Schema manager for datastore MongoDB stub.

    Schema is in format:
    {_id: coll_name, attr: type, ...}
    """

    #: name of collection where to search for schema
    SCHEMA_COLLECTION = '_schema'

    def __init__(self, db):
        self._db = db
        self._schema_coll = self._db[self.SCHEMA_COLLECTION]
        self._local_schema = {}

    def update_if_changed(self, schema):
        """
        Updates schema for one entity group.

        @param schema: dictionary containing schema for one entity group.
        @type schema: dict
        """
        coll_name = schema['_id']
        if coll_name not in self._local_schema:
            self._schema_coll.save(schema)
            self._local_schema[coll_name] = schema
        else:
            # do not touch mongo if not needed
            if self._local_schema[coll_name] != schema:
                self._schema_coll.save(schema)
                self._local_schema[coll_name] = schema

    def load(self):
        """
        Loads the schema from mongo db into datastore stub.
        """
        self._local_schema = {}
        for group in self._schema_coll.find():
            coll = group['_id']
            self._local_schema[coll] = group
    reload = load


    def get_type(self, kind, prop):
        """
        Get type for kind and its property.
        """
        try:
            group = self._local_schema[kind]
        except KeyError:
            raise KeyError("No such kind '%s' in schema" % kind)
        try:
            return group[prop]
        except KeyError:
            raise KeyError("No such property %s.%s" % (kind, prop))


    def get_kinds(self):
        """
        Get all kinds which are stored in datastore.
        """
        return [x['_kind'] for x in self._local_schema.values()]



class MongoIndexManager(object):
    pass




class MongoDatastore(object):
    """
    Base MongoDB Datastore responsible for storing and retrieving data from
    MongoDB database.
    """

    def __init__(self, host, port, app_id, require_indexes):
        self._app_id = app_id
        self._require_indexes = require_indexes
        # get connection
        if PYM_2_4:
            self._conn = MongoClient(host=host, port=port)
            # maximum performance (no write concern, no fsync, no journaling)
            self._conn.write_concern['w'] = 0
        else:
            # plain old connection
            self._conn = Connection(host=host, port=port)

        # database for this application
        self._db = self._conn[app_id]

        # schema manager
        self._schema = MongoSchemaManager(self._db)
        self._schema.load()

        # cursors
        self._cursors = {}

    schema = property(lambda self: self._schema)


    @property
    def write_concern(self):
        if not PYM_2_4:
            raise RuntimeError("write_concern is for pymongo >= 2.4 only.")
        return self._conn.write_concern


    def put(self, entities):
        """
        Put all entities into datastore.

        @param entities: list of entities to be stored.
        @type entities: list<entity_pb.EntityProto>
        @returns: list of keys of stored entities in the right order.
        @rtype: list<datastore_types.Key>
        """
        batch_insert = {}
        keys = []
        for e in entities:
            doc = _Document.from_pb(e, self._app_id)
            # update schema
            self.schema.update_if_changed(doc.get_schema())
            # is insert?
            if doc.is_new():
                coll_name = doc.get_collection()
                if coll_name not in batch_insert:
                    batch_insert[coll_name] = []
                batch_insert[coll_name].append(doc.to_mongo())
                self._db[coll_name].insert(doc.to_mongo())
            else:
                # update
                coll = self._db[doc.get_collection()]
                coll.save(doc.to_mongo())
            keys.append(doc.key.to_datastore_key())
        # batch insert
        for coll_name in batch_insert:
            coll = self._db[coll_name]
            coll.insert(batch_insert[coll_name])
        return keys


    def get(self, keys):
        """
        Get entities by given keys.

        @param keys: list of keys to be fetched
        @type keys: list<entity_pb.Reference>
        @returns: list of entities
        @rtype: list<entity_pb.EntityProto>
        """
        batch_get = {}
        entities = collections.OrderedDict()
        # translate datastore keys (references) to mongodb keys and sort by collection
        for key in keys:
            k = _Key(key, self._app_id)
            if k.collection() not in batch_get:
                batch_get[k.collection()] = []
            mongo_key = k.to_mongo_key()
            batch_get[k.collection()].append(mongo_key)
            entities[mongo_key] = None

        # for each collection do batch get
        for coll_name in batch_get:
            docs = self._db[coll_name].find({'_id': {'$in': batch_get[coll_name]}})
            # insert in right order
            for d in docs:
                entities[d['_id']] = _Document.from_mongo(d, self._app_id).to_pb()
        return entities.values()


    def delete(self, keys):
        """
        Delete entities by given keys.

        @param keys: list of keys to be deleted
        @type keys: list<entity_pb.Reference>
        """
        ids = {}
        for key in keys:
            k = _Key(key, self._app_id)
            if k.collection() not in ids: ids[k.collection()] = []
            ids[k.collection()].append(k.to_mongo_key())
        for coll_name in ids:
            coll = self._db[coll_name]
            coll.remove({'_id': {'$in': ids[coll_name]}})


    def clear(self):
        """
        Clear the whole datastore.
        """
        self._conn.drop_database(self._app_id)


    def get_cursor(self, cursor_id):
        try:
            return self._cursors[cursor_id]
        except KeyError:
            raise apiproxy_errors.ApplicationError(datastore_pb.Error.BAD_REQUEST,
                                                  'Cursor %d not found' % cursor_id)


    def query(self, query):
        """
        Returns cursor ID for given query.
        @type query: datastore_pb.Query
        """
        coll_name = query.kind().lower()
        if coll_name == '__kind__':
            cursor = _PseudoKindCursor(query, self._db, self.schema)
        else:
            cursor = _IteratorCursor(query, self._db)

        # store cursor for further get_query_results() calls
        self._cursors[cursor.id] = cursor.compile()
        return cursor


    def _check_indexes(self, query):
       if self._require_indexes:
            required, kind, ancestor, props, num_eq_filters = datastore_index.CompositeIndexForQuery(query)
            if required:
                index = entity_pb.CompositeIndex()
                index.mutable_definition().set_entity_type(kind)
                index.mutable_definition().set_ancestor(ancestor)
                for (k, v) in props:
                    p = index.mutable_definition().add_property()
                    p.set_name(k)
                    p.set_direction(v)

                if props and not self.__has_index(index):
                    raise apiproxy_errors.ApplicationError(datastore_pb.Error.NEED_INDEX,
                        "This query requires a composite index that is not defined. "
                        "You must update the index.yaml file in your application root.")





class DatastoreMongoDBStub(apiproxy_stub.APIProxyStub):
    """
    Datastore stub based on MongoDB.

    """
    def __init__(self,
                 app_id,
                 require_indexes=False,
                 service_name='datastore_v3',
                 consistency_policy=None,
                 mongodb_host='localhost',
                 mongodb_port=27017):
        """
        @param app_id: jednoznacny identifikator aplikace
        @param require_indexes: true, pokud maji byt vyzadovany indexy
        @param service_name: jmeno sluzby, musi byt 'datastore_v3'
        @param consistency_policy: nastaveni konzistence uloziste (kvuli HRD)

        @type app_id: str
        @type require_indexes: bool
        @type service_name: str
        @type consistency_policy: google.appengine.datastore.datastore_stub_util.*ConsistencyPolicy
        @type mongodb_network_timeout
        """
        super(DatastoreMongoDBStub, self).__init__(service_name)
        assert isinstance(app_id, str), app_id != ''
        self._datastore = MongoDatastore(mongodb_host, mongodb_port, app_id, require_indexes)


    def _Dynamic_Get(self, request, response):
        """
        Ziska vsechny entity, jejichz id je v request.key_list().

        @type request: google.appengine.datastore.datastore_pb.GetRequest
        @type response: google.appengine.datastore.datastore_pb.GetResponse
        """
        entities = self._datastore.get(request.key_list())
        for e in entities:
            wrapper = response.add_entity()
            if e is not None:
                wrapper.mutable_entity().CopyFrom(e)


    def _Dynamic_Put(self, request, response):
        """
        Zapise (pripadne prepise) do datastore vsechny entity, ktere jsou
        v request.entity_list().

        @type request: google.appengine.datastore.datastore_pb.PutRequest
        @type response: google.appengine.datastore.datastore_pb.PutResponse
        """
        keys = self._datastore.put(request.entity_list())
        response.key_list().extend([key._ToPb() for key in keys])


    def _Dynamic_Delete(self, delete_request, delete_response):
        """
        Vymaze vsechny entity, ktere jsou v request.key_list().

        @type request: google.appengine.datastore.datastore_pb.DeleteRequest
        @type response: nepouziva se.
        """
        self._datastore.delete(delete_request.key_list())


    def _Dynamic_RunQuery(self, query, query_result):
        """
        Ziska kurzor pro dany dotaz.
        @type query: google.appengine.datastore.datastore_pb.Query
        @type query_result: google.appengine.datastore.datastore_pb.QueryResult
        @raises: apiproxy_errors.ApplicationError
        """
        if query.keys_only():
            query_result.set_keys_only(True)
        cursor = self._datastore.query(query)
        query_result.mutable_cursor().set_cursor(cursor.id)
        query_result.set_more_results(True)

        query_result.set_skipped_results(cursor.skipped_results)

        if query.compile():
            compiled_query = query_result.mutable_compiled_query()
            compiled_query.set_keys_only(query.keys_only())
            compiled_query.mutable_primaryscan().set_index_name(query.Encode())


    def _Dynamic_Next(self, next_request, query_result):
        """
        Ziska balik dalsich vysledku z kurzoru.
        @type next_request: google.appengine.datastore.datastore_pb.NextRequest
        """
        cursor_id = next_request.cursor().cursor()
        count = next_request.count()
        cursor = self._datastore.get_cursor(cursor_id)
        entities = cursor.fetch_results(count)
        query_result.result_list().extend(entities)
        query_result.set_more_results(False)


    def Clear(self):
        """
        Vymaze cely datastore.
        """
        self._datastore.clear()


    def MakeSyncCall(self, service, call, request, response):
        """
        Zakladni vstupni metoda RPC volani.

        @param service: nazev sluzby datastore. Mel by byt 'datastore_v3'.
        @param call: string reprezentujici nazev RPC metody, ktera se ma provest.
                     Metoda musi byt implementovana s nazvem _Dynamic_<call>.
        @param request: zprava protocol bufferu korespondujici s volanim.
        @param response: zprava protocol bufferu korespondujici s navratovou
                         hodnotou volani.
        @type service: str
        @type call: str
        @type: request: podtrida google.net.proto.ProtocolBuffer.ProtocolMessage
        @type response: podtrida google.net.proto.ProtocolBuffer.ProtocolMessage
        """
        super(DatastoreMongoDBStub, self).MakeSyncCall(service,
                                                       call,
                                                       request,
                                                       response)
        explanation = []
        assert response.IsInitialized(explanation), explanation

    # TODO: def SetConsistencyPolicy

