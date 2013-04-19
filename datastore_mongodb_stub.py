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
import weakref

from google.appengine.api import apiproxy_stub, datastore_types, datastore, users
from google.appengine.datastore import entity_pb, datastore_pb, datastore_stub_util
from google.appengine.datastore.datastore_stub_util import _MAXIMUM_RESULTS, \
     _MAX_QUERY_OFFSET, LoadEntity, ParseNamespaceQuery
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
    from bson import Binary


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
        self.path_chain = []
        if isinstance(key, dict):
            # mongo _id
            self._mongo_key = key['dskey']
            for elem in self._mongo_key:
                type_, id_ = elem.split("-")
                try: id_ = int(id_)
                except: pass
                self.path_chain.extend([type_, id_])

        elif isinstance(key, entity_pb.Reference):
            # protobuf
            path = key.path().element_list()
            self._mongo_key = []
            for elem in path:
                if elem.has_id():
                    self._mongo_key.append(elem.type() + "-" + str(elem.id()))
                    self.path_chain.extend([elem.type(), elem.id()])
                elif elem.has_name():
                    self._mongo_key.append(elem.type() + "-" + elem.name())
                    self.path_chain.extend([elem.type(), elem.name()])
                else:
                    raise RuntimeException("Path element doesnt have id neither name.")

    def to_datastore_key(self):
        """Convert the key into datastore format.

        Returns:
          Converted key as datastore_types.Key
        """
        return datastore_types.Key.from_path(*self.path_chain, _app=self._app_id)

    def to_mongo_key(self):
        """Convert this key into mongodb format.

        Mongodb format is dict, which contains key 'dskey' mapping to list
        containing parent path. The last element is key of the entity.

        Example: {'dskey': ['Product-1', 'Image-4', ...]}

        Returns:
          Converted key as dict.
        """
        return {'dskey': self._mongo_key}

    def collection(self):
        """Get collection name which the key belongs to.

        Returns:
          Name of the collection as string.
        """
        return self.path_chain[0].lower()

    def kind(self):
        """Get kind of the key.

        Returns:
          String representing the kind.
        """
        return self.path_chain[-2]

    def __str__(self):
        return "_Key(%s)" % self._mongo_key



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
        def to_utf8(s):
            if isinstance(s, unicode):
                return s.encode('utf-8')
            try:
                s.decode('utf-8')
                return s
            except UnicodeDecodeError:
                return "$UTF8$" + "~".join([str(ord(l)) for l in s])

        if val.__class__ in self.ENCODER:
            return self.ENCODER[val.__class__](self, val)
        elif val.__class__ is users.User:
            return self._encode_user(val)
        elif isinstance(val, list):
            return [self._encode_value(x) for x in val]
        elif isinstance(val, basestring):
            return to_utf8(val)
        else:
            return val

    def _decode_value(self, val):
        """Translate mongodb value into datastore value.

        Args:
          val: value to be decoded.

        Returns:
          Value in datastore format.
        """
        def from_utf8(s):
            return "".join([chr(int(l)) for l in pp.split("~")])

        if isinstance(val, dict):
            return self.DECODER[val["t"]](self, val["v"])
        elif isinstance(val, list):
            return [self._decode_value(x) for x in val]
        elif isinstance(val, str) and val.startswith("$UTF8$"):
            return from_utf8(val)
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

    def iter_mongo_indexes(self):
        for attr, val in self._mongo_doc.iteritems():
            if attr in ('_id', '__scatter__'):
                continue
            if isinstance(val, dict):
                t = "%s.t" % attr
                v = "%s.v" % attr
                yield [(v, ASCENDING), (t, ASCENDING)]
            else:
                yield attr

    def __str__(self):
        return "_Document(%s)" % str(self._mongo_doc)



class _BaseCursor(object):
    """
    Base class for all cursor wrappers.
    """
    def __init__(self, query):
        self._app_id = query.app()
        self._skipped_results = 0



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
        self.__query = query
        self._skipped_results = 0
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

        self._dummy = self._dummy_proto()

    def _dummy_proto(self):
        pb = entity_pb.EntityProto()
        pb.mutable_entity_group()
        pk = pb.mutable_key()
        pk.set_app(self.__query.app())
        pe = pk.mutable_path().add_element()
        pe.set_type(self.__query.kind())
        pe.set_id(0)
        return pb

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
        # dummy document, XXX: needs refactoring
        _d = _Document('a')
        filters = {}
        for f in query.filter_list():
            # resolve property name and value
            prop = f.property(0).name().decode('utf-8')
            if prop == "__key__":
                prop = "_id.dskey"
            prop = prop.replace(".", STRUCTURED_PROPERTY_DELIMITER)

            val = datastore_types.FromPropertyPb(f.property_list()[0])
            val = _d._encode_value(val)

            # transform filter value of nonequality filter
            if f.op() != datastore_pb.Query_Filter.EQUAL:
                val = {self._DATASTORE_FILTER_MAP[f.op()] : val}
            # if there are more filters on the same property -> AND
            if prop in filters:
                v1 = filters[prop]
                del filters[prop]
                filters["$and"] = [{prop:v1}, {prop:val}]
            elif '$and' in filters:
                filters['$and'].append({prop: val})
            else:
                filters[prop] = val
        return filters

    def _ancestor_query(self, query):
        """Handle ancestor queries. Adds new filter to _id attribute."""
        if query.has_ancestor():
            k = _Key(query.ancestor(), self._app_id)._mongo_key
            self._filters["_id.dskey"] = {'$all' : k}

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
                key = "_id.dskey"
            # translate structured property attributes
            key = key.replace(".", STRUCTURED_PROPERTY_DELIMITER)

            ordering.append((key, direction))

            # add $exists filter to get the same behaviour as google datastore:
            # if the attribute does not exist in the entity, datastore omits it,
            # but mognodb returns it..
            if key in self._filters:
                try:
                    self._filters[key]["$exists"] = True
                except TypeError:
                    pass
            else:
                self._filters[key] = {"$exists" : True}

            # ensure that there are not returned results with unorderable
            # property types
            type_for_key = "%s.t" % key
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
        if isinstance(l, long):
            if l < sys.maxint:
                l = int(l)
            else:
                l = sys.maxint
        self.__limit = l
        self.__cursor.limit(l)
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

    def _next_offset(self):
        if self.__offset > 10000: # XXX ?
            return False
        if self._skipped_results < self.__offset:
            self._skipped_results += 1
            return True
        return False

    def next(self):
        # Return dummy result in case of offset
        if self._next_offset():
            return self._dummy

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



class _PseudoKindCursor(_BaseCursor):
    """
    Special cursor for queries to pseudo kinds.

    These are for example __kind__, __property__, __namespace__, etc.
    """
    def __init__(self, query, db, schema):
        """Constructor. 

        Initializes cursor, which is not a real pymongo Cursor.

        Args:
          query: query (datastore_pb.Query) for which we create the cursor.
          db: database of the application (pymongo.database.Database instance)
          schema: schema manager. Needed for __kind__ queries.
        """
        super(_PseudoKindCursor, self).__init__(query)
        self._schema = schema
        self._payload = []
        self._pseudokind = query.kind()
        if self._pseudokind == "__kind__":
            for kind in self._schema.get_kinds():
                self._payload.append(self._to_pseudo_entity(query, "__kind__", kind))
        elif self._pseudokind == "__namespace__":
            self._payload.append(self._to_pseudo_entity(query, "__namespace__", 1))
        else:
            raise RuntimeError("Wrong type of _PseudoKindCursor query.")

    def _to_pseudo_entity(self, query, *path):
        """Convert path to pseudo entity"""
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
        try:
            return self._payload.pop()
        except IndexError:
            raise StopIteration()



def _StatCursor(query, db):
    """Just a dummy cursor returning all entities in database"""
    app_id = query.app()
    cols = set(db.collection_names())
    cols -= set([u'_indexes', u'system.indexes', u'_schema'])
    for c in cols:
        for e in db[c].find():
            yield _Document.from_mongo(e, app_id).to_pb()



class MongoSchemaManager(object):
    """
    Schema manager for datastore MongoDB stub.

    Schema is in format:
    {_id: coll_name, _kind: kind, attr: type, ...}
    """

    #: name of collection where to search for schema
    SCHEMA_COLLECTION = '_schema'

    def __init__(self, db):
        """Constructor.

        Initializes schema manager.

        Args:
          db: database of the application (pymongo.database.Database instance). 
        """
        self._db = db
        self._schema_coll = self._db[self.SCHEMA_COLLECTION]
        self._local_schema = {}

    def update_if_changed(self, schema):
        """Updates schema for one entity group.

        Args:
          schema: dictionary containing schema for one entity group.
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
        """Loads the schema from mongo db into datastore stub."""
        self._local_schema = {}
        for group in self._schema_coll.find():
            coll = group['_id']
            self._local_schema[coll] = group
    reload = load

    def get_type(self, kind, prop):
        """Get type for kind and its property.

        Args:
          kind: string, kind where we search for the property.
          prop: string, property of which we want to get the type.

        Returns:
          Appropriate property type as string.

        Raises:
          KeyError if kind or property not found.
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
        """Get all kinds which are stored in datastore.

        Returns:
          List of strings representing names of kinds.
        """
        return [x['_kind'] for x in self._local_schema.values()]



class MongoDatastore(object):
    """
    Base MongoDB Datastore.

    This class is responsible for storing and retrieving data from MongoDB
    database except for querying the database - this task is handled
    by cursors.
    """

    def __init__(self, host, port, app_id, require_indexes=False):
        """Constructor.

        Creates mongodb connection (in case of pymongo 2.4 MongoClient)
        and initializes a few helpers.

        Args:
          host: string, mongodb host.
          port: int, port on which the mongod server runs.
          app_id: string representing the application ID.
          require_indexes: bool, default False. If True, composite indexes must
              exist in index.yaml for queries that need them.
        """
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
        """Dictionary representing MongoClient's write concern."""
        if not PYM_2_4:
            raise RuntimeError("write_concern is for pymongo >= 2.4 only.")
        return self._conn.write_concern

    def _ensure_noncomposite_indexes(self, doc):
        """Simulate EntitiesByPropertyASC and EntitiesByPropertyDESC indexes"""
        coll = self._db[doc.get_collection()]
        coll.ensure_index('_id.dskey', cache_for=7200)
        for spec in doc.iter_mongo_indexes():
            coll.ensure_index(spec, cache_for=3600)

    def put(self, entities):
        """Puts all entities into datastore.

        Args:
          entities: list of entities (entity_pb.EntityProto) to be stored.

        Returns:
          list of datastore_types.Key instances of stored entities in
          the right order.
        """
        batch_insert = {}
        keys = []
        for e in entities:
            doc = _Document.from_pb(e, self._app_id)
            # update schema
            self.schema.update_if_changed(doc.get_schema())
            # insert / overwrite
            coll = self._db[doc.get_collection()]
            coll.save(doc.to_mongo())
            # be sure to have all indexes (EntitiesByPropertyASC & DESC)
            self._ensure_noncomposite_indexes(doc)
            keys.append(doc.key.to_datastore_key())
        return keys

    def get(self, key):
        """Get entity by given key.

        Args:
          keys: key (entity_pb.Reference) to be fetched.

        Returns:
           fetched entity (entity_pb.EntityProto).
        """
        # translate datastore key (references) to mongodb key
        k = _Key(key, self._app_id)
        doc = self._db[k.collection()].find({'_id.dskey': {'$all': k._mongo_key}})
        if not doc:
            return None
        return _Document.from_mongo(d, self._app_id).to_pb()

    def delete(self, key):
        """Delete entity by given key.

        Args:
          key: key (entity_pb.Reference) to be deleted.
        """
        k = _Key(key, self._app_id)
        coll = self._db[k.collection()]
        coll.remove({'_id.dskey': {'$all': k._mongo_key}})

    def clear(self):
        """Clear the whole mongo datastore."""
        self._conn.drop_database(self._app_id)

    def query(self, query):
        """Perform a query on specified kind or pseudokind.
    
        Args:
          query: datastore_pb.Query to be performed.

        Returns:
          string, cursor ID for given query.
        """
        coll_name = query.kind().lower()
        if coll_name in ('__kind__', '__namespace__'):
            cursor = _PseudoKindCursor(query, self._db, self.schema)
        elif coll_name == '':
            cursor = _StatCursor(query, self._db)
        else:
            cursor = _IteratorCursor(query, self._db)

        return cursor

    def update_indexes(self, indices):
        d = {'_id' : 1, 'indexes': Binary(indices.Encode())}
        self._db['_indexes'].save(d)

    def load_indexes(self):
        i = self._db['_indexes'].find_one(1)
        if not i: return None
        return i['indexes']



class DatastoreMongoDBStub(datastore_stub_util.BaseDatastore,
                           apiproxy_stub.APIProxyStub,
                           datastore_stub_util.DatastoreStub):
    """
    Persistent stub for the Python datastore API.

    Maps datastore service calls on to a MongoDatastore, which
    stores all entities in an MongoDB database.
    """
    def __init__(self,
                 app_id,
                 require_indexes=False,
                 service_name='datastore_v3',
                 consistency_policy=None,
                 root_path=None,
                 mongodb_host='localhost',
                 mongodb_port=27017):
        """Constructor.

        Initializes stub and connection to mongodb.

        Args:
          app_id: string, application ID.
          require_indexes: bool, default False. If True, composite indexes must
              exist in index.yaml for queries that need them.
          service_name: name of the service, default 'datastore_v3'.
          consistency_policy: The consistency policy to use or None to use the
              default. Consistency policies can be found in
              datastore_stub_util.*ConsistencyPolicy
          mongodb_host: string, mongodb host address.
          mongodb_port: int, port on which the mongod server runs.
        """
        assert isinstance(app_id, str), app_id != ''

        datastore_stub_util.BaseDatastore.__init__(self, require_indexes,
                                                   consistency_policy)
        apiproxy_stub.APIProxyStub.__init__(self, service_name)
        datastore_stub_util.DatastoreStub.__init__(self, weakref.proxy(self),
                                                   app_id, trusted=False,
                                                   root_path=root_path)
        # speed-up dict for _EntitiesByEntityGroup method taken
        # from DatastoreFileStub
        self.__entities_by_group = collections.defaultdict(dict)
        # initialize inner mongo datastore
        self._mongods = MongoDatastore(mongodb_host, mongodb_port, app_id,
                                       require_indexes)
        # load indexes into stub
        index_proto = self._mongods.load_indexes()
        if index_proto:
            indexes = datastore_pb.CompositeIndices(index_proto)
            for index in indexes.index_list():
                # XXX: because self._SideLoadIndex(index) thwors AttributeError,
                # because it uses app() method insead of app_id(), inserting
                # index is hard-wired and imitates the _SideLoadIndex method
                self._BaseIndexManager__indexes[index.app_id()].append(index)


    def MakeSyncCall(self, service, call, request, response, request_id=None):
        """
        Base input RPC method.

        Args:
          service: name of the service. Should be 'datastore_v3'.
          call: string, name of the RPC method. Method must be implemented as
              _Dynamic_<call> in the stub.
          request: request message of protobuf. Subclass of
              google.net.proto.ProtocolBuffer.ProtocolMessage.
          response: response message of protobuf. Subclass of
              google.net.proto.ProtocolBuffer.ProtocolMessage
        """
        super(DatastoreMongoDBStub, self).MakeSyncCall(service,
                                                       call,
                                                       request,
                                                       response,
                                                       request_id)
        explanation = []
        assert response.IsInitialized(explanation), explanation

    def Clear(self):
        """Clears out all stored values."""
        datastore_stub_util.DatastoreStub.Clear(self)
        self._mongods.clear()
        self.__entities_by_group = collections.defaultdict(dict)

    def Read(self):
        """Noop"""

    def Close(self):
        """Noop"""

    def _GetEntityLocation(self, key):
        """Get keys to self.__entities_by_group from the given key.

        Copied from datastore_file_stub.

        Args:
          key: entity_pb.Reference

        Returns:
          Tuple (by_entity_group key, entity key)
        """
        entity_group = datastore_stub_util._GetEntityGroup(key)
        eg_k = datastore_types.ReferenceToKeyValue(entity_group)
        k = datastore_types.ReferenceToKeyValue(key)
        return (eg_k, k)

    def _Put(self, entity, insert):
        """Put the given entity.

        Args:
          entity: The entity_pb.EntityProto to put.
          insert: A boolean that indicates if we should fail if the entity already
            exists.
        """
        entity = datastore_stub_util.StoreEntity(entity)
        # store entity into entity group dict
        eg_k, k = self._GetEntityLocation(entity.key())
        self.__entities_by_group[eg_k][k] = entity
        # put into mongo 
        self._mongods.put([entity])

    def _Get(self, key):
        """Get the entity for the given reference or None.

        Args:
          reference: A entity_pb.Reference to loop up.

        Returns:
          The entity_pb.EntityProto associated with the given reference or None.
        """
        entity = self._mongods.get(key)
        return datastore_stub_util.LoadEntity(entity)

    def _AllocateIds(self, reference, size=1, max_id=None):
        """Allocate ids for given reference.

        Args:
          reference: A entity_pb.Reference to allocate an id for.
          size: The size of the range to allocate
          max_id: The upper bound of the range to allocate

        Returns:
          A tuple containing (min, max) of the allocated range.
        """
        datastore_stub_util.CheckAppId(reference.app(),
                                       self._trusted, self._app_id)
        datastore_stub_util.Check(not (size and max_id),
                                  'Both size and max cannot be set.')

        t = long(time.time() * 10000000)
        return (t, t+size)

    def _Delete(self, key):
        """Delete the entity associated with the specified reference.

        Args:
          reference: The entity_pb.Reference of the entity to delete.
        """
        eg_k, k = self._GetEntityLocation(key)
        try:
            del self.__entities_by_group[eg_k][k]
            if not self.__entities_by_group[eg_k]:
                del self.__entities_by_group[eg_k]
        except KeyError:
            pass
        self._mongods.delete(key)

    def _GetEntitiesInEntityGroup(self, entity_group):
        """Gets the contents of a specific entity group.

        Other entity groups may be modified concurrently.

        Args:
          entity_group: A entity_pb.Reference of the entity group to get.

        Returns:
          A dict mapping datastore_types.ReferenceToKeyValue(key) to EntityProto
        """
        try:
            eg_k = datastore_types.ReferenceToKeyValue(entity_group)
            return self.__entities_by_group[eg_k].copy()
        except KeyError:
            pass
        query = datastore_pb.Query()
        query.set_kind(entity_group.path().element_list()[0].type())
        query.set_app(entity_group.app())
        if entity_group.name_space():
            query.set_name_space(entity_group.name_space())
        query.mutable_ancestor().CopyFrom(entity_group)

        cursor = self._mongods.query(query)
        return dict((datastore_types.ReferenceToKeyValue(entity.key()), entity)
                     for entity in cursor)

    def _GetQueryCursor(self, query, filters, orders, index_list):
        """Runs the given datastore_pb.Query and returns a QueryCursor for it.

        Args:
          query: The datastore_pb.Query to run.
          filters: A list of filters that override the ones found on query.
          orders: A list of orders that override the ones found on query.
          index_list: A list of indexes used by the query.

        Returns:
          An IteratorCursor that can be used to fetch query results.
        """
        db_cursor = self._mongods.query(query)
        orders = datastore_stub_util._GuessOrders(filters, orders)
        dsquery = datastore_stub_util._MakeQuery(query, filters, orders)
        cursor = datastore_stub_util.IteratorCursor(query, dsquery, orders,
                                                    index_list, db_cursor)
        return cursor

    def _OnIndexChange(self, app_id):
        indices = datastore_pb.CompositeIndices()
        for index in self.GetIndexes(app_id, True, self._app_id):
            indices.index_list().append(index)
        self._mongods.update_indexes(indices)


