
import os
import unittest
import string
import datetime
import sys
import textwrap

from google.appengine.api import apiproxy_stub_map, datastore_types, users
from google.appengine.api.memcache import memcache_stub
from google.appengine.api.user_service_stub import UserServiceStub
from google.appengine.api.datastore_file_stub import DatastoreFileStub
from google.appengine.datastore.datastore_stub_util import _MAXIMUM_RESULTS, _MAX_QUERY_OFFSET

from google.appengine.ext import ndb
from google.appengine.ext.blobstore import BlobKey


# import DATASTORE MONGODB STUB from this pkg
from datastore_mongodb_stub import DatastoreMongoDBStub

# TODO: filtering generic property
# TODO: thread tests
# TODO: indexes
# TODO: Expando models (generic properties)
# TODO: Projection queries on multivalued properties

APP_ID = 'test'

class _DatastoreStubTests(object):
    """
    Base class defining common test for datastore stubs.
    """
    def __init__(self, *args, **kwargs):
        """
        Set up SDK testing environment (does not init datastore stubs, they
        are initialized in derived classes).
        """
        # view non-shortened diffs
        self.maxDiff = None

        os.environ['APPLICATION_ID'] = APP_ID
        apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
        # memcache stub
        cache_stub = memcache_stub.MemcacheServiceStub()
        apiproxy_stub_map.apiproxy.RegisterStub('memcache', cache_stub)
        # user stub
        user_stub = UserServiceStub()
        apiproxy_stub_map.apiproxy.RegisterStub('user', user_stub)



    # PUT, GET

    def test_put_parent(self):
        class Product(ndb.Model):
            a = ndb.StringProperty()

        class Commercial(ndb.Model):
            b = ndb.StringProperty()

        c = Commercial(b="b")
        k = c.put()
        p = Product(parent=k, a="a")
        kk = p.put()
        self.assertEqual(kk.get(use_cache=False, use_memcache=False), p)
        ndb.delete_multi([k, kk])
        


    def test_invalid_get(self):
        k = ndb.Key('Car', 2)
        assert k.get() is None


    def test_invalid_get_multi(self):
        keys = [ndb.Key('Car', 2), ndb.Key('House', 1), ndb.Key('John', 584635)]
        res = ndb.get_multi(keys)
        assert res == [None, None, None]


    def test_put_multi(self):
        class Product(ndb.Model):
            a = ndb.StringProperty()

        p = [Product(a=l) for l in string.letters]
        keys = ndb.put_multi(p)
        pp = ndb.get_multi(keys)
        self.assertEqual(p, pp)
        ndb.delete_multi(keys)


    def test_update(self):
        class Product(ndb.Model):
            a = ndb.StringProperty()

        p = Product(a="foo")
        k = p.put()
        p.a = "bar"
        p.put()
        pp = k.get(use_cache=False, use_memcache=False)
        assert pp == p
        assert pp.a == "bar"


    def test_string_property_unicode(self):
        s = unichr(233) + unichr(0x0bf2) + unichr(3972) + unichr(6000) + unichr(13231)
        class Product(ndb.Model):
            title=ndb.StringProperty()

        p = Product(title=s)
        key = p.put()
        pp = key.get(use_cache=False, use_memcache=False)
        assert p == pp


    def test_text_property_unicode(self):
        s = unichr(233) + unichr(0x0bf2) + unichr(3972) + unichr(6000) + unichr(13231)
        class Product(ndb.Model):
            title=ndb.TextProperty()

        p = Product(title=s)
        key = p.put()
        pp = key.get(use_cache=False, use_memcache=False)
        assert p == pp

    
    def test_integer_property(self):
        class Product(ndb.Model):
            price=ndb.IntegerProperty()

        p = Product(price=10000)
        key = p.put()
        pp = key.get(use_cache=False, use_memcache=False)
        assert p == pp, type(pp.price) is int


    def test_float_property(self):
        class Product(ndb.Model):
            price=ndb.FloatProperty()

        p = Product(price=33.3333)
        key = p.put()
        pp = key.get(use_cache=False, use_memcache=False)
        self.assertEqual(p, pp)
        self.assertEqual(type(pp.price), float)


    def test_boolean_property(self):
        class A(ndb.Model):
            b = ndb.BooleanProperty()
        a = A(b=False)
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a.b, aa.b)
        self.assertEqual(a, aa)



    def test_blob_property(self):
        s = "MaA48Er\xa40TYr8WAz\c00CerMPw*#&)(&@|:DKoay5ievJ5f/LL=\x0513"
        class A(ndb.Model):
            b = ndb.BlobProperty()
        a = A(b=s)
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a.b, aa.b)
        self.assertEqual(a, aa)


    def test_datetime_property(self):
        class A(ndb.Model):
            b = ndb.DateTimeProperty()
        a = A(b=datetime.datetime.now())
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a.b, aa.b)
        self.assertEqual(a, aa)


    def test_date_property(self):
        class A(ndb.Model):
            b = ndb.DateProperty()
        a = A(b=datetime.date.today())
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a.b, aa.b)
        self.assertEqual(a, aa)


    def test_time_property(self):
        class A(ndb.Model):
            b = ndb.TimeProperty()
        a = A(b=datetime.datetime.now().time())
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a.b, aa.b)
        self.assertEqual(a, aa)


    def test_geopt_property(self):
        class A(ndb.Model):
            b = ndb.GeoPtProperty()
        a = A(b=ndb.GeoPt(52.37, 4.88))
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a.b, aa.b)
        self.assertEqual(a, aa)


    def test_structured_property(self):
        class Inner(ndb.Model):
            i = ndb.IntegerProperty()
            j = ndb.StringProperty()

        class Outer(ndb.Model):
            s = ndb.StructuredProperty(Inner)

        o = Outer(s=Inner(i=1, j="a"))
        k = o.put()
        oo = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(o.s, oo.s)
        self.assertEqual(o, oo)
        self.assertTrue(o is not oo)


    def test_structured_property_repeated(self):
        class Inner(ndb.Model):
            i = ndb.IntegerProperty()
            j = ndb.StringProperty()

        class Outer(ndb.Model):
            s = ndb.StructuredProperty(Inner, repeated=True)

        o = Outer(s=[Inner(i=1, j="a"), Inner(i=2, j="b")])
        k = o.put()
        oo = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(o.s, oo.s)
        self.assertEqual(o, oo)
        self.assertTrue(o is not oo)


    def test_local_structured_property(self):
        class Inner(ndb.Model):
            i = ndb.IntegerProperty()
            j = ndb.StringProperty()

        class Outer(ndb.Model):
            s = ndb.LocalStructuredProperty(Inner)

        o = Outer(s=Inner(i=1, j="a"))
        k = o.put()
        oo = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(o.s, oo.s)
        self.assertEqual(o, oo)
        self.assertTrue(o is not oo)


    def test_local_structured_property_repeated(self):
        class Inner(ndb.Model):
            i = ndb.IntegerProperty()
            j = ndb.StringProperty()

        class Outer(ndb.Model):
            s = ndb.LocalStructuredProperty(Inner, repeated=True)

        o = Outer(s=[Inner(i=1, j="a"), Inner(i=2, j="b")])
        k = o.put()
        oo = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(o.s, oo.s)
        self.assertEqual(o, oo)
        self.assertTrue(o is not oo)


    def test_key_property(self):
        class K(ndb.Model):
            b = ndb.KeyProperty()

        a = K(b=ndb.Key('Car', 2))
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a.b, aa.b)
        self.assertEqual(a, aa)


    def test_blobkey_property(self):
        class K(ndb.Model):
            b = ndb.BlobKeyProperty()

        a = K(b=BlobKey('SomeBlobKey-28613'))
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a.b, aa.b)
        self.assertEqual(a, aa)
        k.delete()


    def test_user_property(self):
        user = users.User("albert.johnson@example.com")
        class A(ndb.Model):
            b = ndb.UserProperty()
        a = A(b=user)
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a.b, aa.b)
        self.assertEqual(a, aa)
        k.delete()


    def test_json_property(self):
        class A(ndb.Model):
            b = ndb.JsonProperty()
        a = A(b={'c':'d', '4':4})
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a.b, aa.b)
        self.assertEqual(a, aa)


    def test_pickle_property(self):
        class A(ndb.Model):
            b = ndb.PickleProperty()

        a = A(b=frozenset())
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a.b, aa.b)
        self.assertEqual(a, aa)


    def test_generic_property(self):
        class A(ndb.Model):
            b = ndb.GenericProperty()

        a = A(b=datetime.datetime.now())
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a.b, aa.b)
        self.assertEqual(a, aa)
        k.delete()


    def test_computed_property(self):
        class A(ndb.Model):
            b = ndb.StringProperty()
            c = ndb.ComputedProperty(lambda self: len(self.b))

        a = A(b="hello kitty")
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a.c, aa.c)
        self.assertEqual(a, aa)
        k.delete()


    def test_str_property_repeated(self):
        class Article(ndb.Model):
            tags = ndb.StringProperty(repeated=True)
        a = Article(tags=['python', 'ruby'])
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a, aa)
        k.delete()


    def test_key_property_repeated(self):
        class A(ndb.Model):
            keys = ndb.KeyProperty(repeated=True)
        a = A(keys=[ndb.Key('A',345), ndb.Key('B', 8441)])
        k = a.put()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(a, aa)
        k.delete()


    # DELETE

    def test_delete_entity(self):
        class Product(ndb.Model):
            a = ndb.StringProperty()
        k = Product(a="a").put()
        k.delete()
        self.assertEqual(k.get(), None)

    def test_delete_property_value(self):
        class P(ndb.Model):
            a = ndb.StringProperty(repeated=True)
            b = ndb.IntegerProperty()
        p = P(a=['x','y'], b=4)
        k = p.put()
        del p.b
        p.put()
        pp = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(pp.b, None)
        pp.a.pop()
        pp.put()
        ppp = k.get(use_cache=False, use_memcache=False)
        self.assertEqual(ppp.a, ['x'])
        k.delete()


    # SCHEMA

    def test_schema_change(self):
        class A(ndb.Model):
            b = ndb.StringProperty()
        a = A(b="foo")
        k = a.put()
        del A
        class A(ndb.Model):
            a = ndb.IntegerProperty()
        aa = k.get(use_cache=False, use_memcache=False)
        self.assertNotEqual(a, aa)

    # ~~~~~~~~
    # QUERYING
    # ~~~~~~~~

    def _gen_entities(self, count, property_type, inner_type=ndb.IntegerProperty):
        if property_type in (ndb.StructuredProperty, ndb.LocalStructuredProperty):
            class P(ndb.Model):
                m = inner_type()
            class Q(ndb.Model):
                a = property_type(P)
            if inner_type is ndb.IntegerProperty:
                return Q, [Q(a=P(m=i)) for i in xrange(count)]
            elif inner_type in (ndb.StringProperty,
                                ndb.TextProperty,
                                ndb.BlobProperty):
                return Q, [Q(a=P(m=s)) for s in string.letters]
        class Q(ndb.Model):
            a = property_type()
        if property_type is ndb.IntegerProperty:
            return Q, [Q(a=i) for i in xrange(count)]
        elif property_type in (ndb.StringProperty,
                               ndb.TextProperty,
                               ndb.BlobProperty):
            return Q, [Q(a=s) for s in string.letters]
        elif property_type is ndb.JsonProperty:
            return Q, [Q(a={'t':'foo', 'b':i}) for i in xrange(count)]
        elif property_type is ndb.PickleProperty:
            return Q, [Q(a=set([i])) for i in xrange(count)]
        elif property_type is ndb.DateProperty:
            return Q, [Q(a=datetime.date(2013, 1, i+1)) for i in xrange(count)]
        elif property_type is ndb.TimeProperty:
            return Q, [Q(a=datetime.datetime.now().time()) for i in xrange(count)]
        elif property_type is ndb.DateTimeProperty:
            return Q, [Q(a=datetime.datetime.now()) for i in xrange(count)]
        elif property_type is ndb.GenericProperty:
            l = [Q(a=i) for i in xrange(count/2)]
            l.extend( [Q(a=str(i)) for i in xrange(count/2)] )
            return Q, l

    # QUERY FILTERS

    def test_query_filter_gt(self):
        class Q(ndb.Model):
            a = ndb.IntegerProperty()
        q1 = Q(a=4)
        k1 = q1.put()
        q2 = Q(a=7)
        k2 = q2.put()
        try:
            self.assertEqual(Q.query(Q.a > 5).fetch(), [q2])
            self.assertEqual(Q.query(Q.a > 4).fetch(), [q2])
        finally:
            ndb.delete_multi([k1, k2])


    def test_query_filter_gte(self):
        class Q(ndb.Model):
            a = ndb.IntegerProperty()
        q1 = Q(a=4)
        k1 = q1.put()
        q2 = Q(a=7)
        k2 = q2.put()
        try:
            self.assertEqual(Q.query(Q.a >= 5).fetch(), [q2])
            self.assertEqual(Q.query(Q.a >= 7).fetch(), [q2])
        finally:
            ndb.delete_multi([k1, k2])


    def test_query_filter_eq(self):
        class Q(ndb.Model):
            a = ndb.IntegerProperty()
        q1 = Q(a=4)
        k1 = q1.put()
        q2 = Q(a=7)
        k2 = q2.put()
        try:
            self.assertEqual(Q.query(Q.a == 5).fetch(), [])
            self.assertEqual(Q.query(Q.a == 7).fetch(), [q2])
        finally:
            ndb.delete_multi([k1, k2])


    def test_query_filter_lte(self):
        class Q(ndb.Model):
            a = ndb.IntegerProperty()
        q1 = Q(a=4)
        k1 = q1.put()
        q2 = Q(a=7)
        k2 = q2.put()
        try:
            self.assertEqual(Q.query(Q.a <= 4).fetch(), [q1])
            self.assertEqual(Q.query(Q.a <= 7).fetch(), [q1, q2])
        finally:
            ndb.delete_multi([k1, k2])


    def test_query_filter_lt(self):
        class Q(ndb.Model):
            a = ndb.IntegerProperty()
        q1 = Q(a=4)
        k1 = q1.put()
        q2 = Q(a=7)
        k2 = q2.put()
        try:
            self.assertEqual(Q.query(Q.a < 4).fetch(), [])
            self.assertEqual(Q.query(Q.a < 11).fetch(), [q1, q2])
        finally:
            ndb.delete_multi([k1, k2])


    def test_query_filter_in(self):
        class Q(ndb.Model):
            a = ndb.StringProperty(repeated=True)
        e = [Q(a=['a', 'b', 'c']), Q(a=['z']), Q(a=['t','w'])]
        keys = ndb.put_multi(e)
        l = Q.query(Q.a.IN(['b', 'w'])).fetch()
        ref = [e[0], e[2]]
        try:
            self.assertEqual(l, ref)
        finally:
            ndb.delete_multi(keys)


    def test_query_filter_neq(self):
        class Q(ndb.Model):
            a = ndb.StringProperty(repeated=True)
        e = [Q(a=['a', 'b', 'c']), Q(a=['z']), Q(a=['t','w'])]
        keys = ndb.put_multi(e)
        l = Q.query(Q.a != 'z').fetch()
        ref = [e[0], e[2]]
        try:
            # there is a big problem with default ordering, because we do not
            # have native monotonical order of _id by insert time, but by object
            # creation time...
            self.assertTrue(ref[0] in l and ref[1] in l and len(l) == 2)
            #self.assertEqual(ref, l)
        finally:
            ndb.delete_multi(keys)

        e = [Q(a=['a']), Q(a=['b']), Q(a=['z'])]
        keys = ndb.put_multi(e)
        l = Q.query(Q.a != 'z').fetch()
        ref = [e[0], e[1]]
        try:
            self.assertTrue(ref[0] in l and ref[1] in l and len(l) == 2)
        finally:
            ndb.delete_multi(keys)


    def test_query_filter_and(self):
        Q, e = self._gen_entities(100, ndb.IntegerProperty)
        keys = ndb.put_multi(e)
        l = Q.query(Q.a > 20, Q.a <= 60).order(Q.a).fetch()
        try:
            self.assertEqual(len(l), 40)
            self.assertEqual(l, e[21:61])
        finally:
            ndb.delete_multi(keys)


    def test_query_filter_generic_property(self):
        Q, e = self._gen_entities(10, ndb.GenericProperty)
        keys = ndb.put_multi(e)
        l = Q.query(Q.a < 4).order(Q.a).fetch()
        try:
            self.assertEqual(e[:4], l)
        except AssertionError:
            ndb.delete_multi(keys)
            raise
        try:
            l = Q.query().fetch()
            ll = sorted([x.a for x in l])
            ref = [0, 1, 2, 3, 4, '0', '1', '2', '3', '4']
            self.assertEqual(ll, ref)
        finally:
            ndb.delete_multi(keys)


    def test_query_filter_none(self):
        class Q(ndb.Model):
            a = ndb.StringProperty()
            b = ndb.IntegerProperty()
            c = ndb.GenericProperty()
        q = Q(a=None, b=None, c=None)
        k = q.put()
        try:
            def check(r):
                self.assertNotEqual(r, None)
                self.assertEqual(r.key, k)
                self.assertTrue(r.a is None and r.b is None and r.c is None)
            r1 = Q.query(Q.a == None).get()
            check(r1)
            r2 = Q.query(Q.a == None, Q.c==None).get()
            check(r2)
            self.assertEqual(Q.query(Q.b != None).get(), None)
        finally:
            k.delete()

    # QUERY OPTIONS

    def test_query_opt_ancestor(self):
        Q, (e1,e2) = self._gen_entities(2, ndb.IntegerProperty)
        e3 = Q(a=3455, parent=e1.put())
        e4 = Q(a=9999999, parent=e2.put())
        keys = ndb.put_multi([e3, e4])
        keys.extend([e1.key, e2.key])
        try:
            self.assertEqual(Q.query(ancestor=e1.key).fetch(), [e1,e3])
            self.assertEqual(Q.query(ancestor=e2.key).fetch(), [e2,e4])
        except AssertionError:
            ndb.delete_multi(keys)
            raise
        e5 = Q(a=110, parent=e4.key)
        keys.append(e5.put())
        try:
            self.assertEqual(Q.query(ancestor=e4.key).fetch(), [e4,e5])
        finally:
            ndb.delete_multi(keys)


    def test_query_opt_keys_only(self):
        Q, e = self._gen_entities(2, ndb.IntegerProperty)
        keys = ndb.put_multi(e)
        qo = ndb.QueryOptions(keys_only=True)
        l = Q.query().fetch(options=qo)
        try:
            self.assertTrue(all(map(lambda x: isinstance(x, ndb.Key), l)))
        finally:
            ndb.delete_multi(keys)


    def test_query_opt_count(self):
        Q, e = self._gen_entities(10, ndb.IntegerProperty)
        keys = ndb.put_multi(e)
        # test counting ability
        try:
            self.assertEqual(Q.query().count(), 10)
            self.assertEqual(Q.query(Q.a >= 5).count(), 5)
        finally:
            ndb.delete_multi(keys)
        # test count ranges (max results)
        Q, e = self._gen_entities(_MAXIMUM_RESULTS+10, ndb.IntegerProperty)
        keys = ndb.put_multi(e)
        try:
            self.assertEqual(Q.query().count(), _MAXIMUM_RESULTS+10)
        finally:
            ndb.delete_multi(keys)


    def test_query_opt_limit(self):
        class Q(ndb.Model):
            a = ndb.IntegerProperty()
        q1 = Q(a=4)
        k1 = q1.put()
        q2 = Q(a=7)
        k2 = q2.put()
        q3 = Q(a=930489759)
        k3 = q3.put()
        try:
            for i in xrange(1,4):
                self.assertEqual(len(Q.query().fetch(i)), i)
        finally:
            ndb.delete_multi([k1, k2, k3])


    def test_query_opt_offset(self):
        class Q(ndb.Model):
            a = ndb.IntegerProperty()
        q1 = Q(a=1)
        k1 = q1.put()
        q2 = Q(a=999)
        k2 = q2.put()
        q3 = Q(a=109238503475)
        k3 = q3.put()
        try:
            self.assertTrue(len(Q.query().fetch(offset=1)), 2)
            self.assertEqual(len(Q.query().fetch(offset=2)), 1)
            self.assertEqual(Q.query().fetch(offset=3), [])
        finally:
            ndb.delete_multi([k1, k2, k3])
        # test MAX offset
        Q, e = self._gen_entities(_MAX_QUERY_OFFSET+10, ndb.IntegerProperty)
        keys = ndb.put_multi(e)
        try:
            l = Q.query().fetch(offset=_MAX_QUERY_OFFSET)
            self.assertEqual(len(l), 10)
        except AssertionError:
            ndb.delete_multi(keys)
            raise
        try:
            l = Q.query().fetch(offset=_MAX_QUERY_OFFSET+100)
            self.assertEqual(len(l), 0)
        finally:
            ndb.delete_multi(keys)


    #TODO: def test_query_opt_offset_n_limit(self):


    # QUERY SORT ORDERS

    def test_query_order_int(self):
        Q, e = self._gen_entities(10, ndb.IntegerProperty)
        keys = ndb.put_multi(e)
        # test ascending order
        l_asc = Q.query().order(Q.a).fetch()
        ll_asc = sorted(e, lambda x,y: x.a < y.a)
        try:
            self.assertEqual(ll_asc, l_asc)
        except AssertionError, e:
            ndb.delete_multi(keys)
            raise
        # test descending order
        l_desc = Q.query().order(-Q.a).fetch()
        ll_desc = [x for x in reversed(ll_asc)]
        try:
            self.assertEqual(ll_desc, l_desc)
        finally:
            ndb.delete_multi(keys)


    def test_query_order_int_repeated(self):
        class Q(ndb.Model):
            a = ndb.IntegerProperty(repeated=True)

        seq = [range(i, i + (i % 5) + 1) for i in xrange(20)]
        e = [Q(a=l) for l in seq]
        keys = ndb.put_multi(e)

        l = map(lambda x: x.a, Q.query().order(Q.a).fetch())
        try:
            self.assertEqual(l, seq)
        except AssertionError, e:
            ndb.delete_multi(keys)
            raise

        l_rev = map(lambda x: x.a, Q.query().order(-Q.a).fetch())
        seq_rev = [[19, 20, 21, 22, 23], [18, 19, 20, 21], [17, 18, 19],
          [14, 15, 16, 17, 18], [16, 17], [13, 14, 15, 16], [15], [12, 13, 14],
          [9, 10, 11, 12, 13], [11, 12], [8, 9, 10, 11], [10], [7, 8, 9],
          [4, 5, 6, 7, 8], [6, 7], [3, 4, 5, 6], [5], [2, 3, 4], [1, 2], [0]]
        try:
            self.assertEqual(l_rev, seq_rev)
        finally:
            ndb.delete_multi(keys)


    def test_query_order_str(self):
        Q, e = self._gen_entities(10, ndb.StringProperty)
        keys = ndb.put_multi(e)
        l_desc = map(lambda x: x.a, Q.query().order(-Q.a).fetch())
        ll_desc = sorted(string.letters, reverse=True)
        try:
            self.assertEqual(ll_desc, l_desc)
        finally:
            ndb.delete_multi(keys)


    def test_query_order_geopt(self):
        class Q(ndb.Model):
            a = ndb.GeoPtProperty()
        
        e = [Q(a=ndb.GeoPt(i*2.17, (10-i)*1.17)) for i in xrange(10)]
        keys = ndb.put_multi(e)
        l = map(lambda x: x.a, Q.query().order(Q.a).fetch())
        ref = [datastore_types.GeoPt(0.0, 11.7), datastore_types.GeoPt(2.17, 10.53),
               datastore_types.GeoPt(4.34, 9.36), datastore_types.GeoPt(6.51, 8.19),
               datastore_types.GeoPt(8.68, 7.02), datastore_types.GeoPt(10.85, 5.85),
               datastore_types.GeoPt(13.02, 4.68), datastore_types.GeoPt(15.19, 3.51),
               datastore_types.GeoPt(17.36, 2.34), datastore_types.GeoPt(19.53, 1.17)]
        try:
            self.assertEqual(ref, l)
        except AssertionError:
            ndb.delete_multi(keys)
            raise

        l_desc = map(lambda x: x.a, Q.query().order(-Q.a).fetch())
        ref_desc = [x for x in reversed(ref)]
        try:
            self.assertEqual(ref_desc, l_desc)
        finally:
            ndb.delete_multi(keys)
        
        # test on other dataset
        e = [Q(a=ndb.GeoPt(i*2.17, i*1.17)) for i in xrange(10)]
        keys = ndb.put_multi(e)
        l = map(lambda x: x.a, Q.query().order(Q.a).fetch())
        ref = [datastore_types.GeoPt(0.0, 0.0), datastore_types.GeoPt(2.17, 1.17),
               datastore_types.GeoPt(4.34, 2.34), datastore_types.GeoPt(6.51, 3.51),
               datastore_types.GeoPt(8.68, 4.68), datastore_types.GeoPt(10.85, 5.85),
               datastore_types.GeoPt(13.02, 7.02), datastore_types.GeoPt(15.19, 8.19),
               datastore_types.GeoPt(17.36, 9.36), datastore_types.GeoPt(19.53, 10.53)]
        try:
            self.assertEqual(ref, l)
        except AssertionError:
            ndb.delete_multi(keys)
            raise

        l_desc = map(lambda x: x.a, Q.query().order(-Q.a).fetch())
        ref_desc = [x for x in reversed(ref)]
        try:
            self.assertEqual(ref_desc, l_desc)
        finally:
            ndb.delete_multi(keys)


    def test_query_order_geopt_repeated(self):
        class Q(ndb.Model):
            a = ndb.GeoPtProperty(repeated=True)
         
        e = [Q(a=[ndb.GeoPt(i*2.17, x*1.17) for x in xrange(i*13 % 7)]) for i in xrange(5)]
        keys = ndb.put_multi(e)
        l = map(lambda x: x.a, Q.query().order(Q.a).fetch())
        ref = [[datastore_types.GeoPt(2.17, 0.0), datastore_types.GeoPt(2.17, 1.17),
                datastore_types.GeoPt(2.17, 2.34), datastore_types.GeoPt(2.17, 3.51),
                datastore_types.GeoPt(2.17, 4.68), datastore_types.GeoPt(2.17, 5.85)],
               [datastore_types.GeoPt(4.34, 0.0), datastore_types.GeoPt(4.34, 1.17),
                datastore_types.GeoPt(4.34, 2.34), datastore_types.GeoPt(4.34, 3.51),
                datastore_types.GeoPt(4.34, 4.68)],
               [datastore_types.GeoPt(6.51, 0.0), datastore_types.GeoPt(6.51, 1.17),
                datastore_types.GeoPt(6.51, 2.34), datastore_types.GeoPt(6.51, 3.51)],
               [datastore_types.GeoPt(8.68, 0.0), datastore_types.GeoPt(8.68, 1.17),
                datastore_types.GeoPt(8.68, 2.34)]]
        try:
            self.assertEqual(l, ref)
        finally:
            ndb.delete_multi(keys)


    def test_query_order_undefined_property(self):
        _, e = self._gen_entities(4, ndb.IntegerProperty)
        keys = ndb.put_multi(e)
        class Q(ndb.Model):
            b = ndb.StringProperty()
        bb = Q(b="a")
        k = bb.put()
        l = Q.query().order(Q.b).fetch()
        try:
            self.assertEqual(l, [bb])
        finally:
            k.delete()
            ndb.delete_multi(keys)


    def test_query_order_structured_property(self):
        Q, e = self._gen_entities(4, ndb.StructuredProperty)
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.a.m).fetch()
        try:
            self.assertEqual(l, e)
        finally:
            ndb.delete_multi(keys)


    def test_query_order_structured_property_unorderable_inner(self):
        Q, e = self._gen_entities(4, ndb.StructuredProperty, ndb.TextProperty)
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.a.m).fetch()
        try:
            self.assertEqual(l, [])
        finally:
            ndb.delete_multi(keys)
        Q, e = self._gen_entities(4, ndb.StructuredProperty, ndb.BlobProperty)
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.a.m).fetch()
        try:
            self.assertEqual(l, [])
        finally:
            ndb.delete_multi(keys)


    def test_query_order_computed_property(self):
        class Q(ndb.Model):
            a = ndb.StringProperty(repeated=True)
            a_len = ndb.ComputedProperty(lambda self: len(self.a))
        e = [Q(a=['a', 'b', 'c']), Q(a=['z']), Q(a=['t','w'])]
        e_ref = [e[1], e[2], e[0]]
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.a_len).fetch()
        try:
            self.assertEqual(l, e_ref)
        finally:
            ndb.delete_multi(keys)


    def test_query_order_unorderable_property(self):
        Q, e = self._gen_entities(4, ndb.TextProperty)
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.a).fetch()
        try:
            self.assertEqual(l, [])
        finally:
            ndb.delete_multi(keys)

        Q, e = self._gen_entities(4, ndb.BlobProperty)
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.a).fetch()
        try:
            self.assertEqual(l, [])
        finally:
            ndb.delete_multi(keys)

        Q, e = self._gen_entities(4, ndb.JsonProperty)
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.a).fetch()
        try:
            self.assertEqual(l, [])
        finally:
            ndb.delete_multi(keys)

        Q, e = self._gen_entities(4, ndb.PickleProperty)
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.a).fetch()
        try:
            self.assertEqual(l, [])
        finally:
            ndb.delete_multi(keys)

        Q, e = self._gen_entities(4, ndb.LocalStructuredProperty, ndb.IntegerProperty)
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.a).fetch()
        try:
            self.assertEqual(l, [])
        finally:
            ndb.delete_multi(keys)
        # TODO: what more is unorderable? GenericProperty?


    def test_query_order_unorderable_repeated_property(self):
        class Q(ndb.Model):
            a = ndb.BlobProperty(repeated=True)
        e = [Q(a=['a', 'b', 'c']), Q(a=['z']), Q(a=['t','w'])]
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.a).fetch()
        try:
            self.assertEqual(l, [])
        finally:
            ndb.delete_multi(keys)

        # text property
        class Q(ndb.Model):
            a = ndb.TextProperty(repeated=True)
        e = [Q(a=['a', 'b', 'c']), Q(a=['z']), Q(a=['t','w'])]
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.a).fetch()
        try:
            self.assertEqual(l, [])
        finally:
            ndb.delete_multi(keys)

        # structured property
        class I(ndb.Model):
            i = ndb.StringProperty()
            j = ndb.IntegerProperty()
        class Q(ndb.Model):
            a = ndb.StructuredProperty(I, repeated=True)
        e = [
            Q(a=[I(i="a",j=1), I(i="b",j=2)]),
            Q(a=[I(i="foo bar",j=861)]),
            Q(a=[I(i="a",j=1), I(i="b",j=2), I(i="c",j=3)])
        ]
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.a).fetch()
        try:
            self.assertEqual(l, [])
        finally:
            ndb.delete_multi(keys)

        # local structured property
        class I(ndb.Model):
            i = ndb.StringProperty()
            j = ndb.IntegerProperty()
        class Q(ndb.Model):
            a = ndb.LocalStructuredProperty(I, repeated=True)
        e = [
            Q(a=[I(i="a",j=1), I(i="b",j=2)]),
            Q(a=[I(i="foo bar",j=861)]),
            Q(a=[I(i="a",j=1), I(i="b",j=2), I(i="c",j=3)])
        ]
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.a).fetch()
        try:
            self.assertEqual(l, [])
        finally:
            ndb.delete_multi(keys)


    def _datetime_test_wrapper(self, type_):
        Q, e = self._gen_entities(4, type_)
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.a).fetch()
        try:
            self.assertEqual(e, l)
        except AssertionError:
            ndb.delete_multi(keys)
            raise

        # descending order
        l_desc = Q.query().order(-Q.a).fetch()
        try:
            self.assertEqual(l_desc, [x for x in reversed(e)])
        finally:
            ndb.delete_multi(keys)


    def test_query_order_date_property(self):
        self._datetime_test_wrapper(ndb.DateProperty)


    def test_query_order_datetime_property(self):
        self._datetime_test_wrapper(ndb.DateTimeProperty)


    def test_query_order_time_property(self):
        self._datetime_test_wrapper(ndb.TimeProperty)


    def test_query_order_named_property(self):
        """Test if stub does work correctly with named properties.

        Named properties have another name in datastore, than in model.
        """
        class Q(ndb.Model):
            name = ndb.StringProperty('n')
        e = [Q(name=s) for s in string.letters]
        keys = ndb.put_multi(e)
        l = map(lambda x: x.name, Q.query().order(Q.name).fetch())
        l_ref = sorted(string.letters)
        try:
            self.assertEqual(l, l_ref)
        finally:
            ndb.delete_multi(keys)


    def test_query_order_by_multiple_prop(self):
        class Q(ndb.Model):
            i = ndb.StringProperty()
            j = ndb.IntegerProperty()
        e = []
        for x in xrange(5):
            for y in xrange(3):
                e.append(Q(i=string.letters[20-x], j=y))
        keys = ndb.put_multi(e)
        l = Q.query().order(Q.i, -Q.j).fetch()
        ref = [(u'q', 2), (u'q', 1), (u'q', 0), (u'r', 2), (u'r', 1),
               (u'r', 0), (u's', 2), (u's', 1), (u's', 0), (u't', 2),
               (u't', 1), (u't', 0), (u'u', 2), (u'u', 1), (u'u', 0)]
        try:
            self.assertEqual(map(lambda x: (x.i, x.j), l), ref)
        finally:
            ndb.delete_multi(keys)


    def test_query_projection(self):
        """Test simple projection cases using unerepeated properties
           and one repeated property. These cases are pretty usual.
        """
        class Q(ndb.Model):
            a = ndb.StringProperty()
            b = ndb.IntegerProperty(repeated=True)

        q1 = Q(a='bar1', b=[1])
        k = q1.put()
        try:
            l = Q.query().get(projection=['b'], use_cache=False, use_memcache=False)
            self.assertEqual(l._to_dict(), {'b':[1]})
        finally:
            del Q
            k.delete()
        class Inner(ndb.Model):
            i = ndb.IntegerProperty()
            j = ndb.StringProperty(repeated=True)
            l = ndb.IntegerProperty(repeated=True)
        class Q(ndb.Model):
            s = ndb.StructuredProperty(Inner)
            a = ndb.StringProperty()
        q1 = Q(s=Inner(i=1, j=['a', 'b', 'foo'], l=[7,8]), a='blew')
        k1 = q1.put()
        try:
            # test simple get on structured property
            l = Q.query().get(projection=['s.i'], use_cache=False, use_memcache=False)
            self.assertEqual(l._to_dict(), {'s': {'i':1}})
            # test fetch on unrepeated property
            l = Q.query().fetch(projection=['s.i'], use_cache=False, use_memcache=False)
            self.assertTrue(len(l) == 1)
            self.assertEqual(l[0]._to_dict(), {'s': {'i':1}})
            # test fetch: one repeated property in structured property
            l = Q.query().fetch(projection=['s.j'], use_cache=False, use_memcache=False)
            self.assertTrue(len(l) == 3)
            self.assertEqual(l[0]._to_dict(), {'s': {'j':['a']}})
            self.assertEqual(l[1]._to_dict(), {'s': {'j':['b']}})
            self.assertEqual(l[2]._to_dict(), {'s': {'j':['foo']}})
            # test fetch: more projected properties, one repeated
            l = Q.query().fetch(projection=['s.j', 'a'], use_cache=False, use_memcache=False)
            self.assertTrue(len(l) == 3)
            self.assertEqual(l[0]._to_dict(), {'s': {'j':['a']}, 'a':'blew'})
            self.assertEqual(l[1]._to_dict(), {'s': {'j':['b']}, 'a':'blew'})
            self.assertEqual(l[2]._to_dict(), {'s': {'j':['foo']}, 'a': 'blew'})
        finally:
            k1.delete()


    def test_query_projection_multiple_repeated_properties(self):
        """Test wicked behaviour of datastore when using projection
           on more than one repeated property.
        """
        raise unittest.SkipTest()
        class Q(ndb.Model):
            x = ndb.StringProperty(repeated=True)
            y = ndb.IntegerProperty(repeated=True)
            z = ndb.BooleanProperty()
        q = Q(x=['a', 'b', 'a'], y=[1,2,2], z=False)
        k = q.put()
        try:
            l = Q.query().order(Q.x,Q.y).fetch(projection=['x', 'y'])
            self.assertTrue(len(l) == 4)
            self.assertEqual(l[0]._to_dict(), {'x':['a'], 'y':[1]})
            self.assertEqual(l[1]._to_dict(), {'x':['a'], 'y':[2]})
            self.assertEqual(l[2]._to_dict(), {'x':['b'], 'y':[1]})
            self.assertEqual(l[3]._to_dict(), {'x':['b'], 'y':[2]})
        finally:
            k.delete()


    def test_query_projection_with_filter(self):
        class Q(ndb.Model):
            x = ndb.StringProperty(repeated=True)
            y = ndb.IntegerProperty()
            z = ndb.BooleanProperty()
        q = Q(x=['a', 'b', 'a'], y=1, z=False)
        q2 = Q(x=['c', 'd', 'c'], y=3, z=True)
        k = q.put()
        k2 = q2.put()
        try:
            # test one filter on unrepeated property
            l = Q.query(Q.y < 3).order(Q.y, Q.x).fetch(projection=['x', 'y'])
            self.assertTrue(len(l) == 2)
            self.assertEqual(l[0]._to_dict(), {'x':['a'], 'y':1})
            self.assertEqual(l[1]._to_dict(), {'x':['b'], 'y':1})
        finally:
            ndb.delete_multi([k, k2])
        class Q(ndb.Model):
            x = ndb.IntegerProperty(repeated=True)
            y = ndb.StringProperty()
        q = Q(x=[1,2,3], y='foo')
        k = q.put()
        try:
            # test one filter on repeated property
            l = Q.query(Q.x < 3).fetch(projection=['x'])
            self.assertTrue(len(l) == 2)
            self.assertEqual(l[0]._to_dict(), {'x':[1]})
            self.assertEqual(l[1]._to_dict(), {'x':[2]})
            # test multiple filters on repeated property
            l = Q.query(Q.x < 3, Q.x > 1).fetch(projection=['x'])
            self.assertTrue(len(l) == 1)
            self.assertEqual(l[0]._to_dict(), {'x':[2]})
        finally:
            k.delete()


    def test_query_projection_unindexed(self):
        """Projection query should fail on unindexable properties like text or blob."""
        class Q(ndb.Model):
            a = ndb.StringProperty()
            b = ndb.TextProperty()
        q = Q(a='a', b='looong text')
        k = q.put()
        with self.assertRaises(ndb.BadProjectionError):
            l = Q.query().fetch(projection=['b'])
        k.delete()


class TestDatastoreFileStub(_DatastoreStubTests, unittest.TestCase):
    """
    In order to validate tests againts existing stub, we need to
    fire tests on DatastoreFileStub.
    """
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        _DatastoreStubTests.__init__(self)

    @classmethod
    def setUpClass(cls):
        datastore_stub = DatastoreFileStub(APP_ID, None, None)
        apiproxy_stub_map.apiproxy.ReplaceStub('datastore_v3', datastore_stub)

        underline = '~'*len(cls.__name__)
        sys.stderr.write(underline + '\n' +cls.__name__ + '\n' + underline \
                         + textwrap.dedent(cls.__doc__) + '\n')


class TestDatastoreMongodbStub(_DatastoreStubTests, unittest.TestCase):
    """
    Test mongodb stub.
    """
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        _DatastoreStubTests.__init__(self)

    @classmethod
    def tearDownClass(cls):
        cls._datastore_stub._datastore.clear()

    @classmethod
    def setUpClass(cls):
        datastore_stub = DatastoreMongoDBStub(APP_ID)
        apiproxy_stub_map.apiproxy.ReplaceStub('datastore_v3', datastore_stub)
        cls._datastore_stub = datastore_stub
        underline = '~'*len(cls.__name__)
        sys.stderr.write(underline + '\n' +cls.__name__ + '\n' + underline \
                         + textwrap.dedent(cls.__doc__) + '\n')

