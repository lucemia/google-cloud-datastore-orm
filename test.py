import os, sys
import google

# inject protobuf
google.__path__.append(os.path.join('lib', 'google'))


# setup clouddatastore_stub
import datastore_clouddatastore_stub
from google.appengine.api import apiproxy_stub_map
stub_map = apiproxy_stub_map.APIProxyStubMap()


dataset_id = "YOUR_DATASET_ID"
email  = "YOUR_SERVICE_ACCOUNT"
key_path = "YOUR_KEY_PATH"

try:
    from local_setting import *
except:
    pass

os.environ["APPLICATION_ID"] = dataset_id
stub = datastore_clouddatastore_stub.DatastoreCloudDatastoreStub(dataset_id, email, key_path)
stub_map.RegisterStub("datastore_v3", stub)

from google.appengine.api.memcache import memcache_stub
memcache_stub = memcache_stub.MemcacheServiceStub()
stub_map.RegisterStub("memcache", memcache_stub)

apiproxy_stub_map.apiproxy = stub_map

from google.appengine.ext import db, ndb


class AdAction(ndb.Model):
    #test_int = db.IntegerProperty()
    #test_string = db.TextProperty()
    action_pattern = ndb.StringProperty()
    advertiser = ndb.IntegerProperty()

t = AdAction.get_by_id(2)
print t
t.put()
