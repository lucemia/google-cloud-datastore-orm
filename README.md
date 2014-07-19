google-cloud-datastore-orm
==========================

# Introduction
Thanks for the great works [dbWrapper](https://github.com/transceptor-technology/dbWrapper)
[gcloud-python](https://github.com/GoogleCloudPlatform/gcloud-python)
, it is much easier to use cloud datastore now :)

This project focus on create an alternative work-through solution to bridge the db/ndb model and cloud datastore before official solution [release](https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/2),
I did some experiment based on previous discussion and [comment](http://stackoverflow.com/a/16671694/656408).

It is not a complete implementation yet.
just a proof of concept, but it did work.

## Sample 
```python
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
```

While implement the orm, there are two method come up my mind:

1. monkey patch the `datastore_rpc.py`, `async_get` ... or `_make_rpc_call` method. however, this method need to mock rpc object. I don't have a clear thought about how to do it yet.

2. create a new datastore_stub and register to api_proxy, just like the `datastore_sqlite_stub.py` or `datastore_file_stub.py` did in development server. This method looks like easier. Currently, I choose this approach.

The project is still under construction.
Any feedback or comments are welcome.
