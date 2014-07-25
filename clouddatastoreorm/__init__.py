import os, sys
import google
# setup clouddatastore_stub
import datastore_clouddatastore_stub
from google.appengine.api import apiproxy_stub_map

def connect(dataset_id, email, key_path):
    stub_map = apiproxy_stub_map.APIProxyStubMap()

    os.environ["APPLICATION_ID"] = dataset_id
    stub = datastore_clouddatastore_stub.DatastoreCloudDatastoreStub(dataset_id, email, key_path)
    stub_map.RegisterStub("datastore_v3", stub)

    from google.appengine.api.memcache import memcache_stub
    memcache_stub = memcache_stub.MemcacheServiceStub()
    stub_map.RegisterStub("memcache", memcache_stub)


    apiproxy_stub_map.apiproxy = stub_map

