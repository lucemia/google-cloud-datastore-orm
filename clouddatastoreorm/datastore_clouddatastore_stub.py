import array
import itertools
import logging
import threading
import weakref
import converter
from google.appengine.api import apiproxy_stub
from google.appengine.api import datastore_types
from google.appengine.datastore import datastore_pb
from google.appengine.datastore import datastore_stub_util
from google.appengine.datastore import sortable_pb_encoder
from google.appengine.runtime import apiproxy_errors
from google.appengine.datastore import entity_pb
from oauth2client import client
from googledatastore import datastore_v1_pb2 as datastore_pb

import googledatastore
converter = converter.Converter()

class DatastoreCloudDatastoreStub(datastore_stub_util.BaseDatastore,
                          apiproxy_stub.APIProxyStub,
                          datastore_stub_util.DatastoreStub):
  def __init__(self,
    app_id,
    email=None,
    key_path=None,
    require_indexes=False,
    verbose=False,
    service_name='datastore_v3',
    trusted=False,
    consistency_policy=None,
    root_path=None,
    use_atexit=True,
    auto_id_policy=datastore_stub_util.SEQUENTIAL):
    datastore_stub_util.BaseDatastore.__init__(self, require_indexes,
                                               consistency_policy,
                                               use_atexit,
                                               auto_id_policy)
    apiproxy_stub.APIProxyStub.__init__(self, service_name)
    datastore_stub_util.DatastoreStub.__init__(self, weakref.proxy(self),
                                               app_id, trusted, root_path)

    self.__email = email
    self.__key_path = key_path
    self.__verbose = verbose

    self.__id_map_sequential = {}
    self.__id_map_scattered = {}
    self.__id_counter_tables = {
        datastore_stub_util.SEQUENTIAL: ('IdSeq', self.__id_map_sequential),
        datastore_stub_util.SCATTERED: ('ScatteredIdCounters',
                                         self.__id_map_scattered),
        }
    self.__id_lock = threading.Lock()

    if self.__email and self.__key_path:
      credentials = client.SignedJwtAssertionCredentials(
            email, open(key_path).read(), googledatastore.connection.SCOPE)
      self.__connection = googledatastore.connection.Datastore(app_id, credentials)
    else:
      self.__connection = googledatastore.connection.Datastore(app_id)

    self.__connection.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')

    self.__connection_lock = threading.RLock()

    self.__namespaces = set()

    self.__query_history = {}


  def _GetConnection(self):
    return self.__connection

  def _GetEntitiesInEntityGroup(self, entity_group):
    connection = self._GetConnection()

    if isinstance(entity_group, entity_pb.Reference):
      req = googledatastore.LookupRequest()
      v1_key = req.key.add()
      converter.v3_entity_ref_to_v1_key(entity_group, v1_key)

      v1_key.partition_id.Clear()

      resp = connection.lookup(req)

      if resp.found:
        v3_entities = []
        for v1_entity_result in resp.found:
          v3_entity = entity_pb.EntityProto()
          converter.v1_to_v3_entity(v1_entity_result.entity, v3_entity)
          v3_entity.mutable_key().set_app(entity_group.app())
          v3_entity.mutable_key().set_name_space(entity_group.name_space())
          v3_entities.append(v3_entity)

        return dict((datastore_types.ReferenceToKeyValue(entity.key()), entity)
                  for entity in v3_entities)

    return {}

  def _Put(self, v3_entity, insert):
    connection = self._GetConnection()
    v3_entity = datastore_stub_util.StoreEntity(v3_entity)
    req = googledatastore.CommitRequest()

    req.mode = datastore_pb.CommitRequest.NON_TRANSACTIONAL
    v1_entity = req.mutation.upsert.add()
    converter.v3_to_v1_entity(v3_entity, v1_entity)
    v1_entity.key.partition_id.Clear()
    resp = connection.commit(req)

  def _Delete(self, v3_key):
    connection = self._GetConnection()
    req = googledatastore.CommitRequest()

    req.mode = datastore_pb.CommitRequest.NON_TRANSACTIONAL
    v1_key = req.mutation.delete.add()
    converter.v3_to_v1_key(v3_key, v1_key)
    v1_key.partition_id.Clear()

    resp = connection.commit(req)


