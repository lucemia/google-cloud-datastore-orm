from google.appengine.datastore import datastore_pbs

# # if using gcloud-python, the meaning will loss
# class Converter(datastore_pbs._EntityConverter):
#     def v3_get_request_to_v1_lookup_request(self, v3_request, v1_request):
#         v1_request.Clear()
#         if v3_request.has_allow_deferred():
#             pass
#         if v3_request.has_failover_ms():
#             pass
#         if v3_request.has_header():
#             pass
#         if v3_request.has_strong():
#             v1_request.read_options.ReadConsistency = datastore_v1_pb2.ReadConsistency.STRONG
#         if v3_request.has_transaction():
#             raise
#         for key_v3 in v3_request.key_list():
#             key_v1 = v1_request.key.add()
#             path_v3 = key_v3.path()
#             for element_v3 in path_v3.element_list():
#                 element_v1 = key_v1.path_element.add()
#                 if element_v3.has_name():
#                     element_v1.name = element_v3.name()
#                 if element_v3.has_type():
#                     element_v1.kind = element_v3.type()
#                 if element_v3.has_id():
#                     element_v1.id = element_v3.id()

#     def v1_lookup_response_to_v3_get_response(self, v1_response, v3_response):
#         v3_response.Clear()
#         import pdb; pdb.set_trace()
#         for entity_result_v1 in v1_response.found:
#             v3_entity = v3_response.add_entity()

#             self.v1_to_v3_entity(entity_result_v1.entity, v3_entity.mutable_entity())

#     def __v1_to_v3_property(self, property_name, is_multi, v1_value, v3_property):
#         assert not v1_value.list_value, "v1 list_value not convertable to v3"

#         v3_property.Clear()
#         v3_property.set_name(property_name)
#         v3_property.set_multiple(is_multi)
#         self.v1_value_to_v3_property_value(v1_value, v3_property.mutable_value())
#         v1_meaning = None
#         if v1_value.HasField("meaning"):
#             v1_meaning = v1_value.meaning

#         if v1_value.HasField('timestamp_microseconds_value'):
#             v3_property.set_meaning(entity_pb.Property.GD_WHEN)
#         elif v1_value.HasField('blob_key_value'):
#             v3_property.set_meaning(entity_pb.Property.BLOBKEY)
#         elif v1_value.HasField('blob_value'):
#             if v1_meaning == datastore_pbs.MEANING_ZLIB:
#                 v3_property.set_meaning_uri(datastore_pbs.URI_MEANING_ZLIB)
#             if v1_meaning == entity_pb.Property.BYTESTRING:
#                 if v1_value.indexed:
#                     pass
#             else:
#                 if v1_value.indexed:
#                     v3_property.set_meaning(entity_pb.Property.BYTESTRING)
#                 else:
#                     v3_property.set_meaning(entity_pb.Property.BLOB)
#                 v1_meaning = None
#         elif v1_value.HasField('entity_value'):
#             if v1_meaning != datastore_pbs.MEANING_GEORSS_POINT:
#                 if (v1_meaning != datastore_pbs.MEANING_PREDEFINED_ENTITY_POINT
#                     and v1_meaning != datastore_pbs.MEANING_PREDEFINED_ENTITY_USER):
#                     v3_property.set_meaning(entity_pb.Property.ENTITY_PROTO)
#                 v1_meaning = None

#         else:
#             pass

#         if v1_meaning is not None:
#             v3_property.set_meaning(v1_meaning)

#     def __add_v3_property(self, property_name, is_multi, v1_value, v3_entity):
#         if v1_value.indexed:
#             self.__v1_to_v3_property(property_name, is_multi, v1_value, v3_entity.add_property())
#         else:
#             self.__v1_to_v3_property(property_name, is_multi, v4_value, v3_entity.add_raw_property())

#     def v1_to_v3_reference(self, v1_key, v3_ref):
#         v3_ref.Clear()
#         if v1_key.HasField('partition_id'):
#             if v1_key.partition_id.HasField('dataset_id'):
#                 v3_ref.set_app(v1_key.partition_id.dataset_id)
#             if v1_key.partition_id.HasField('namespace'):
#                 v3_ref.set_name_space(v1_key.partition_id.namespace)
#         for v1_element in v1_key.path_element:
#             v3_element = v3_ref.mutable_path().add_element()
#             v3_element.set_type(v1_element.kind)
#             if v1_element.HasField('id'):
#                 v3_element.set_id(v1_element.id)
#             if v1_element.HasField('name'):
#                 v3_element.set_name(v1_element.name)

#     def v1_to_v3_entity(self, v1_entity, v3_entity):
#         v3_entity.Clear()
#         for v1_property in v1_entity.property:
#             property_name = v1_property.name
#             v1_value = v1_property.value

#             if v1_value.list_value:
#                 for v1_sub_value in v1_value.list_valie:
#                     self.__add_v3_property(property_name, True, v1_sub_value, v3_entity)
#             else:
#                 self.__add_v3_property(property_name, False, v1_value, v3_entity)

#         if v1_entity.HasField('key'):
#             v1_key = v1_entity.key
#             self.v1_to_v3_reference(v1_key, v3_entity.mutable_key())
#             v3_ref = v3_entity.key()
#             self.v3_reference_to_group(v3_ref, v3_entity.mutable_entity_group())
#         else:
#             pass

#     def v1_value_to_v3_property_value(self, v1_value, v3_value):
#         v3_value.Clear()
#         if v1_value.HasField('boolean_value'):
#             v3_value.set_booleanvalue(v1_value.boolean_value)
#         elif v1_value.HasField('integer_value'):
#             v3_value.set_int64value(v1_value.integer_value)
#         elif v1_value.HasField('double_value'):
#             v3_value.set_double_value(v1_value.double_value)
#         elif v1_value.HasField('timestamp_microseconds_value'):
#             v3_value.set_int64value(v1_value.timestamp_microseconds_value)
#         elif v1_value.HasField('key_value'):
#             v3_ref = entity_pb.Reference()
#             self.v1_to_v3_reference(v1_value.key_value, v3_ref)
#             self.v3_reference_to_v3_property_value(v3_ref, v3_value)
#         elif v1_value.HasField('blob_key_value'):
#             v3_value.set_stringvalue(v1_value.blob_key_value)
#         elif v1_value.HasField('string_value'):
#             v3_value.set_stringvalue(v1_value.string_value)
#         elif v1_value.HasField('blob_value'):
#             v3_value.set_stringvalue(v1_value.blob_value)
#         elif v1_value.HasField('entity_value'):
#             v1_entity_value = v1_value.entity_value
#             v1_meaning = v1_value.meaning
#             if(v1_meaning == datastore_pbs.MEANING_GEORSS_POINT or v1_meaning == datastore_pbs.MEANING_PREDEFINED_ENTITY_POINT):
#                 self.__v1_to_v3_point_value(v1_entity_value, v3_value.mutable_pointvalue())
#             elif v1_meaning == datastore_pbs.MEANING_PREDEFINED_ENTITY_USER:
#                 self.__v1_to_v3_user_value(v1_entity_value, v3_value.mutable_uservalue())
#             else:
#                 v3_entity_value = entity_pb.EntityProto()
#                 self.v1_to_v3_entity(v1_entity_value, v3_entity_value)
#                 v3_value.set_stringvalue(v3_entity_value.SerializePartialToString())
#         else:
#             pass



class Converter(datastore_pbs._EntityConverter):
    def v3_get_request_to_v1_lookup_request(self, v3_request, v1_request):
        v1_request.Clear()
        if v3_request.has_allow_deferred():
            pass
        if v3_request.has_failover_ms():
            pass
        if v3_request.has_header():
            pass
        if v3_request.has_strong():
            v1_request.read_options.ReadConsistency = datastore_v1_pb2.ReadConsistency.STRONG
        if v3_request.has_transaction():
            raise
        for key_v3 in v3_request.key_list():
            key_v1 = v1_request.key.add()
            path_v3 = key_v3.path()
            for element_v3 in path_v3.element_list():
                element_v1 = key_v1.path_element.add()
                if element_v3.has_name():
                    element_v1.name = element_v3.name()
                if element_v3.has_type():
                    element_v1.kind = element_v3.type()
                if element_v3.has_id():
                    element_v1.id = element_v3.id()

    def v1_lookup_response_to_v3_get_response(self, v1_response, v3_response):
        v3_response.Clear()
        import pdb; pdb.set_trace()
        for entity_result_v1 in v1_response.found:
            v3_entity = v3_response.add_entity()

            self.v1_to_v3_entity(entity_result_v1.entity, v3_entity.mutable_entity())


    def v3_entity_ref_to_v1_key(self, v3_ref, v1_key):
        v1_key.Clear()

        if v3_ref.has_name_space():
            v1_key.partition_id.namespace = v3_ref.name_space()

        if v3_ref.has_app():
            v1_key.partition_id.dataset_id = v3_ref.app()

        v3_path = v3_ref.path()
        for v3_element in v3_path.element_list():
            v1_element = v1_key.path_element.add()
            if v3_element.has_name():
                v1_element.name = v3_element.name()
            if v3_element.has_type():
                v1_element.kind = v3_element.type()
            if v3_element.has_id():
                v1_element.id = v3_element.id()

    def __v1_to_v3_property(self, property_name, is_multi, v1_value, v3_property):
        assert not v1_value.list_value, "v1 list_value not convertable to v3"

        v3_property.Clear()
        v3_property.set_name(str(property_name))
        v3_property.set_multiple(is_multi)
        self.v1_value_to_v3_property_value(v1_value, v3_property.mutable_value())
        v1_meaning = None
        if v1_value.HasField("meaning"):
            v1_meaning = v1_value.meaning

        if v1_value.HasField('timestamp_microseconds_value'):
            v3_property.set_meaning(entity_pb.Property.GD_WHEN)
        elif v1_value.HasField('blob_key_value'):
            v3_property.set_meaning(entity_pb.Property.BLOBKEY)
        elif v1_value.HasField('blob_value'):
            if v1_meaning == datastore_pbs.MEANING_ZLIB:
                v3_property.set_meaning_uri(datastore_pbs.URI_MEANING_ZLIB)
            if v1_meaning == entity_pb.Property.BYTESTRING:
                if v1_value.indexed:
                    pass
            else:
                if v1_value.indexed:
                    v3_property.set_meaning(entity_pb.Property.BYTESTRING)
                else:
                    v3_property.set_meaning(entity_pb.Property.BLOB)
                v1_meaning = None
        elif v1_value.HasField('entity_value'):
            if v1_meaning != datastore_pbs.MEANING_GEORSS_POINT:
                if (v1_meaning != datastore_pbs.MEANING_PREDEFINED_ENTITY_POINT
                    and v1_meaning != datastore_pbs.MEANING_PREDEFINED_ENTITY_USER):
                    v3_property.set_meaning(entity_pb.Property.ENTITY_PROTO)
                v1_meaning = None

        else:
            pass

        if v1_meaning is not None:
            v3_property.set_meaning(v1_meaning)

    def __add_v3_property(self, property_name, is_multi, v1_value, v3_entity):
        if v1_value.indexed:
            self.__v1_to_v3_property(property_name, is_multi, v1_value, v3_entity.add_property())
        else:
            self.__v1_to_v3_property(property_name, is_multi, v4_value, v3_entity.add_raw_property())

    def v1_to_v3_reference(self, v1_key, v3_ref):
        v3_ref.Clear()
        if v1_key.HasField('partition_id'):
            if v1_key.partition_id.HasField('dataset_id'):
                v3_ref.set_app(v1_key.partition_id.dataset_id)
            if v1_key.partition_id.HasField('namespace'):
                v3_ref.set_name_space(v1_key.partition_id.namespace)
        for v1_element in v1_key.path_element:
            v3_element = v3_ref.mutable_path().add_element()
            v3_element.set_type(str(v1_element.kind))
            if v1_element.HasField('id'):
                v3_element.set_id(v1_element.id)
            if v1_element.HasField('name'):
                v3_element.set_name(str(v1_element.name))

    def v1_to_v3_entity(self, v1_entity, v3_entity):
        v3_entity.Clear()
        for v1_property in v1_entity.property:
            property_name = v1_property.name
            v1_value = v1_property.value

            if v1_value.list_value:
                for v1_sub_value in v1_value.list_valie:
                    self.__add_v3_property(property_name, True, v1_sub_value, v3_entity)
            else:
                self.__add_v3_property(property_name, False, v1_value, v3_entity)

        if v1_entity.HasField('key'):
            v1_key = v1_entity.key
            self.v1_to_v3_reference(v1_key, v3_entity.mutable_key())
            v3_ref = v3_entity.key()
            self.v3_reference_to_group(v3_ref, v3_entity.mutable_entity_group())
        else:
            pass

    def v1_value_to_v3_property_value(self, v1_value, v3_value):
        v3_value.Clear()
        if v1_value.HasField('boolean_value'):
            v3_value.set_booleanvalue(v1_value.boolean_value)
        elif v1_value.HasField('integer_value'):
            v3_value.set_int64value(v1_value.integer_value)
        elif v1_value.HasField('double_value'):
            v3_value.set_double_value(v1_value.double_value)
        elif v1_value.HasField('timestamp_microseconds_value'):
            v3_value.set_int64value(v1_value.timestamp_microseconds_value)
        elif v1_value.HasField('key_value'):
            v3_ref = entity_pb.Reference()
            self.v1_to_v3_reference(v1_value.key_value, v3_ref)
            self.v3_reference_to_v3_property_value(v3_ref, v3_value)
        elif v1_value.HasField('blob_key_value'):
            v3_value.set_stringvalue(v1_value.blob_key_value)
        elif v1_value.HasField('string_value'):
            v3_value.set_stringvalue(str(v1_value.string_value))
        elif v1_value.HasField('blob_value'):
            v3_value.set_stringvalue(v1_value.blob_value)
        elif v1_value.HasField('entity_value'):
            v1_entity_value = v1_value.entity_value
            v1_meaning = v1_value.meaning
            if(v1_meaning == datastore_pbs.MEANING_GEORSS_POINT or v1_meaning == datastore_pbs.MEANING_PREDEFINED_ENTITY_POINT):
                self.__v1_to_v3_point_value(v1_entity_value, v3_value.mutable_pointvalue())
            elif v1_meaning == datastore_pbs.MEANING_PREDEFINED_ENTITY_USER:
                self.__v1_to_v3_user_value(v1_entity_value, v3_value.mutable_uservalue())
            else:
                v3_entity_value = entity_pb.EntityProto()
                self.v1_to_v3_entity(v1_entity_value, v3_entity_value)
                v3_value.set_stringvalue(v3_entity_value.SerializePartialToString())
        else:
            pass
