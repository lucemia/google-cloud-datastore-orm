from google.appengine.datastore import datastore_pbs
from google.appengine.datastore import entity_pb

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


    def v3_to_v1_key(self, v3_ref, v1_key):
        v1_key.Clear()
        if not v3_ref.app():
            return
        v1_key.partition_id.dataset_id = v3_ref.app()
        if v3_ref.name_space():
            v1_key.partition_id.namespace = v3_ref.name_space()
        for v3_element in v3_ref.path().element_list():
            v1_element = v1_key.path_element.add()
            v1_element.kind = v3_element.type()
            if v3_element.has_id():
                v1_element.id = v3_element.id()
            if v3_element.has_name():
                v1_element.name = v3_element.name()


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

    def __is_v3_property_value_union_valid(self, v3_property_value):
        """Returns True if the v3 PropertyValue's union is valid."""
        num_sub_values = (v3_property_value.has_booleanvalue()
                      + v3_property_value.has_int64value()
                      + v3_property_value.has_doublevalue()
                      + v3_property_value.has_referencevalue()
                      + v3_property_value.has_stringvalue()
                      + v3_property_value.has_pointvalue()
                      + v3_property_value.has_uservalue())
        return num_sub_values <= 1

    def __is_v3_property_value_meaning_valid(self, v3_property_value, v3_meaning):
        """Returns True if the v3 PropertyValue's type value matches its meaning."""
        def ReturnTrue():
            return True
        def HasStringValue():
            return v3_property_value.has_stringvalue()
        def HasInt64Value():
            return v3_property_value.has_int64value()
        def HasPointValue():
            return v3_property_value.has_pointvalue()
        def ReturnFalse():
            return False
        value_checkers = {
            entity_pb.Property.NO_MEANING: ReturnTrue,
            entity_pb.Property.INDEX_VALUE: ReturnTrue,
            entity_pb.Property.BLOB: HasStringValue,
            entity_pb.Property.TEXT: HasStringValue,
            entity_pb.Property.BYTESTRING: HasStringValue,
            entity_pb.Property.ATOM_CATEGORY: HasStringValue,
            entity_pb.Property.ATOM_LINK: HasStringValue,
            entity_pb.Property.ATOM_TITLE: HasStringValue,
            entity_pb.Property.ATOM_CONTENT: HasStringValue,
            entity_pb.Property.ATOM_SUMMARY: HasStringValue,
            entity_pb.Property.ATOM_AUTHOR: HasStringValue,
            entity_pb.Property.GD_EMAIL: HasStringValue,
            entity_pb.Property.GD_IM: HasStringValue,
            entity_pb.Property.GD_PHONENUMBER: HasStringValue,
            entity_pb.Property.GD_POSTALADDRESS: HasStringValue,
            entity_pb.Property.BLOBKEY: HasStringValue,
            entity_pb.Property.ENTITY_PROTO: HasStringValue,
            entity_pb.Property.GD_WHEN: HasInt64Value,
            entity_pb.Property.GD_RATING: HasInt64Value,
            entity_pb.Property.GEORSS_POINT: HasPointValue,
            }
        default = ReturnFalse
        return value_checkers.get(v3_meaning, default)()

    def __v3_reference_has_id_or_name(self, v3_ref):
        """Determines if a v3 Reference specifies an ID or name.

        Args:
          v3_ref: an entity_pb.Reference

        Returns:
          boolean: True if the last path element specifies an ID or name.
        """
        path = v3_ref.path()
        assert path.element_size() >= 1
        last_element = path.element(path.element_size() - 1)
        return last_element.has_id() or last_element.has_name()

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


    def v3_property_to_v1_value(self, v3_property, indexed, v1_value):
        v1_value.Clear()
        # TODO
        v3_property_value = v3_property.value()
        v3_meaning = v3_property.meaning()
        v3_uri_meaning = None
        if v3_property.meaning_uri():
            v3_uri_meaning = v3_property.meaning_uri()

        if not self.__is_v3_property_value_union_valid(v3_property_value):
            v3_meaning = None
            v3_uri_meaning = None
        elif v3_meaning == entity_pb.Property.NO_MEANING:
            v3_meaning = None
        elif not self.__is_v3_property_value_meaning_valid(v3_property_value, v3_meaning):
            v3_meaning = None

        is_zlib_value = False
        if v3_uri_meaning:
            if v3_uri_meaning == datastore_pbs.URI_MEANING_ZLIB:
                if v3_property_value.has_stringvalue():
                    is_zlib_value = True
                    if v3_meaning != entity_pb.Property.BLOB:
                        v3_meaning = entity_pb.Property.BLOB
                else:
                    pass
            else:
                pass

        if v3_property_value.has_booleanvalue():
            v1_value.boolean_value = v3_property_value.booleanvalue()
        elif v3_property_value.has_int64value():
            if v3_meaning == entity_pb.Property.GD_WHEN:
                v1_value.timestamp_microseconds_value = v3_property_value.int64value()
                v3_meaning = None
            else:
                v1_value.integer_value = v3_property_value.int64value()
        elif v3_property_value.has_doublevalue():
            v1_value.double_value = v3_property_value.double_value()
        elif v3_property_value.has_referencevalue():
            v3_ref = entity_pb.Reference()
            self.__v3_reference_value_to_v3_reference(v3_property_value.referencevalue(), v3_ref)
            self.v3_to_v1_key(v3_ref, v1_value.key)
        elif v3_property_value.has_stringvalue():
            if v3_meaning == entity_pb.Property.ENTITY_PROTO:
                serialized_entity_v3 = v3_property_value.stringvalue()
                v3_entity = entity_pb.EntityProto()

                v3_entity.ParsePartialFromString(serialized_entity_v3)
                self.v3_to_v1_entity(v3_entity, v1_value.entity_value)
                v3_meaning = None
            elif (v3_meaning == entity_pb.Property.BLOB or v3_meaning == entity_pb.Property.BYTESTRING):
                v1_value.blob_value = v3_property_value.stringvalue()

                if indexed or v3_meaning == entity_pb.Property.BLOB:
                    v3_meaning = None
            else:
                string_value = v3_property_value.stringvalue()
                if datastore_pbs.is_valid_utf8(string_value):
                    if v3_meaning == entity_pb.Property.BLOBKEY:
                        v1_value.blob_key_value = string_value
                        v3_meaning= None
                    else:
                        v1_value.string_value = string_value
                else:
                    v1_value.blob_value = string_value

                    if v3_meaning != entity_pb.Property.INDEX_VALUE:
                        v3_meaning = None
        elif v3_property_value.has_pointvalue():
            self.__v3_to_v1_point_entity(v3_property_value.pointvalue(), v1_value.entity_value)
            if v3_meaning != entity_pb.Property.GEORESS_POINT:
                v1_value.meaning = datastore_pbs.MEANING_PREDEFINED_ENTITY_POINT
                v3_meaning = None
        elif v3_property_value.has_uservalue():
            self.__v3_to_v1_user_entity(v3_property_value.uservalue(), v1_value.entity_value)
            v1_value.meaning = datastore_pbs.MEANING_PREDEFINED_ENTITY_USER
        else:
            pass

        if is_zlib_value:
            v1_value.meaning = datastore_pbs.MEANING_ZLIB
        elif v3_meaning:
            v1_value.meaning = v3_meaning

        if indexed != v1_value.indexed:
            v1_value.indexed = indexed

    def __add_v1_property_to_entity(self, v1_entity, property_map, v3_property, indexed):
        property_name = v3_property.name()
        if property_name.startswith("__"):
            return
        if property_name in property_map:
            v1_property = property_map[property_name]
        else:
            v1_property = v1_entity.property.add()
            v1_property.name = property_name
            property_map[property_name] = v1_property
        if v3_property.multiple():
            self.v3_property_to_v1_value(v3_property, indexed, v1_property.value.list_value.add())
        else:
            self.v3_property_to_v1_value(v3_property, indexed, v1_property.value)


    def v3_to_v1_entity(self, v3_entity, v1_entity):
        v1_entity.Clear()
        self.v3_to_v1_key(v3_entity.key(), v1_entity.key)
        if not v3_entity.key().has_app():
            v1_entity.key.Clear()

        v1_properties = {}
        for v3_property in v3_entity.property_list():
            self.__add_v1_property_to_entity(v1_entity, v1_properties, v3_property, True)

        for v3_property in v3_entity.raw_property_list():
            self.__add_v1_property_to_entity(v1_entity, v1_properties, v3_property, False)


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
