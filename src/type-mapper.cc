
#include "type-mapper.h"
using namespace v8;

CassValueType
TypeMapper::infer_type(const Local<Value>& value)
{
    if (node::Buffer::HasInstance(value)) {
        return CASS_VALUE_TYPE_BLOB;
    }
    else if (value->IsInt32()) {
        return CASS_VALUE_TYPE_INT;
    }
    else if (value -> IsNumber()) {
        return CASS_VALUE_TYPE_DOUBLE;
    }
    else if (value->IsArray()) {
        return CASS_VALUE_TYPE_LIST;
    }
    else if (value->IsString()) {
        return CASS_VALUE_TYPE_VARCHAR;
    }
    else if (value -> IsBoolean()) {
        return CASS_VALUE_TYPE_BOOLEAN;
    }
    else if (value -> IsObject()) {
        return CASS_VALUE_TYPE_MAP;
    }
    else {
        return CASS_VALUE_TYPE_UNKNOWN;
    }
}

int
TypeMapper::bind_statement_params(CassStatement* statement, Local<Array> params, Local<Array> hints)
{
    for (u_int32_t i = 0; i < params->Length(); ++i) {
        const Local<Value>& arg = params->Get(i);
        CassValueType type = hints.IsEmpty()
        ? CASS_VALUE_TYPE_UNKNOWN
        : (CassValueType) hints->Get(i).As<Number>()->Int32Value();

        if (! TypeMapper::bind_statement_param(statement, i, arg, type)) {
            return i;
        }
    }

    return -1;
}


bool
TypeMapper::bind_statement_param(CassStatement* statement, u_int32_t i,
                                const Local<Value>& value, CassValueType given_type)
{
    CassValueType type = given_type == CASS_VALUE_TYPE_UNKNOWN ? infer_type(value) : given_type;
    switch(type) {
    case CASS_VALUE_TYPE_BLOB: {
        Local<Object> obj = value->ToObject();
        CassBytes data;
        data.data = (cass_byte_t*) node::Buffer::Data(obj);
        data.size = node::Buffer::Length(obj);
        cass_statement_bind_bytes(statement, i, data);
        return true;
    }
    case CASS_VALUE_TYPE_DOUBLE: {
        cass_double_t doubleValue = value->ToNumber()->NumberValue();
        cass_statement_bind_double(statement, i, doubleValue);
        return true;
    }
    case CASS_VALUE_TYPE_LIST: {
        Local<Value> val = value;
        Local<Array> array = val.As<Array>();
        size_t length = array->Length();
        CassCollection* list = cass_collection_new(CASS_COLLECTION_TYPE_LIST, array->Length());
        for (size_t i = 0; i < length; ++i) {
            Local<Value> val = array->Get(i);
            append_collection(list, val);
        }
        cass_statement_bind_collection(statement, i, list);
        cass_collection_free(list);
        return true;
    }
    case CASS_VALUE_TYPE_VARCHAR: {
        String::AsciiValue ascii_str(value->ToString());
        CassString str = cass_string_init(*ascii_str);
        cass_statement_bind_string(statement, i, str);
        return true;
    }
    case CASS_VALUE_TYPE_INT: {
        cass_int32_t intValue = value->ToNumber()->Int32Value();
        cass_statement_bind_int32(statement, i, intValue);
        return true;
    }
    case CASS_VALUE_TYPE_BOOLEAN: {
        cass_bool_t booleanValue = (value->BooleanValue() ? cass_true : cass_false);
        cass_statement_bind_bool(statement, i, booleanValue);
        return true;
    }
    case CASS_VALUE_TYPE_MAP: {
        Local<Object> obj = value->ToObject();
        Local<Array> keys = obj->GetOwnPropertyNames();
        CassCollection* cassObj = cass_collection_new(CASS_COLLECTION_TYPE_MAP, keys->Length());
        for (size_t i = 0; i < keys->Length(); i++) {
            Local<Value> key = keys->Get(i);
            Local<Value> val = obj->Get(key);
            if (!append_collection(cassObj, key) || !append_collection(cassObj, val)) {
                return false;
            }
        }
        cass_statement_bind_collection(statement, i, cassObj);
        cass_collection_free(cassObj);
        return true;
    }
    case CASS_VALUE_TYPE_UNKNOWN:
    case CASS_VALUE_TYPE_CUSTOM:
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_BIGINT:
    case CASS_VALUE_TYPE_COUNTER:
    case CASS_VALUE_TYPE_DECIMAL:
    case CASS_VALUE_TYPE_FLOAT:
    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_TIMESTAMP:
    case CASS_VALUE_TYPE_UUID:
    case CASS_VALUE_TYPE_VARINT:
    case CASS_VALUE_TYPE_TIMEUUID:
    case CASS_VALUE_TYPE_INET:
    case CASS_VALUE_TYPE_SET:
        return false;
    }
}

// Append the Javascript value to the given collection
bool
TypeMapper::append_collection(CassCollection* collection, const Local<Value>& value)
{
    CassValueType type = infer_type(value);
    switch(type) {
    case CASS_VALUE_TYPE_BLOB: {
        Local<Object> obj = value->ToObject();
        CassBytes data;
        data.data = (cass_byte_t*) node::Buffer::Data(obj);
        data.size = node::Buffer::Length(obj);
        cass_collection_append_bytes(collection, data);
        return true;
    }
    case CASS_VALUE_TYPE_INT: {
        cass_int32_t intValue = value->ToNumber()->Int32Value();
        cass_collection_append_int32(collection, intValue);
        return true;
    }
    case CASS_VALUE_TYPE_DOUBLE: {
        cass_double_t doubleValue = value->ToNumber()->NumberValue();
        cass_collection_append_double(collection, doubleValue);
        return true;
    }
    case CASS_VALUE_TYPE_BOOLEAN: {
        cass_bool_t booleanValue = (value->BooleanValue() ? cass_true : cass_false);
        cass_collection_append_bool(collection, booleanValue);
        return true;
    }
    case CASS_VALUE_TYPE_VARCHAR: {
        String::AsciiValue ascii_str(value->ToString());
        CassString str = cass_string_init(*ascii_str);
        cass_collection_append_string(collection, str);
        return true;
    }
    case CASS_VALUE_TYPE_UNKNOWN:
    case CASS_VALUE_TYPE_CUSTOM:
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_BIGINT:
    case CASS_VALUE_TYPE_COUNTER:
    case CASS_VALUE_TYPE_DECIMAL:
    case CASS_VALUE_TYPE_FLOAT:
    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_TIMESTAMP:
    case CASS_VALUE_TYPE_UUID:
    case CASS_VALUE_TYPE_VARINT:
    case CASS_VALUE_TYPE_TIMEUUID:
    case CASS_VALUE_TYPE_INET:
    case CASS_VALUE_TYPE_LIST:
    case CASS_VALUE_TYPE_MAP:
    case CASS_VALUE_TYPE_SET:
    default:
        return false;
    }
}

bool
TypeMapper::v8_from_cassandra(v8::Local<v8::Value>* result, CassValueType type,
                         const CassValue* value, BufferPool* pool)
{
    switch(type) {
    case CASS_VALUE_TYPE_BLOB: {
        CassBytes blob;
        if (cass_value_get_bytes(value, &blob) != CASS_OK) {
            return false;
        }
        *result = pool->allocate(blob.data, blob.size);
        return true;
    }
    case CASS_VALUE_TYPE_VARCHAR: {
        CassString str;
        if (cass_value_get_string(value, &str) != CASS_OK) {
            return false;
        }
        *result = NanNew<String>(str.data, str.length);
        return true;
    }
    case CASS_VALUE_TYPE_INT: {
        cass_int32_t intValue;
        if (cass_value_get_int32(value, &intValue) != CASS_OK) {
            return false;
        }
        *result = NanNew<Number>(intValue);
        return true;
    }
    case CASS_VALUE_TYPE_DOUBLE: {
        cass_double_t doubleValue;
        if (cass_value_get_double(value, &doubleValue) != CASS_OK) {
            return false;
        }
        *result = NanNew<Number>(doubleValue);
        return true;
    }
    case CASS_VALUE_TYPE_BOOLEAN: {
        cass_bool_t booleanValue;
        if (cass_value_get_bool(value, &booleanValue) != CASS_OK) {
            return false;
        }
        *result = booleanValue ? NanTrue() : NanFalse();
        return true;
    }
    case CASS_VALUE_TYPE_MAP: {
        Local<Object> obj = NanNew<Object>();
        CassIterator* iterator = cass_iterator_from_map(value);
        CassValueType keyType = cass_value_primary_sub_type(value);
        CassValueType valueType = cass_value_secondary_sub_type(value);
        while (cass_iterator_next(iterator)) {
            const CassValue* key = cass_iterator_get_map_key(iterator);
            const CassValue* val = cass_iterator_get_map_value(iterator);
            Local<Value> a, b;
            if (!v8_from_cassandra(&a, keyType, key, pool) || !v8_from_cassandra(&b, valueType, val, pool)) {
                return false;
            }
            obj->Set(a, b);
        }
        cass_iterator_free(iterator);
        *result = obj;
        return true;
    }
    case CASS_VALUE_TYPE_UNKNOWN:
    case CASS_VALUE_TYPE_CUSTOM:
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_BIGINT:
    case CASS_VALUE_TYPE_COUNTER:
    case CASS_VALUE_TYPE_DECIMAL:
    case CASS_VALUE_TYPE_FLOAT:
    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_TIMESTAMP:
    case CASS_VALUE_TYPE_UUID:
    case CASS_VALUE_TYPE_VARINT:
    case CASS_VALUE_TYPE_TIMEUUID:
    case CASS_VALUE_TYPE_INET:
    case CASS_VALUE_TYPE_LIST:
    case CASS_VALUE_TYPE_SET:
    default:
        return false;
    }
}
