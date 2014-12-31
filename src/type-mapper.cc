
#include "type-mapper.h"
using namespace v8;

CassValueType
TypeMapper::infer_type(const Local<Value>& value)
{
    if (node::Buffer::HasInstance(value)) {
        return CASS_VALUE_TYPE_BLOB;
    }
    else if (value->IsArray()) {
        return CASS_VALUE_TYPE_LIST;
    }
    else {
        return CASS_VALUE_TYPE_UNKNOWN;
    }
}

bool
TypeMapper::bind_statement_param(CassStatement* statement, u_int32_t i, Local<Value>& value)
{
    CassValueType type = infer_type(value);
    switch(type) {
    case CASS_VALUE_TYPE_BLOB: {
        Local<Object> obj = value.As<Object>();
        CassBytes data;
        data.data = (cass_byte_t*) node::Buffer::Data(obj);
        data.size = node::Buffer::Length(obj);
        cass_statement_bind_bytes(statement, i, data);
        return true;
    }
    case CASS_VALUE_TYPE_LIST: {
        Local<Array> array = value.As<Array>();
        size_t length = array->Length();
        CassCollection* list = cass_collection_new(CASS_COLLECTION_TYPE_LIST, array->Length());
        for (size_t i = 0; i < length; ++i) {
            Local<Value> value = array->Get(i);
            append_collection(list, value);
        }
        cass_statement_bind_collection(statement, i, list);
        return true;
    }

    case CASS_VALUE_TYPE_UNKNOWN:
    case CASS_VALUE_TYPE_CUSTOM:
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_BIGINT:
    case CASS_VALUE_TYPE_BOOLEAN:
    case CASS_VALUE_TYPE_COUNTER:
    case CASS_VALUE_TYPE_DECIMAL:
    case CASS_VALUE_TYPE_DOUBLE:
    case CASS_VALUE_TYPE_FLOAT:
    case CASS_VALUE_TYPE_INT:
    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_TIMESTAMP:
    case CASS_VALUE_TYPE_UUID:
    case CASS_VALUE_TYPE_VARCHAR:
    case CASS_VALUE_TYPE_VARINT:
    case CASS_VALUE_TYPE_TIMEUUID:
    case CASS_VALUE_TYPE_INET:
    case CASS_VALUE_TYPE_MAP:
    case CASS_VALUE_TYPE_SET:
        return false;
    }
}

// Append the Javascript value to the given collection
bool
TypeMapper::append_collection(CassCollection* collection, Local<Value>& value)
{
    CassValueType type = infer_type(value);
    switch(type) {
    case CASS_VALUE_TYPE_BLOB: {
        Local<Object> obj = value.As<Object>();
        CassBytes data;
        data.data = (cass_byte_t*) node::Buffer::Data(obj);
        data.size = node::Buffer::Length(obj);
        cass_collection_append_bytes(collection, data);
        return true;
    }
    case CASS_VALUE_TYPE_UNKNOWN:
    case CASS_VALUE_TYPE_CUSTOM:
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_BIGINT:
    case CASS_VALUE_TYPE_BOOLEAN:
    case CASS_VALUE_TYPE_COUNTER:
    case CASS_VALUE_TYPE_DECIMAL:
    case CASS_VALUE_TYPE_DOUBLE:
    case CASS_VALUE_TYPE_FLOAT:
    case CASS_VALUE_TYPE_INT:
    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_TIMESTAMP:
    case CASS_VALUE_TYPE_UUID:
    case CASS_VALUE_TYPE_VARCHAR:
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
TypeMapper::column_value(v8::Local<v8::Value>* value, CassValueType type,
                         const CassRow* row, size_t i, BufferPool* pool)
{
    switch(type) {
    case CASS_VALUE_TYPE_BLOB: {
        CassBytes blob;
        cass_value_get_bytes(cass_row_get_column(row, i), &blob);
        *value = pool->allocate(blob.data, blob.size);
        return true;
    }
    case CASS_VALUE_TYPE_UNKNOWN:
    case CASS_VALUE_TYPE_CUSTOM:
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_BIGINT:
    case CASS_VALUE_TYPE_BOOLEAN:
    case CASS_VALUE_TYPE_COUNTER:
    case CASS_VALUE_TYPE_DECIMAL:
    case CASS_VALUE_TYPE_DOUBLE:
    case CASS_VALUE_TYPE_FLOAT:
    case CASS_VALUE_TYPE_INT:
    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_TIMESTAMP:
    case CASS_VALUE_TYPE_UUID:
    case CASS_VALUE_TYPE_VARCHAR:
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
