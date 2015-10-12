
#include "type-mapper.h"
using namespace v8;

static const u_int32_t BIGINT_AS_OBJECT = 0x1 << 16;

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
TypeMapper::bind_statement_params(CassStatement* statement, Local<Array> params, Local<Object> param_types)
{
    for (u_int32_t i = 0; i < params->Length(); ++i) {
        const Local<Value> arg = Nan::Get(params, i).ToLocalChecked();
        CassValueType type = CASS_VALUE_TYPE_UNKNOWN;
        u_int32_t encoding = 0;
        if (!param_types.IsEmpty()) {
            u_int32_t code = Nan::Get(param_types, i).ToLocalChecked()->Int32Value();
            type = cass_type_from_code(code);
            encoding = encoding_from_code(code);
        }

        if (! TypeMapper::bind_statement_param(statement, i, arg, type, encoding)) {
            return i;
        }
    }

    return -1;
}

bool
TypeMapper::bind_statement_param(CassStatement* statement,
                                 u_int32_t i,
                                 const Local<Value>& value,
                                 CassValueType given_type,
                                 u_int32_t encoding)
{
    if (value->IsNull()) {
        cass_statement_bind_null(statement, i);
        return true;
    }

    CassValueType type = given_type == CASS_VALUE_TYPE_UNKNOWN ? infer_type(value) : given_type;
    switch(type) {
    case CASS_VALUE_TYPE_BLOB: {
        Local<Object> obj = value->ToObject();
        cass_statement_bind_bytes(statement, i,
            (cass_byte_t*) node::Buffer::Data(obj),
            node::Buffer::Length(obj));
        return true;
    }
    case CASS_VALUE_TYPE_DOUBLE: {
        cass_double_t doubleValue = value->ToNumber()->NumberValue();
        cass_statement_bind_double(statement, i, doubleValue);
        return true;
    }
    case CASS_VALUE_TYPE_FLOAT: {
        cass_float_t floatValue = value->ToNumber()->NumberValue();
        cass_statement_bind_float(statement, i, floatValue);
        return true;
    }
    case CASS_VALUE_TYPE_LIST: {
        Local<Value> val = value;
        Local<Array> array = val.As<Array>();
        size_t length = array->Length();
        CassCollection* list = cass_collection_new(CASS_COLLECTION_TYPE_LIST, array->Length());
        for (size_t j = 0; j < length; ++j) {
            Local<Value> val = Nan::Get(array, j).ToLocalChecked();
            append_collection(list, val);
        }
        cass_statement_bind_collection(statement, i, list);
        cass_collection_free(list);
        return true;
    }
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_VARCHAR: {
        String::Utf8Value str(value->ToString());
        cass_statement_bind_string_n(statement, i, *str, str.length());
        return true;
    }
    case CASS_VALUE_TYPE_INT: {
        cass_int32_t intValue = value->ToNumber()->Int32Value();
        cass_statement_bind_int32(statement, i, intValue);
        return true;
    }
    case CASS_VALUE_TYPE_COUNTER:
    case CASS_VALUE_TYPE_TIMESTAMP:
    case CASS_VALUE_TYPE_BIGINT: {
        cass_int64_t intValue;
        if (encoding == BIGINT_AS_OBJECT) {
            // Bigints passed in as {'low': <lowInt>, 'high': <highInt>}
            Local<Object> obj = value->ToObject();
            Local<String> lowKey = Nan::New<String>("low").ToLocalChecked();
            Local<String> highKey = Nan::New<String>("high").ToLocalChecked();
            int lowVal = obj->Get(lowKey)->ToNumber()->NumberValue();
            int highVal = obj->Get(highKey)->ToNumber()->NumberValue();

            intValue = ((long)highVal) << 32 | lowVal;
        } else {
            intValue = value->ToNumber()->IntegerValue();
        }
        cass_statement_bind_int64(statement, i, intValue);
        return true;
    }
    case CASS_VALUE_TYPE_BOOLEAN: {
        cass_bool_t booleanValue = (Nan::To<bool>(value).FromJust() ? cass_true : cass_false);
        cass_statement_bind_bool(statement, i, booleanValue);
        return true;
    }
    case CASS_VALUE_TYPE_MAP: {
        Local<Object> obj = value->ToObject();
        Local<Array> keys = Nan::GetOwnPropertyNames(obj).ToLocalChecked();
        CassCollection* cassObj = cass_collection_new(CASS_COLLECTION_TYPE_MAP, keys->Length());
        for (size_t j = 0; j < keys->Length(); j++) {
            Local<Value> key = Nan::Get(keys, j).ToLocalChecked();
            Local<Value> val = Nan::Get(obj, key).ToLocalChecked();
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
    case CASS_VALUE_TYPE_DECIMAL:
    case CASS_VALUE_TYPE_UUID:
    case CASS_VALUE_TYPE_VARINT:
    case CASS_VALUE_TYPE_TIMEUUID:
    case CASS_VALUE_TYPE_INET:
    case CASS_VALUE_TYPE_SET:
    default:
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
        cass_collection_append_bytes(collection,
            (cass_byte_t*)node::Buffer::Data(obj),
            node::Buffer::Length(obj));
        return true;
    }
    case CASS_VALUE_TYPE_INT: {
        cass_int32_t intValue = value->ToNumber()->Int32Value();
        cass_collection_append_int32(collection, intValue);
        return true;
    }
    case CASS_VALUE_TYPE_COUNTER:
    case CASS_VALUE_TYPE_TIMESTAMP: {
        cass_int64_t intValue = value->ToNumber()->IntegerValue();
        cass_collection_append_int64(collection, intValue);
        return true;
    }
    case CASS_VALUE_TYPE_DOUBLE: {
        cass_double_t doubleValue = value->ToNumber()->NumberValue();
        cass_collection_append_double(collection, doubleValue);
        return true;
    }
    case CASS_VALUE_TYPE_FLOAT: {
        cass_float_t floatValue = value->ToNumber()->NumberValue();
        cass_collection_append_float(collection, floatValue);
        return true;
    }
    case CASS_VALUE_TYPE_BOOLEAN: {
        cass_bool_t booleanValue = (Nan::To<bool>(value).FromJust() ? cass_true : cass_false);
        cass_collection_append_bool(collection, booleanValue);
        return true;
    }
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_VARCHAR: {
        String::Utf8Value str(value->ToString());
        cass_collection_append_string_n(collection, *str, str.length());
        return true;
    }
    case CASS_VALUE_TYPE_UNKNOWN:
    case CASS_VALUE_TYPE_CUSTOM:
    case CASS_VALUE_TYPE_BIGINT:
    case CASS_VALUE_TYPE_DECIMAL:
    case CASS_VALUE_TYPE_VARINT:
    case CASS_VALUE_TYPE_INET:
    case CASS_VALUE_TYPE_LIST:
    case CASS_VALUE_TYPE_MAP:
    case CASS_VALUE_TYPE_SET:
    case CASS_VALUE_TYPE_TIMEUUID:
    case CASS_VALUE_TYPE_UUID:
    default:
        return false;
    }
}

bool
TypeMapper::v8_from_cassandra(v8::Local<v8::Value>* result,
                              u_int32_t code,
                              const CassValue* value)
{
    CassValueType type = cass_type_from_code(code);
    u_int32_t encoding = encoding_from_code(code);

    if (value == NULL || cass_value_is_null(value)) {
        *result = Nan::Null();
        return true;
    }

    switch(type) {
    case CASS_VALUE_TYPE_BLOB: {
        const cass_byte_t* data;
        size_t size;
        if (cass_value_get_bytes(value, &data, &size) != CASS_OK) {
            return false;
        }
        *result = Nan::CopyBuffer((char*)data, size).ToLocalChecked();
        return true;
    }
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_VARCHAR: {
        const char* data;
        size_t size;
        if (cass_value_get_string(value, &data, &size) != CASS_OK) {
            return false;
        }
        *result = Nan::New<String>(data, size).ToLocalChecked();
        return true;
    }
    case CASS_VALUE_TYPE_INT: {
        cass_int32_t intValue;
        if (cass_value_get_int32(value, &intValue) != CASS_OK) {
            return false;
        }
        *result = Nan::New<Number>(intValue);
        return true;
    }
    case CASS_VALUE_TYPE_COUNTER:
    case CASS_VALUE_TYPE_TIMESTAMP:
    case CASS_VALUE_TYPE_BIGINT: {
        // Get the bigint value
        cass_int64_t intValue;
        if (cass_value_get_int64(value, &intValue) != CASS_OK) {
            return false;
        }
        if (encoding == BIGINT_AS_OBJECT) {
            // Because Node's native Number object only goes up to 53bit, unpack
            // the 64bit value into two 32bit values who can be passed up to
            // node separately
            int low = (int) intValue;
            int high = (int) (intValue >> 32);

            // We pass up the following object:
            //     {
            //         "low": <lowValue>,
            //         "high": <highValue>
            //     }
            Local<String> lowKey = Nan::New<String>("low").ToLocalChecked();
            Local<String> highKey = Nan::New<String>("high").ToLocalChecked();
            Local<Number> lowVal = Nan::New<Number>((double)low);
            Local<Number> highVal = Nan::New<Number>((double)high);
            Local<Object> obj = Nan::New<Object>();
            obj->Set(lowKey, lowVal);
            obj->Set(highKey, highVal);
            *result = obj;
        } else {
            *result = Nan::New<Number>((double)intValue);
        }
        return true;
    }
    case CASS_VALUE_TYPE_DOUBLE: {
        cass_double_t doubleValue;
        if (cass_value_get_double(value, &doubleValue) != CASS_OK) {
            return false;
        }
        *result = Nan::New<Number>(doubleValue);
        return true;
    }
    case CASS_VALUE_TYPE_FLOAT: {
        cass_float_t floatValue;
        if (cass_value_get_float(value, &floatValue) != CASS_OK) {
            return false;
        }
        *result = Nan::New<Number>(floatValue);
        return true;
    }
    case CASS_VALUE_TYPE_BOOLEAN: {
        cass_bool_t booleanValue;
        if (cass_value_get_bool(value, &booleanValue) != CASS_OK) {
            return false;
        }
        *result = booleanValue ? Nan::True() : Nan::False();
        return true;
    }
    case CASS_VALUE_TYPE_MAP: {
        Local<Object> obj = Nan::New<Object>();
        CassIterator* iterator = cass_iterator_from_map(value);
        CassValueType keyType = cass_value_primary_sub_type(value);
        CassValueType valueType = cass_value_secondary_sub_type(value);
        while (cass_iterator_next(iterator)) {
            const CassValue* key = cass_iterator_get_map_key(iterator);
            const CassValue* val = cass_iterator_get_map_value(iterator);
            Local<Value> a, b;
            if (!v8_from_cassandra(&a, keyType, key) || !v8_from_cassandra(&b, valueType, val)) {
                return false;
            }
            Nan::Set(obj, a, b);
        }
        cass_iterator_free(iterator);
        *result = obj;
        return true;
    }
    case CASS_VALUE_TYPE_UNKNOWN:
    case CASS_VALUE_TYPE_CUSTOM:
    case CASS_VALUE_TYPE_DECIMAL:
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
