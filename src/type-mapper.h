#ifndef _CASSANDRA_NATIVE_TYPE_MAPPER_H_
#define _CASSANDRA_NATIVE_TYPE_MAPPER_H_

#include <nan.h>
#include "cassandra.h"

class TypeMapper {
public:
    // Infer the cassandra type based on the javascript value
    static CassValueType infer_type(const v8::Local<v8::Value>& value);

    // Bind each element of params to statement, using hints if given to infer types
    // if one of the bindings fails, return the index of that element
    // or -1 if successful
    static int bind_statement_params(CassStatement* statement, v8::Local<v8::Array> params, v8::Local<v8::Object> hints);

    // Set the given Javascript value as the i'th parameter to the statement
    static bool bind_statement_param(CassStatement* statement, u_int32_t i,
                                     const v8::Local<v8::Value>& value,
                                     CassValueType given_type);

    // Append the Javascript value to the given collection
    static bool append_collection(CassCollection* collection,
                                  const v8::Local<v8::Value>& value);

    // Get a Javascript result value for the given CassValue of the given type.
    // Will use inference if type is CASS_VALUE_UNKNOWN.
    static bool v8_from_cassandra(v8::Local<v8::Value>* result, CassValueType type,
                             const CassValue* value);
};

#endif
