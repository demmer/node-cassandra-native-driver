#ifndef _CASSANDRA_NATIVE_TYPE_MAPPER_H_
#define _CASSANDRA_NATIVE_TYPE_MAPPER_H_

#include <nan.h>
#include "cassandra.h"

class TypeMapper {
public:
    // Given a type code, extract the cassandra type from the low bits
    static inline CassValueType cass_type_from_code(u_int32_t code) {
        return (CassValueType) (code & 0xffff);
    }

    static inline u_int32_t encoding_from_code(u_int32_t code) {
        return code & ~0xffff;
    }

    // Infer the cassandra type based on the javascript value
    static CassValueType infer_type(const v8::Local<v8::Value>& value);

    // Bind each element of params to statement, using either type inference or
    // the explicit Type codes array to determine how to encode the type.
    //
    // If one of the bindings fails, return the index of that element or -1 if
    // successful;
    static int bind_statement_params(CassStatement* statement,
                                     v8::Local<v8::Array> params,
                                     v8::Local<v8::Object> param_types); // XXX array?

    // Set the given Javascript value as the i'th parameter to the statement
    static bool bind_statement_param(CassStatement* statement, u_int32_t i,
                                     const v8::Local<v8::Value>& value,
                                     CassValueType type,
                                     u_int32_t encoding);

    // Append the Javascript value to the given collection
    static bool append_collection(CassCollection* collection,
                                  const v8::Local<v8::Value>& value);

    // Get a Javascript result value for the given CassValue of the given type.
    // Will use inference if type is CASS_VALUE_UNKNOWN.
    static bool v8_from_cassandra(v8::Local<v8::Value>* result,
                                  u_int32_t code,
                                  const CassValue* value);
};

#endif
