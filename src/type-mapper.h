#ifndef _CASSANDRA_NATIVE_TYPE_MAPPER_H_
#define _CASSANDRA_NATIVE_TYPE_MAPPER_H_

#include <nan.h>
#include "cassandra.h"
#include "buffer-pool.h"

class TypeMapper {
public:
    // Infer the cassandra type based on the javascript value
    static CassValueType infer_type(const v8::Local<v8::Value>& value);

    // Set the given Javascript value as the i'th parameter to the statement
    static bool bind_statement_param(CassStatement* statement, u_int32_t i,
                                     v8::Local<v8::Value>& value);

    // Append the Javascript value to the given collection
    static bool append_collection(CassCollection* collection,
                                  v8::Local<v8::Value>& value);

    // Get a Javascript result value for the i'th column from the specified row.
    static bool column_value(v8::Local<v8::Value>* result, CassValueType type,
                             const CassRow* row, size_t i, BufferPool* pool);
};

#endif
