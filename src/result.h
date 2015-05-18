#ifndef __CASS_DRIVER_RESULT_H__
#define __CASS_DRIVER_RESULT_H__

#include "cassandra.h"
#include "nan.h"
#include "buffer-pool.h"
#include <vector>

using namespace v8;

// Helper object to process the result of a query or a batch query.
class Result {
public:
    Result();
    ~Result();

    const CassResult* result() { return result_; }

    void do_callback(CassFuture* future, NanCallback* callback);
private:
    // Encapsulation of column metadata that can be cached for each row in the
    // results.
    struct Column {
        Column(const char* name, size_t name_length, CassValueType type) {
            NanAssignPersistent(name_, NanNew<v8::String>(name, name_length));
            type_ = type;
        };

        v8::Persistent<v8::String> name_;
        CassValueType type_;
    };
    typedef std::vector<Column> ColumnInfo;

    ColumnInfo column_info_;
    const CassResult* result_;

    BufferPool buffer_pool_;
};

#endif
