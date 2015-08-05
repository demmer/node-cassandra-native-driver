#ifndef __CASS_DRIVER_RESULT_H__
#define __CASS_DRIVER_RESULT_H__

#include "cassandra.h"
#include "nan.h"
#include <vector>

using namespace v8;

// Helper object to process the result of a query or a batch query.
class Result {
public:
    Result();
    ~Result();

    const CassResult* result() { return result_; }

    void do_callback(CassFuture* future, Nan::Callback* callback);
private:
    // Encapsulation of column metadata that can be cached for each row in the
    // results.
    struct Column {
        Column(const char* name, size_t name_length, CassValueType type) {
            name_.Reset(Nan::New<v8::String>(name, name_length).ToLocalChecked());
            type_ = type;
        };

        Nan::Persistent<v8::String> name_;
        CassValueType type_;
    };
    typedef std::vector<Column*> ColumnInfo;

    ColumnInfo column_info_;
    const CassResult* result_;
};

#endif
