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

    // XXX/demmer fix this
    uv_async_t* async_;

    const CassResult* result() { return result_; }

    void do_callback(NanCallback* callback);

    static void on_ready(CassFuture* future, void* data);
private:
    // Encapsulation of column metadata that can be cached for each row in the
    // results.
    struct Column {
        Column(CassString name, CassValueType type) {
            NanAssignPersistent(name_, NanNew(name.data));
            type_ = type;
        };

        v8::Persistent<v8::String> name_;
        CassValueType type_;
    };
    typedef std::vector<Column> ColumnInfo;

    _NAN_METHOD_RETURN_TYPE bind(Local<Array>& params);

    void ready(CassFuture* future);

    static void on_async_ready(uv_async_t* handle, int status);
    void async_ready();

    CassSession* session_;
    CassStatement* statement_;
    bool prepared_;

    bool fetching_;
    NanCallback* callback_;

    ColumnInfo column_info_;
    const CassResult* result_;
    CassError result_code_;
    std::string result_error_;

    BufferPool buffer_pool_;

    static v8::Persistent<v8::Function> constructor;
};

#endif
