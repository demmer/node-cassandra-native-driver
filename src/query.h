#ifndef __CASS_DRIVER_QUERY_H__
#define __CASS_DRIVER_QUERY_H__

#include "node.h"
#include "nan.h"
#include "buffer-pool.h"
#include <vector>

using namespace v8;

class Client;

// Wrapper for an in-progress query to the back end
class Query: public node::ObjectWrap {
public:
    static void Init();
    static v8::Local<v8::Object> NewInstance();

    // XXX this should just be part of the constructor but for now it may need
    // to return an error if the type mapping fails.
    bool bind(v8::Persistent<v8::Object>& client, v8::Local<v8::String>& query,
              v8::Local<v8::Array>& params);
    void fetch(CassSession* session, v8::Local<v8::Object>& options,
               NanCallback* callback);

private:
    Query();
    ~Query();

    static NAN_METHOD(New);

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

    static void on_result_ready(CassFuture* future, void* data);
    void result_ready(CassFuture* future);

    static void on_async_ready(uv_async_t* handle, int status);
    void async_ready();

    v8::Persistent<Object> client_;
    CassStatement* statement_;

    bool fetching_;
    NanCallback* callback_;

    ColumnInfo column_info_;
    const CassResult* result_;
    CassError result_code_;
    std::string result_error_;

    uv_async_t async_;

    BufferPool buffer_pool_;

    static v8::Persistent<v8::Function> constructor;
};

#endif
