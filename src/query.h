#ifndef __CASS_DRIVER_QUERY_H__
#define __CASS_DRIVER_QUERY_H__

#include "node.h"
#include "nan.h"
#include "buffer-pool.h"
#include "wrapped-method.h"
#include <vector>

using namespace v8;

class Client;

// Wrapper for an in-progress query to the back end
class Query: public node::ObjectWrap {
public:
    // Initialize the class constructor.
    static void Init();

    // Create a new instance of the class.
    static v8::Local<v8::Object> NewInstance();

    // Stash the reference to the parent client object and extract the pointer
    // to the session.
    void set_client(v8::Local<v8::Object>& client);

private:
    u_int32_t id_;

    Query();
    ~Query();

    // The actual implementation of the constructor
    static NAN_METHOD(New);

    // Bind the query and parameters
    WRAPPED_METHOD_DECL(Bind);

    // Execute the query, potentially retrieving additional pages.
    WRAPPED_METHOD_DECL(Execute);

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

    CassSession* session_;
    CassStatement* statement_;

    bool fetching_;
    NanCallback* callback_;

    ColumnInfo column_info_;
    const CassResult* result_;
    CassError result_code_;
    std::string result_error_;

    uv_async_t* async_;

    BufferPool buffer_pool_;

    static v8::Persistent<v8::Function> constructor;
};

#endif
