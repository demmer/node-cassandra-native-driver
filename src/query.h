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
    void set_client(const v8::Local<v8::Object>& client);

    // Set a reference to a prepared statement
    void set_prepared_statement(CassStatement* statement);
private:
    u_int32_t id_;

    Query();
    ~Query();

    // The actual implementation of the constructor
    static NAN_METHOD(New);

    // Parse the query and bind the given parameters (if any).
    WRAPPED_METHOD_DECL(Parse);

    // Bind the given parameters to the prepared statement. This only works if
    // the query was obtained from a Prepared.
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

    _NAN_METHOD_RETURN_TYPE bind(Local<Array>& params);

    static void on_result_ready(CassFuture* future, void* data);
    void result_ready(CassFuture* future);

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

    uv_async_t* async_;

    BufferPool buffer_pool_;

    static v8::Persistent<v8::Function> constructor;
};

#endif
