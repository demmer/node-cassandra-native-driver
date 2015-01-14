#ifndef __CASS_DRIVER_PREPARED_QUERY_H__
#define __CASS_DRIVER_PREPARED_QUERY_H__

#include "node.h"
#include "nan.h"
#include "buffer-pool.h"
#include "wrapped-method.h"
#include <vector>

using namespace v8;

class Client;

// Wrapper for an in-progress PreparedQuery to the back end
class PreparedQuery: public node::ObjectWrap {
public:
    // Initialize the class constructor.
    static void Init();

    // Create a new instance of the class.
    static v8::Local<v8::Object> NewInstance();

    // Stash the reference to the parent client object and extract the pointer
    // to the session.
    void set_client(v8::Local<v8::Object>& client);

    // Return a new cassandra prepared statement
    CassStatement* prepare_statement()
    {
        return cass_prepared_bind(prepared_);
    }

private:
    PreparedQuery();
    ~PreparedQuery();

    // The actual implementation of the constructor
    static NAN_METHOD(New);

    // Parse the query statement
    WRAPPED_METHOD_DECL(Prepare);
    WRAPPED_METHOD_DECL(GetQuery);

    static void on_prepared_ready(CassFuture* future, void* data);
    void prepared_ready(CassFuture* future);

    static void on_async_ready(uv_async_t* handle, int status);
    void async_ready();

    CassSession* session_;
    CassStatement* statement_;
    const CassPrepared* prepared_;
    NanCallback* callback_;
    CassError result_code_;
    std::string result_error_;

    uv_async_t* async_;

    static v8::Persistent<v8::Function> constructor;
};

#endif
