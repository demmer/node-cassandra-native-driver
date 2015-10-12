#ifndef __CASS_DRIVER_PREPARED_QUERY_H__
#define __CASS_DRIVER_PREPARED_QUERY_H__

#include "node.h"
#include "nan.h"
#include "wrapped-method.h"
#include <vector>

using namespace v8;

class AsyncFuture;
class Client;
class Metrics;

// Wrapper for an in-progress PreparedQuery to the back end
class PreparedQuery: public Nan::ObjectWrap {
public:
    // Initialize the class constructor.
    static void Init();

    // Create a new instance of the class.
    static v8::Local<v8::Object> NewInstance();

    // Stash the reference to the parent client object and extract the pointer
    // to the session.
    void set_client(v8::Local<v8::Object> client);

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

    static void on_prepared_ready(CassFuture* future, void* client, void* data);
    void prepared_ready(CassFuture* future, Nan::Callback* callback);

    CassSession* session_;
    AsyncFuture* async_;
    Metrics* metrics_;
    CassStatement* statement_;
    const CassPrepared* prepared_;

    static Nan::Persistent<v8::Function> constructor;
};

#endif
