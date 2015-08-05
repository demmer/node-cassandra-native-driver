#ifndef __CASS_DRIVER_BATCH_H__
#define __CASS_DRIVER_BATCH_H__

#include "node.h"
#include "nan.h"
#include "result.h"
#include "wrapped-method.h"
#include <vector>

using namespace v8;

class AsyncFuture;
class Client;
class Metrics;

// Wrapper for a batched query
class Batch: public Nan::ObjectWrap {
public:
    // Initialize the class constructor.
    static void Init();

    // Create a new instance of the class.
    static v8::Local<v8::Object> NewInstance(const v8::Local<v8::String>& type);

    // Stash the reference to the parent client object and extract the pointer
    // to the session.
    void set_client(v8::Local<v8::Object> client);

private:
    Batch(CassBatchType type);
    ~Batch();

    // The actual implementation of the constructor
    static NAN_METHOD(New);

    // Add a bound query to the batch
    WRAPPED_METHOD_DECL(AddQuery);

    // Add a prepared query to the batch
    WRAPPED_METHOD_DECL(AddPrepared);

    // Execute the batch
    WRAPPED_METHOD_DECL(Execute);

    static void on_result_ready(CassFuture* future, void* client, void* data);
    void result_ready(CassFuture* future, Nan::Callback* callback);

    CassSession* session_;
    CassBatch* batch_;
    Metrics* metrics_;

    bool fetching_;

    AsyncFuture* async_;
    Result result_;

    static Nan::Persistent<v8::Function> constructor;
};

#endif
