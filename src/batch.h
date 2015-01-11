#ifndef __CASS_DRIVER_BATCH_H__
#define __CASS_DRIVER_BATCH_H__

#include "node.h"
#include "nan.h"
#include "result.h"
#include "wrapped-method.h"
#include <vector>

using namespace v8;

class Client;

// Wrapper for a batched query
class Batch: public node::ObjectWrap {
public:
    // Initialize the class constructor.
    static void Init();

    // Create a new instance of the class.
    static v8::Local<v8::Object> NewInstance(const v8::Local<v8::String>& type);

    // Stash the reference to the parent client object and extract the pointer
    // to the session.
    void set_client(const v8::Local<v8::Object>& client);

private:
    Batch(CassBatchType type);
    ~Batch();

    // The actual implementation of the constructor
    static NAN_METHOD(New);

    // Add a Query reference to the batch
    WRAPPED_METHOD_DECL(AddQuery);

    // Execute the batch, potentially retrieving additional pages.
    WRAPPED_METHOD_DECL(Execute);

    static void on_async_ready(uv_async_t* handle, int status);
    void async_ready();

    CassSession* session_;
    CassBatch* batch_;

    bool fetching_;
    NanCallback* callback_;

    Result result_;

    uv_async_t* async_;

    static v8::Persistent<v8::Function> constructor;
};

#endif
