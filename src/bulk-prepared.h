#ifndef __CASS_DRIVER_BULK_PREPARED_H__
#define __CASS_DRIVER_BULK_PREPARED_H__

#include "node.h"
#include "nan.h"
#include "result.h"
#include "wrapped-method.h"
#include <vector>

using namespace v8;

class AsyncFuture;
class Client;

// For cases where multiple rows are being inserted or updated as a group but
// without wrapping them in a batch operation, the caller can create a bulk
// query to wrap a bunch of operations without requiring a separate
// javascript-exposed query object for each operation. This is only suitable for
// INSERT / UPDATE queries since the results are not gathered individually but
// instead the whole operation can either succeed or fail.
class BulkPrepared: public node::ObjectWrap {
public:
    // Initialize the class constructor.
    static void Init();

    // Create a new instance of the class.
    static v8::Local<v8::Object> NewInstance();

    // Stash the reference to the parent client object and extract the pointer
    // to the session.
    void set_client(const v8::Local<v8::Object>& client);

private:
    BulkPrepared();
    virtual ~BulkPrepared();

    // The actual implementation of the constructor
    static NAN_METHOD(New);

    // Start executing the given prepared query, adding it to the queue of
    // pending queries.
    WRAPPED_METHOD_DECL(Add);

    // Mark the end of this bulk operation. Waits for all outstanding queries
    // and then issues the given callback.
    WRAPPED_METHOD_DECL(Done);

    void check_if_done();

    static void on_future_ready(CassFuture* future, void* data);
    void future_ready(CassFuture* future);

    static void on_async_ready(uv_async_t* handle, int status);
    void async_ready();

    CassSession* session_;

    uv_mutex_t lock_;

    size_t pending_;                  // count of outstanding queries
    size_t success_;                  // count of successful queries
    std::vector<std::string> errors_; // error strings from failed queries

    NanCallback* callback_;
    uv_async_t* async_;

    static v8::Persistent<v8::Function> constructor;
};

#endif
