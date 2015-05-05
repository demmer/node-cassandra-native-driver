#ifndef __CASS_DRIVER_CLIENT_H__
#define __CASS_DRIVER_CLIENT_H__

#include "cassandra.h"
#include "node.h"
#include "nan.h"
#include "wrapped-method.h"
#include "buffer-pool.h"
#include "async-future.h"

class Client : public node::ObjectWrap {
public:
    static void Init();
    static v8::Local<v8::Object> NewInstance(v8::Local<v8::Value> arg);

    CassSession* get_session() { return session_; }
    AsyncFuture* get_async() { return &async_; }
private:
    CassCluster* cluster_;
    CassSession* session_;

    AsyncFuture async_;

    static void on_connected(CassFuture* future, void* client, void* data);
    void connected(CassFuture* future, NanCallback* callback);

    explicit Client();
    ~Client();

    static NAN_METHOD(New);

    WRAPPED_METHOD_DECL(Connect);
    WRAPPED_METHOD_DECL(NewQuery);
    WRAPPED_METHOD_DECL(NewPreparedQuery);
    WRAPPED_METHOD_DECL(NewBatch);
    WRAPPED_METHOD_DECL(NewBulkPrepared);

    void configure(v8::Local<v8::Object> opts);

    static v8::Persistent<v8::Function> constructor;
};

#endif
