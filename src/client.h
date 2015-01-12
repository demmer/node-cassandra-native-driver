#ifndef __CASS_DRIVER_CLIENT_H__
#define __CASS_DRIVER_CLIENT_H__

#include "cassandra.h"
#include "node.h"
#include "nan.h"
#include "wrapped-method.h"
#include "buffer-pool.h"

class Client : public node::ObjectWrap {
public:
    static void Init();
    static v8::Local<v8::Object> NewInstance(v8::Local<v8::Value> arg);

private:
    CassCluster* cluster_;
    CassSession* session_;
    NanCallback* callback_;
    CassError result_code_;
    std::string result_error_;
    uv_async_t* async_;

    static void on_result_ready(CassFuture* future, void* data);
    void on_connect(CassFuture* future);

    static void on_async_ready(uv_async_t* handle, int status);
    void async_ready();

    explicit Client();
    ~Client();

    static NAN_METHOD(New);

    WRAPPED_METHOD_DECL(Connect);
    WRAPPED_METHOD_DECL(NewQuery);

    static v8::Persistent<v8::Function> constructor;
};

#endif
