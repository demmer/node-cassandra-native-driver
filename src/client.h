#ifndef __CASS_DRIVER_CLIENT_H__
#define __CASS_DRIVER_CLIENT_H__

#include "cassandra.h"
#include "node.h"
#include "nan.h"
#include "wrapped-method.h"
#include <queue>

typedef std::queue<const CassResult*> CassResultQueue;

class Client : public node::ObjectWrap {
public:
    static void Init();
    static v8::Local<v8::Object> NewInstance(v8::Local<v8::Value> arg);

private:
    CassCluster* cluster_;
    CassSession* session_;

    // Structure to wrap a paged select operation.
    struct SelectState {
        Client* client_;
        CassStatement* statement_;
        NanCallback* callback_;

        CassResultQueue results_;

        uv_async_t async_;
        uv_mutex_t lock_;
    };

    explicit Client();
    ~Client();

    static NAN_METHOD(New);

    WRAPPED_METHOD_DECL(Connect);
    static v8::Persistent<v8::Function> constructor;
};

#endif
