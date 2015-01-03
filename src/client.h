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

    explicit Client();
    ~Client();

    static NAN_METHOD(New);

    WRAPPED_METHOD_DECL(Connect);
    WRAPPED_METHOD_DECL(NewQuery);

    static v8::Persistent<v8::Function> constructor;
};

#endif
