
#include "cassandra.h"
#include "node.h"
#include "nan.h"
#include <queue>

typedef std::queue<const CassResult*> CassResultQueue;

#define WRAPPED_METHOD_NAME(_name) JS##_name

#define DECLARE_METHOD(_name) \
    static NAN_METHOD(WRAPPED_METHOD_NAME(_name)); \
    NAN_METHOD(_name);

#define DEFINE_METHOD(_name) \
NAN_METHOD(Client::JS##_name) { \
    Client* obj = ObjectWrap::Unwrap<Client>(args.Holder()); \
    return obj->_name(args); \
} \
NAN_METHOD(Client::_name)

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

    DECLARE_METHOD(Connect);

    static v8::Persistent<v8::Function> constructor;
};
