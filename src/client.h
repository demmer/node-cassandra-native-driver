
#include "node.h"
#include "nan.h"

class Client : public node::ObjectWrap {
public:
    static void Init();
    static v8::Local<v8::Object> NewInstance(v8::Local<v8::Value> arg);

private:
    explicit Client();
    ~Client();

    static NAN_METHOD(New);
    static NAN_METHOD(Connect);
    static v8::Persistent<v8::Function> constructor;
};
