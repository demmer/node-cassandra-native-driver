#include <node.h>
#include <nan.h>

#include <cassandra.h>

#include "client.h"

using namespace v8;

NAN_METHOD(CreateClient) {
    NanScope();
    NanReturnValue(Client::NewInstance(args[0]));
}

void InitAll(Handle<Object> exports) {
    NanScope();

    Client::Init();

    exports->Set(NanNew("Client"),
        NanNew<FunctionTemplate>(CreateClient)->GetFunction());
}

NODE_MODULE(cassandra_native, InitAll)
