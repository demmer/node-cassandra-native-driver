#include <node.h>
#include <nan.h>

#include <cassandra.h>

#include "batch.h"
#include "client.h"
#include "logging.h"
#include "prepared-query.h"
#include "query.h"

using namespace v8;

NAN_METHOD(CreateClient) {
    info.GetReturnValue().Set(Client::NewInstance(info[0]));
}

void InitAll(Handle<Object> exports) {
    Nan::Scope scope;

    Batch::Init();
    Client::Init();
    PreparedQuery::Init();
    Query::Init();

    Nan::Set(exports, Nan::New("Client").ToLocalChecked(),
        Nan::GetFunction(Nan::New<FunctionTemplate>(CreateClient)));
    Nan::Set(exports, Nan::New("set_log_callback").ToLocalChecked(),
        Nan::GetFunction(Nan::New<FunctionTemplate>(SetLogCallback)));
    Nan::Set(exports, Nan::New("set_log_level").ToLocalChecked(),
        Nan::GetFunction(Nan::New<FunctionTemplate>(SetLogLevel)));
}

NODE_MODULE(cassandra_native_driver, InitAll)
