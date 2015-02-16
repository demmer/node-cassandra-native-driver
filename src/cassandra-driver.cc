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
    NanScope();
    NanReturnValue(Client::NewInstance(args[0]));
}

void InitAll(Handle<Object> exports) {
    NanScope();

    Batch::Init();
    Client::Init();
    PreparedQuery::Init();
    Query::Init();

    exports->Set(NanNew("Client"),
        NanNew<FunctionTemplate>(CreateClient)->GetFunction());
    exports->Set(NanNew("set_log_callback"),
        NanNew<FunctionTemplate>(SetLogCallback)->GetFunction());
    exports->Set(NanNew("set_log_level"),
        NanNew<FunctionTemplate>(SetLogLevel)->GetFunction());
}

NODE_MODULE(cassandra_native_driver, InitAll)
