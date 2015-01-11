#include <node.h>
#include <nan.h>

#include <cassandra.h>

#include "batch.h"
#include "client.h"
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
}

NODE_MODULE(cassandra_native, InitAll)
