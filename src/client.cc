
#include "client.h"

using namespace v8;

Persistent<Function> Client::constructor;

Client::Client() {
}

Client::~Client() {
}

void Client::Init() {
    NanScope();

    // Prepare constructor template
    Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(New);
    tpl->SetClassName(NanNew("Client"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    // Prototype
    NODE_SET_PROTOTYPE_METHOD(tpl, "connect", Connect);

    NanAssignPersistent(constructor, tpl->GetFunction());
}

Local<Object> Client::NewInstance(Local<Value> arg) {
    NanEscapableScope();

    const unsigned argc = 1;
    Local<Value> argv[argc] = { arg };
    Local<Function> cons = NanNew<Function>(constructor);
    Local<Object> instance = cons->NewInstance(argc, argv);

    return NanEscapeScope(instance);
}

NAN_METHOD(Client::New) {
    NanScope();

    if (args.IsConstructCall()) {
        // Invoked as constructor: `new Client(...)`
        Client* obj = new Client();
        obj->Wrap(args.This());
        NanReturnValue(args.This());
    } else {
        // Invoked as plain function `Client(...)`, turn into construct call.
        const int argc = 1;
        Local<Value> argv[argc] = { args[0] };
        Local<Function> cons = NanNew<Function>(constructor);
        NanReturnValue(cons->NewInstance(argc, argv));
    }
}

NAN_METHOD(Client::Connect) {
    NanScope();

    printf("in Client::Connect\n");

    NanReturnUndefined();
}
