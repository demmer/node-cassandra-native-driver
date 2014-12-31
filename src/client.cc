
#include "client.h"

using namespace v8;

Persistent<Function> Client::constructor;

Client::Client() {
    cluster_ = cass_cluster_new();
    session_ = cass_session_new();
}

Client::~Client() {
    cass_session_free(session_);
    cass_cluster_free(cluster_);
}

void Client::Init() {
    NanScope();

    // Prepare constructor template
    Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(New);
    tpl->SetClassName(NanNew("Client"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    // Prototype
    NODE_SET_PROTOTYPE_METHOD(tpl, "connect", WRAPPED_METHOD_NAME(Connect));

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

WRAPPED_METHOD(Connect) {
    NanScope();

    // XXX/demmer:
    // 1. make this async
    // 2. parameterize contact points and other options
    cass_cluster_set_contact_points(cluster_, "127.0.0.1");

    CassFuture* future = cass_session_connect(session_, cluster_);
    cass_future_wait(future);

    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        CassString message = cass_future_error_message(future);
        std::string msg(message.data, message.length);
        cass_future_free(future);
        return NanThrowError(msg.c_str());
    }
    cass_future_free(future);

    NanReturnUndefined();
}
