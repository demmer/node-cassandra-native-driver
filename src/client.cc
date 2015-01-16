
#include "client.h"
#include "batch.h"
#include "prepared-query.h"
#include "query.h"

using namespace v8;

Persistent<Function> Client::constructor;

Client::Client() {
    printf("In Client constructor %p\n", this);
    cluster_ = cass_cluster_new();
    session_ = NULL;
}

Client::~Client() {
    printf("In Client destructor %p\n", this);
    if (session_) {
        cass_session_free(session_);
        session_ = NULL;
    }
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
    NODE_SET_PROTOTYPE_METHOD(tpl, "new_query", WRAPPED_METHOD_NAME(NewQuery));
    NODE_SET_PROTOTYPE_METHOD(tpl, "new_prepared_query", WRAPPED_METHOD_NAME(NewPreparedQuery));
    NODE_SET_PROTOTYPE_METHOD(tpl, "new_batch", WRAPPED_METHOD_NAME(NewBatch));

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

WRAPPED_METHOD(Client, Connect) {
    NanScope();
    if (args.Length() < 2) {
        return NanThrowError("connect requires 2 arguments: address and callback");
    }

    String::AsciiValue address(args[0].As<String>());
    NanCallback* callback = new NanCallback(args[1].As<Function>());

    cass_cluster_set_contact_points(cluster_, *address);

    if (args.Length() > 2) {
        int port = args[2].As<Number>()->Int32Value();
        cass_cluster_set_port(cluster_, port);
    }

    session_ = cass_session_new();
    CassFuture* future = cass_session_connect(session_, cluster_);
    async_.schedule(on_connected, future, this, callback);

    Ref();
    NanReturnUndefined();
}

void
Client::on_connected(CassFuture* future, void* client, void* data)
{
    Client* self = (Client*) client;
    NanCallback* callback = (NanCallback*) data;
    self->connected(future, callback);
}

void
Client::connected(CassFuture* future, NanCallback* callback)
{
    NanScope();

    CassError code = cass_future_error_code(future);
    if (code != CASS_OK) {
        CassString error = cass_future_error_message(future);
        std::string error_str = std::string(error.data, error.length);

        Handle<Value> argv[] = {
            NanError(error_str.c_str())
        };
        callback->Call(1, argv);
    } else {
        Handle<Value> argv[] = {
            NanNull(),
        };
        callback->Call(1, argv);
    }
    cass_future_free(future);
    delete callback;

    Unref();
}

WRAPPED_METHOD(Client, NewQuery) {
    NanScope();
    Local<Value> val = Query::NewInstance();

    Query* query = node::ObjectWrap::Unwrap<Query>(val->ToObject());

    Local<Object> self = Local<Object>::New(handle_);
    query->set_client(self);

    NanReturnValue(val);
}

WRAPPED_METHOD(Client, NewPreparedQuery) {
    NanScope();
    Local<Value> val = PreparedQuery::NewInstance();

    PreparedQuery* query = node::ObjectWrap::Unwrap<PreparedQuery>(val->ToObject());

    Local<Object> self = Local<Object>::New(handle_);
    query->set_client(self);

    NanReturnValue(val);
}

WRAPPED_METHOD(Client, NewBatch) {
    NanScope();

    if (args.Length() != 1) {
        return NanThrowError("must specify batch type");
    }

    Local<String> type = args[0].As<String>();
    Local<Value> val = Batch::NewInstance(type);
    if (! val.IsEmpty()) {
        Batch* batch = node::ObjectWrap::Unwrap<Batch>(val->ToObject());

        Local<Object> self = Local<Object>::New(handle_);
        batch->set_client(self);
    }

    NanReturnValue(val);
}
