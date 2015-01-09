
#include "client.h"
#include "query.h"

using namespace v8;

Persistent<Function> Client::constructor;

Client::Client() {
    printf("In Client constructor %p\n", this);
    cluster_ = cass_cluster_new();
    session_ = cass_session_new();
    async_ = new uv_async_t();
    uv_async_init(uv_default_loop(), async_, Client::on_async_ready);
    async_->data = this;
}

static void
async_destroy(uv_handle_t* handle)
{
    uv_async_t* async = (uv_async_t*)handle;
    delete async;
}

Client::~Client() {
    printf("In Client destructor %p\n", this);
    cass_session_free(session_);
    cass_cluster_free(cluster_);
    delete callback_;
    uv_close((uv_handle_t*) async_, async_destroy);
}

void Client::Init() {
    NanScope();

    // Prepare constructor template
    Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(New);
    tpl->SetClassName(NanNew("Client"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    // Prototype
    NODE_SET_PROTOTYPE_METHOD(tpl, "connect", WRAPPED_METHOD_NAME(Connect));
    NODE_SET_PROTOTYPE_METHOD(tpl, "query", WRAPPED_METHOD_NAME(NewQuery));

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
    if (args.Length() != 2) {
        return NanThrowError("connect requires 2 arguments: address and callback");
    }

    String::AsciiValue address(args[0].As<String>());

    cass_cluster_set_contact_points(cluster_, *address);
    callback_ = new NanCallback(args[1].As<Function>());

    CassFuture* future = cass_session_connect(session_, cluster_);
    cass_future_set_callback(future, on_result_ready, this);
    NanReturnUndefined();
}

void
Client::on_result_ready(CassFuture* future, void* data)
{
    Client* self = (Client*) data;
    self->on_connect(future);
}

void
Client::on_connect(CassFuture* future)
{
    result_code_ = cass_future_error_code(future);
    if (result_code_ != CASS_OK) {
        CassString error = cass_future_error_message(future);
        result_error_ = std::string(error.data, error.length);
    }
    cass_future_free(future);

    uv_async_send(async_);
}

void
Client::on_async_ready(uv_async_t* handle, int status)
{
    Client* self = (Client*)handle->data;
    self->async_ready();
}

void
Client::async_ready() {
    NanScope();
    if (result_code_ != CASS_OK) {
        Handle<Value> argv[] = {
            NanError(result_error_.c_str())
        };
        callback_->Call(1, argv);
        return;
    }

    Handle<Value> argv[] = {
        NanNull(),
    };
    callback_->Call(1, argv);
}

WRAPPED_METHOD(Client, NewQuery) {
    NanScope();

    if (args.Length() != 4) {
        return NanThrowError("query requires 4 arguments: query, params, options, callback");
    }

    Local<String> query_str = args[0].As<String>();
    Local<Array> params = args[1].As<Array>();
    Local<Object> options = args[2].As<Object>();
    NanCallback* callback = new NanCallback(args[3].As<Function>());

    Query* query;
    Local<Value> query_obj = options->Get(NanNew("query"));
    if (query_obj->IsUndefined()) {
        query_obj = Query::NewInstance();
        query = node::ObjectWrap::Unwrap<Query>(query_obj->ToObject());
        if (! query->bind(handle_, query_str, params)) {
            return NanThrowError("unable to bind statement params");
        }
    } else {
        query = node::ObjectWrap::Unwrap<Query>(query_obj->ToObject());
    }
    query->fetch(session_, options, callback);

    NanReturnUndefined();
}
