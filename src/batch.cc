#include <cassandra.h>

#include "batch.h"
#include "client.h"
#include "query.h"
#include "type-mapper.h"

#define dprintf(...)
//#define dprintf printf

Persistent<Function> Batch::constructor;

void Batch::Init() {
    NanScope();

    // Prepare constructor template
    Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(New);
    tpl->SetClassName(NanNew("Batch"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    NODE_SET_PROTOTYPE_METHOD(tpl, "add", WRAPPED_METHOD_NAME(AddQuery));
    NODE_SET_PROTOTYPE_METHOD(tpl, "execute", WRAPPED_METHOD_NAME(Execute));

    NanAssignPersistent(constructor, tpl->GetFunction());
}

Local<Object> Batch::NewInstance(const Local<String>& type) {
    NanEscapableScope();

    String::AsciiValue type_str(type);

    const unsigned argc = 1;
    Local<Value> argv[argc] = {type};
    Local<Function> cons = NanNew<Function>(constructor);
    Local<Object> instance = cons->NewInstance(argc, argv);

    return NanEscapeScope(instance);
}

NAN_METHOD(Batch::New) {
    NanEscapableScope();

    String::AsciiValue type_str(args[0].As<String>());

    CassBatchType type;
    if (!strcasecmp(*type_str, "logged")) {
        type = CASS_BATCH_TYPE_LOGGED;
    }
    else if (!strcasecmp(*type_str, "unlogged")) {
        type = CASS_BATCH_TYPE_UNLOGGED;
    }
    else if (!strcasecmp(*type_str, "counter")) {
        type = CASS_BATCH_TYPE_COUNTER;
    }
    else {
        return NanThrowError("invalid batch type");
    }
    Batch* obj = new Batch(type);
    obj->Wrap(args.This());

    NanReturnValue(args.This());
}

Batch::Batch(CassBatchType type)
{
    dprintf("Batch::Batch %u %u\n", id_, ACTIVE);

    fetching_ = false;
    callback_ = NULL;

    batch_ = cass_batch_new(type);

    async_ = new uv_async_t();
    uv_async_init(uv_default_loop(), async_, Batch::on_async_ready);
    async_->data = this;

    // XXX/demmer fix this up
    result_.async_ = async_;
}

static void
async_destroy(uv_handle_t* handle)
{
    uv_async_t* async = (uv_async_t*)handle;
    delete async;
}

Batch::~Batch()
{
    cass_batch_free(batch_);
    uv_handle_t* async_handle = (uv_handle_t*)async_;
    uv_close(async_handle, async_destroy);
}

void
Batch::set_client(const Local<Object>& client)
{
    handle_->Set(NanNew("client"), client);
    session_ = node::ObjectWrap::Unwrap<Client>(client)->get_session();
}

WRAPPED_METHOD(Batch, AddQuery)
{
    if (args.Length() != 1) {
        return NanThrowError("add requires query");
    }

    Local<Object> query_obj = args[0].As<Object>();
    Query* query = node::ObjectWrap::Unwrap<Query>(query_obj->ToObject());

    cass_batch_add_statement(batch_, query->statement());

    NanReturnUndefined();
}

WRAPPED_METHOD(Batch, Execute)
{
    NanScope();

    if (args.Length() != 2) {
        return NanThrowError("execute requires 2 arguments: options, callback");
    }

    if (session_ == NULL) {
        return NanThrowError("client must be connected");
    }

    // Guard against running fetch multiple times in parallel
    if (fetching_) {
        return NanThrowError("fetch already in progress");
    }
    fetching_ = true;

    // Need a reference while the operation is in progress
    Ref();

    Local<Object> options = args[0].As<Object>();
    (void)options;

    NanCallback* callback = new NanCallback(args[1].As<Function>());
    callback_ = callback;

    if (result_.result()) {
        cass_result_free(result_.result());
    }

    CassFuture* future = cass_session_execute_batch(session_, batch_);
    cass_future_set_callback(future, Result::on_ready, &result_);

    NanReturnUndefined();
}

// Callback on the main v8 thread when results have been posted
void
Batch::on_async_ready(uv_async_t* handle, int status)
{
    // XXX/demmer status?
    Batch* self = (Batch*)handle->data;
    self->async_ready();
}

void
Batch::async_ready()
{
    NanScope();

    fetching_ = false;
    result_.do_callback(callback_);

    Unref();
}
