#include <cassandra.h>

#include "prepared-query.h"
#include "client.h"
#include "query.h"
#include "type-mapper.h"

Persistent<Function> PreparedQuery::constructor;

void PreparedQuery::Init() {
    NanScope();

    // Prepare constructor template
    Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(New);
    tpl->SetClassName(NanNew("PreparedQuery"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    NODE_SET_PROTOTYPE_METHOD(tpl, "prepare", WRAPPED_METHOD_NAME(Prepare));
    NODE_SET_PROTOTYPE_METHOD(tpl, "query", WRAPPED_METHOD_NAME(GetQuery));

    NanAssignPersistent(constructor, tpl->GetFunction());
}

Local<Object> PreparedQuery::NewInstance() {
    NanEscapableScope();

    const unsigned argc = 0;
    Local<Value> argv[argc] = {};
    Local<Function> cons = NanNew<Function>(constructor);
    Local<Object> instance = cons->NewInstance(argc, argv);

    return NanEscapeScope(instance);
}

NAN_METHOD(PreparedQuery::New) {
    NanEscapableScope();

    PreparedQuery* obj = new PreparedQuery();
    obj->Wrap(args.This());

    NanReturnValue(args.This());
}

PreparedQuery::PreparedQuery()
{
    printf("PreparedQuery::PreparedQuery\n");
    callback_ = NULL;
    statement_ = NULL;
    prepared_ = NULL;

    async_ = new uv_async_t();
    printf("new async %p\n", async_);

    uv_async_init(uv_default_loop(), async_, PreparedQuery::on_async_ready);
    async_->data = this;
}

static void
async_destroy(uv_handle_t* handle)
{
    uv_async_t* async = (uv_async_t*)handle;
    delete async;
}

PreparedQuery::~PreparedQuery()
{
    printf("PreparedQuery::~PreparedQuery\n");

    if (statement_) {
        cass_statement_free(statement_);
    }

    if (callback_) {
        delete callback_;
    }

    uv_handle_t* async_handle = (uv_handle_t*)async_;
    uv_close(async_handle, async_destroy);
}

void
PreparedQuery::set_client(Local<Object>& client)
{
    handle_->Set(NanNew("client"), client);
    session_ = node::ObjectWrap::Unwrap<Client>(client)->get_session();
}

WRAPPED_METHOD(PreparedQuery, Prepare)
{
    NanScope();

    if (args.Length() != 2) {
        return NanThrowError("invalid arguments");
    }

    Local<String> query = args[0].As<String>();
    callback_ = new NanCallback(args[1].As<Function>());

    String::AsciiValue query_str(query);
    CassFuture* future = cass_session_prepare(session_, cass_string_init(*query_str));

    // Need a reference while the operation is in progress
    Ref();

    cass_future_set_callback(future, on_prepared_ready, this);

    NanReturnUndefined();
}

// Callback on the I/O thread when a result is ready from cassandra
void
PreparedQuery::on_prepared_ready(CassFuture* future, void* data)
{
    PreparedQuery* self = (PreparedQuery*)data;
    self->prepared_ready(future);
}

void
PreparedQuery::prepared_ready(CassFuture* future)
{
    prepared_ = cass_future_get_prepared(future);
    result_code_ = cass_future_error_code(future);
    if (result_code_ != CASS_OK) {
        CassString error = cass_future_error_message(future);
        result_error_ = std::string(error.data, error.length);
    }
    cass_future_free(future);

    uv_async_send(async_);
}

// Callback on the main v8 thread when results have been posted
void
PreparedQuery::on_async_ready(uv_async_t* handle, int status)
{
    // XXX/demmer status?
    PreparedQuery* self = (PreparedQuery*)handle->data;
    self->async_ready();
}

void
PreparedQuery::async_ready()
{
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
        handle_
    };
    callback_->Call(2, argv);

    Unref();
}

WRAPPED_METHOD(PreparedQuery, GetQuery)
{
    NanScope();

    if (prepared_ == NULL) {
        return NanThrowError("query can only be called after prepare");
    }

    Local<Value> val = Query::NewInstance();

    Query* query = node::ObjectWrap::Unwrap<Query>(val->ToObject());

    query->set_client(handle_->Get(NanNew("client")).As<Object>());

    query->set_prepared_statement(prepare_statement());

    NanReturnValue(val);
}
