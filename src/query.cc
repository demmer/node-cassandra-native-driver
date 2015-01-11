#include <cassandra.h>

#include "query.h"
#include "client.h"
#include "type-mapper.h"

#define dprintf(...)
//#define dprintf printf

Persistent<Function> Query::constructor;

void Query::Init() {
    NanScope();

    // Prepare constructor template
    Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(New);
    tpl->SetClassName(NanNew("Query"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    NODE_SET_PROTOTYPE_METHOD(tpl, "parse", WRAPPED_METHOD_NAME(Parse));
    NODE_SET_PROTOTYPE_METHOD(tpl, "bind", WRAPPED_METHOD_NAME(Bind));
    NODE_SET_PROTOTYPE_METHOD(tpl, "execute", WRAPPED_METHOD_NAME(Execute));

    NanAssignPersistent(constructor, tpl->GetFunction());
}

Local<Object> Query::NewInstance() {
    NanEscapableScope();

    const unsigned argc = 0;
    Local<Value> argv[argc] = {};
    Local<Function> cons = NanNew<Function>(constructor);
    Local<Object> instance = cons->NewInstance(argc, argv);

    return NanEscapeScope(instance);
}

NAN_METHOD(Query::New) {
    NanEscapableScope();

    Query* obj = new Query();
    obj->Wrap(args.This());

    NanReturnValue(args.This());
}

u_int32_t COUNT = 0;
u_int32_t ACTIVE = 0;

Query::Query()
{
    id_ = COUNT;
    ++COUNT;
    ++ACTIVE;

    dprintf("Query::Query %u %u\n", id_, ACTIVE);
    fetching_ = false;
    callback_ = NULL;
    statement_ = NULL;
    prepared_ = false;

    async_ = new uv_async_t();
    uv_async_init(uv_default_loop(), async_, Query::on_async_ready);
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

Query::~Query()
{
    --ACTIVE;
    dprintf("Query::~Query id %u active %u\n", id_, ACTIVE);

    if (statement_) {
        cass_statement_free(statement_);
    }

    uv_handle_t* async_handle = (uv_handle_t*)async_;
    uv_close(async_handle, async_destroy);
}

void
Query::set_client(const Local<Object>& client)
{
    handle_->Set(NanNew("client"), client);
    session_ = node::ObjectWrap::Unwrap<Client>(client)->get_session();
}

void
Query::set_prepared_statement(CassStatement* statement)
{
    statement_ = statement;
    prepared_ = true;
}

WRAPPED_METHOD(Query, Parse)
{
    NanScope();

    if (args.Length() < 1 || args.Length() > 2) {
        return NanThrowError("invalid arguments");
    }

    Local<String> query = args[0].As<String>();
    Local<Array> params;

    u_int32_t num_params = 0;
    if (args.Length() == 2) {
        params = args[1].As<Array>();
        num_params = params->Length();
    }

    // Stash the query so the client library can check it later.
    handle_->Set(NanNew("query"), query);

    String::AsciiValue query_str(query);
    statement_ = cass_statement_new(cass_string_init(*query_str), num_params);

    return bind(params);
}

WRAPPED_METHOD(Query, Bind)
{
    if (args.Length() != 1) {
        return NanThrowError("invalid arguments");
    }

    if (statement_ == NULL || prepared_ == false) {
        return NanThrowError("bind can only be called on a prepared query");
    }

    Local<Array> params = args[0].As<Array>();
    return bind(params);
}

_NAN_METHOD_RETURN_TYPE
Query::bind(Local<Array>& params)
{
    for (u_int32_t i = 0; i < params->Length(); ++i) {
        Local<Value> arg = params->Get(i);
        if (! TypeMapper::bind_statement_param(statement_, i, arg)) {
            char err[1024];
            sprintf(err, "error binding statement argument %d", i);
            return NanThrowError(err);
        }
    }

    NanReturnUndefined();
}

WRAPPED_METHOD(Query, Execute)
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
    NanCallback* callback = new NanCallback(args[1].As<Function>());

    u_int32_t paging_size = 5000;
    Local<String> fetchSize = NanNew("fetchSize");
    if (options->Has(fetchSize)) {
        paging_size = options->Get(fetchSize).As<Number>()->Uint32Value();
    }

    cass_statement_set_paging_size(statement_, paging_size);

    callback_ = callback;

    // If there's a result from the previous iteration, update the paging state
    // to fetch the next page and free it.
    if (result_.result()) {
        cass_statement_set_paging_state(statement_, result_.result());
        cass_result_free(result_.result());
    }

    CassFuture* future = cass_session_execute(session_, statement_);
    cass_future_set_callback(future, Result::on_ready, &result_);

    NanReturnUndefined();
}

// Callback on the main v8 thread when results have been posted
void
Query::on_async_ready(uv_async_t* handle, int status)
{
    // XXX/demmer status?
    Query* self = (Query*)handle->data;
    self->async_ready();
}

void
Query::async_ready()
{
    NanScope();

    fetching_ = false;
    result_.do_callback(callback_);

    Unref();
}
