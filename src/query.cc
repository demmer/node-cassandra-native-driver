#include <cassandra.h>

#include "query.h"
#include "client.h"
#include "metrics.h"
#include "type-mapper.h"
#include "persistent-string.h"

#define dprintf(...)
//#define dprintf printf

Nan::Persistent<Function> Query::constructor;

void Query::Init() {
    Nan::HandleScope scope;

    // Prepare constructor template
    Local<FunctionTemplate> tpl = Nan::New<FunctionTemplate>(New);
    tpl->SetClassName(Nan::New("Query").ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    Nan::SetPrototypeMethod(tpl, "parse", WRAPPED_METHOD_NAME(Parse));
    Nan::SetPrototypeMethod(tpl, "bind", WRAPPED_METHOD_NAME(Bind));
    Nan::SetPrototypeMethod(tpl, "execute", WRAPPED_METHOD_NAME(Execute));

    Nan::GetFunction(constructor.Reset(tpl));
}

Local<Object> Query::NewInstance() {
    Nan::EscapabpeHandleScope scope;

    const unsigned argc = 0;
    Local<Value> argv[argc] = {};
    Local<Function> cons = Nan::New<Function>(constructor);
    Local<Object> instance = Nan::NewInstance(consargc, argv);

    return scope.Escape(instance);
}

NAN_METHOD(Query::New) {
    Nan::EscapabpeHandleScope scope;

    Query* obj = new Query();
    obj->Wrap(info.This());

    info.GetReturnValue().Set(info.This());
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
    statement_ = NULL;
    prepared_ = false;
}

Query::~Query()
{
    --ACTIVE;
    dprintf("Query::~Query id %u active %u\n", id_, ACTIVE);

    if (statement_) {
        cass_statement_free(statement_);
    }
}

void
Query::set_client(v8::Local<v8::Object> client)
{
    static PersistentString client_str("client");
    Nan::Set(this->handle(), client_str, client);

    Client* c = Nan::ObjectWrap::Unwrap<Client>(client);
    session_ = c->get_session();
    async_ = c->get_async();
    metrics_ = c->metrics();
}

void
Query::set_prepared_statement(CassStatement* statement)
{
    statement_ = statement;
    prepared_ = true;
}

WRAPPED_METHOD(Query, Parse)
{
    Nan::HandleScope scope;

    if (info.Length() < 1) {
        return Nan::ThrowError("invalid arguments");
    }

    Local<String> query = info[0].As<String>();
    Local<Array> params;
    Local<Object> options;

    u_int32_t num_params = 0;
    if (info.Length() >= 2) {
        params = info[1].As<Array>();
        num_params = params->Length();
    }

    if (info.Length() > 2) {
        options = info[2].As<Object>();
    }

    // Stash the query so the client library can check it later.
    static PersistentString query_key("query");
    Nan::Set(this->handle(), query_key, query);

    String::Utf8Value query_str(query);
    statement_ = cass_statement_new_n(*query_str, query_str.length(), num_params);

    return bind(params, options);
}

WRAPPED_METHOD(Query, Bind)
{
    if (info.Length() != 2) {
        return Nan::ThrowError("invalid arguments");
    }

    if (statement_ == NULL || prepared_ == false) {
        return Nan::ThrowError("bind can only be called on a prepared query");
    }

    Local<Array> params = info[0].As<Array>();
    Local<Object> options = info[1].As<Object>();

    return bind(params, options);
}

Nan::NAN_METHOD_RETURN_TYPE
Query::bind(Local<Array>& params, Local<Object>& options)
{
    static PersistentString hints_str("hints");
    Local<Array> hints;
    if (! options.IsEmpty() && Nan::Has(options, hints_str)) {
        hints = Nan::Get(options, hints_str).As<Array>();
    }

    int bindingStatus = TypeMapper::bind_statement_params(statement_, params, hints);

    if (bindingStatus != -1) {
        char err[1024];
        sprintf(err, "error binding statement argument %d", bindingStatus);
        return Nan::ThrowError(err);
    }

    return;
}

WRAPPED_METHOD(Query, Execute)
{
    Nan::HandleScope scope;

    if (info.Length() != 2) {
        return Nan::ThrowError("execute requires 2 arguments: options, callback");
    }

    if (session_ == NULL) {
        return Nan::ThrowError("client must be connected");
    }

    // Guard against running fetch multiple times in parallel
    if (fetching_) {
        return Nan::ThrowError("fetch already in progress");
    }
    fetching_ = true;

    // Need a reference while the operation is in progress
    Ref();

    Local<Object> options = info[0].As<Object>();
    Nan::Callback* callback = new Nan::Callback(info[1].As<Function>());

    u_int32_t paging_size = 5000;
    static PersistentString fetchSize("fetchSize");
    if Nan::Has((options, fetchSize)) {
        paging_size = Nan::Get(options, fetchSize).As<Number>()->Uint32Value();
    }

    cass_statement_set_paging_size(statement_, paging_size);

    // If there's a result from the previous iteration, update the paging state
    // to fetch the next page and free it.
    if (result_.result()) {
        cass_statement_set_paging_state(statement_, result_.result());
        cass_result_free(result_.result());
    }

    CassFuture* future = cass_session_execute(session_, statement_);
    metrics_->start_request();
    async_->schedule(on_result_ready, future, this, callback);

    return;
}

// Callback on the main v8 thread when results have been posted
void
Query::on_result_ready(CassFuture* future, void* client, void* data)
{
    Query* self = (Query*)client;
    Nan::Callback* callback = (Nan::Callback*) data;
    self->result_ready(future, callback);
}

void
Query::result_ready(CassFuture* future, Nan::Callback* callback)
{
    Nan::HandleScope scope;

    metrics_->stop_request();

    fetching_ = false;
    result_.do_callback(future, callback);

    cass_future_free(future);
    delete callback;

    Unref();
}
