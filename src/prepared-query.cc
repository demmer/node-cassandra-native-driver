#include <cassandra.h>

#include "prepared-query.h"
#include "client.h"
#include "error-callback.h"
#include "persistent-string.h"
#include "query.h"
#include "metrics.h"
#include "type-mapper.h"

Nan::Persistent<Function> PreparedQuery::constructor;

void PreparedQuery::Init() {
    Nan::HandleScope scope;

    // Prepare constructor template
    Local<FunctionTemplate> tpl = Nan::New<FunctionTemplate>(New);
    tpl->SetClassName(Nan::New("PreparedQuery").ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    Nan::SetPrototypeMethod(tpl, "prepare", WRAPPED_METHOD_NAME(Prepare));
    Nan::SetPrototypeMethod(tpl, "query", WRAPPED_METHOD_NAME(GetQuery));

    constructor.Reset(tpl->GetFunction());
}

Local<Object> PreparedQuery::NewInstance() {
    Nan::EscapableHandleScope scope;

    const unsigned argc = 0;
    Local<Value> argv[argc] = {};
    Local<Function> cons = Nan::New<Function>(constructor);
//    Local<Object> instance = Nan::NewInstance(cons, argc, argv).ToLocalChecked();
    Local<Object> instance = cons->NewInstance(argc, argv);

    return scope.Escape(instance);
}

NAN_METHOD(PreparedQuery::New) {
    Nan::EscapableHandleScope scope;

    PreparedQuery* obj = new PreparedQuery();
    obj->Wrap(info.This());

    info.GetReturnValue().Set(info.This());
}

PreparedQuery::PreparedQuery()
{
    statement_ = NULL;
    prepared_ = NULL;
}

PreparedQuery::~PreparedQuery()
{
    if (statement_) {
        cass_statement_free(statement_);
    }
    if (prepared_) {
        cass_prepared_free(prepared_);
    }
}

void
PreparedQuery::set_client(v8::Local<v8::Object> client)
{
    static PersistentString client_str("client");
    Nan::Set(this->handle(), client_str, client);

    Client* c = Nan::ObjectWrap::Unwrap<Client>(client);
    session_ = c->get_session();
    async_ = c->get_async();
    metrics_ = c->metrics();
}

WRAPPED_METHOD(PreparedQuery, Prepare)
{
    Nan::HandleScope scope;

    if (info.Length() != 2) {
        return Nan::ThrowError("invalid arguments");
    }

    Local<String> query = info[0].As<String>();
    Nan::Callback* callback = new Nan::Callback(info[1].As<Function>());

    String::Utf8Value query_str(query);
    CassFuture* future = cass_session_prepare_n(session_, *query_str, query_str.length());
    metrics_->start_request();
    async_->schedule(on_prepared_ready, future, this, callback);

    Ref();

    return;
}

void
PreparedQuery::on_prepared_ready(CassFuture* future, void* client, void* data)
{
    PreparedQuery* self = (PreparedQuery*)client;
    Nan::Callback* callback = (Nan::Callback*) data;
    self->prepared_ready(future, callback);
}

void
PreparedQuery::prepared_ready(CassFuture* future, Nan::Callback* callback)
{
    Nan::HandleScope scope;

    metrics_->stop_request();
    CassError code = cass_future_error_code(future);
    if (code != CASS_OK) {
        error_callback(future, callback);
    } else {
        prepared_ = cass_future_get_prepared(future);

        Local<Value> argv[] = {
            Nan::Null(),
            this->handle()
        };
        callback->Call(2, argv);
    }
    cass_future_free(future);
    delete callback;

    Unref();
}

WRAPPED_METHOD(PreparedQuery, GetQuery)
{
    Nan::HandleScope scope;

    if (prepared_ == NULL) {
        return Nan::ThrowError("query can only be called after prepare");
    }

    Local<Value> val = Query::NewInstance();

    Query* query = Nan::ObjectWrap::Unwrap<Query>(val->ToObject());

    static PersistentString client_str("client");
    query->set_client(Nan::To<v8::Object>(Nan::Get(this->handle(), client_str).ToLocalChecked()).ToLocalChecked());

    query->set_prepared_statement(prepare_statement());

    info.GetReturnValue().Set(val);
}
