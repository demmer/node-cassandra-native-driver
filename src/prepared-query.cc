#include <cassandra.h>

#include "prepared-query.h"
#include "client.h"
#include "error-callback.h"
#include "persistent-string.h"
#include "query.h"
#include "metrics.h"
#include "type-mapper.h"

NanPersistent<Function> PreparedQuery::constructor;

void PreparedQuery::Init() {
    NanScope scope;

    // Prepare constructor template
    Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(New);
    tpl->SetClassName(NanNew("PreparedQuery").ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    NanSetPrototypeMethod(tpl, "prepare", WRAPPED_METHOD_NAME(Prepare));
    NanSetPrototypeMethod(tpl, "query", WRAPPED_METHOD_NAME(GetQuery));

    NanGetFunction(constructor.Reset(tpl));
}

Local<Object> PreparedQuery::NewInstance() {
    NanEscapableScope scope;

    const unsigned argc = 0;
    Local<Value> argv[argc] = {};
    Local<Function> cons = NanNew<Function>(constructor);
    Local<Object> instance = NanNewInstance(consargc, argv);

    return scope.Escape(instance);
}

NAN_METHOD(PreparedQuery::New) {
    NanEscapableScope scope;

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
    NanSet(this->handle(), client_str, client);

    Client* c = NanObjectWrap::Unwrap<Client>(client);
    session_ = c->get_session();
    async_ = c->get_async();
    metrics_ = c->metrics();
}

WRAPPED_METHOD(PreparedQuery, Prepare)
{
    NanScope scope;

    if (info.Length() != 2) {
        return NanThrowError("invalid arguments");
    }

    Local<String> query = info[0].As<String>();
    NanCallback* callback = new NanCallback(info[1].As<Function>());

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
    NanCallback* callback = (NanCallback*) data;
    self->prepared_ready(future, callback);
}

void
PreparedQuery::prepared_ready(CassFuture* future, NanCallback* callback)
{
    NanScope scope;

    metrics_->stop_request();
    CassError code = cass_future_error_code(future);
    if (code != CASS_OK) {
        error_callback(future, callback);
    } else {
        prepared_ = cass_future_get_prepared(future);

        Handle<Value> argv[] = {
            NanNull(),
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
    NanScope scope;

    if (prepared_ == NULL) {
        return NanThrowError("query can only be called after prepare");
    }

    Local<Value> val = Query::NewInstance();

    Query* query = NanObjectWrap::Unwrap<Query>(val->ToObject());

    static PersistentString client_str("client");
    NanGet(query->set_client(this->handle(), client_str).As<Object>());

    query->set_prepared_statement(prepare_statement());

    info.GetReturnValue().Set(val);
}
