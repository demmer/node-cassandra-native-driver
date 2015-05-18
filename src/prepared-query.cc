#include <cassandra.h>

#include "prepared-query.h"
#include "client.h"
#include "error-callback.h"
#include "persistent-string.h"
#include "query.h"
#include "metrics.h"
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
PreparedQuery::set_client(Local<Object>& client)
{
    static PersistentString client_str("client");
    handle_->Set(client_str, client);

    Client* c = node::ObjectWrap::Unwrap<Client>(client);
    session_ = c->get_session();
    async_ = c->get_async();
    metrics_ = c->metrics();
}

WRAPPED_METHOD(PreparedQuery, Prepare)
{
    NanScope();

    if (args.Length() != 2) {
        return NanThrowError("invalid arguments");
    }

    Local<String> query = args[0].As<String>();
    NanCallback* callback = new NanCallback(args[1].As<Function>());

    String::AsciiValue query_str(query);
    CassFuture* future = cass_session_prepare_n(session_, *query_str, query_str.length());
    metrics_->start_request();
    async_->schedule(on_prepared_ready, future, this, callback);

    Ref();

    NanReturnUndefined();
}

// Callback on the I/O thread when a result is ready from cassandra
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
    metrics_->stop_request();
    CassError code = cass_future_error_code(future);
    if (code != CASS_OK) {
        error_callback(future, callback);
    } else {
        prepared_ = cass_future_get_prepared(future);

        Handle<Value> argv[] = {
            NanNull(),
            handle_
        };
        callback->Call(2, argv);
    }
    cass_future_free(future);
    delete callback;

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

    static PersistentString client_str("client");
    query->set_client(handle_->Get(client_str).As<Object>());

    query->set_prepared_statement(prepare_statement());

    NanReturnValue(val);
}
