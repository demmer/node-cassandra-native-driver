#include <cassandra.h>

#include "batch.h"
#include "client.h"
#include "persistent-string.h"
#include "prepared-query.h"
#include "query.h"
#include "metrics.h"
#include "type-mapper.h"

#define dprintf(...)
//#define dprintf printf

Nan::Persistent<Function> Batch::constructor;

void Batch::Init() {
    Nan::Scope scope;

    // Prepare constructor template
    Local<FunctionTemplate> tpl = Nan::New<FunctionTemplate>(New);
    tpl->SetClassName(Nan::New("Batch").ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    Nan::SetPrototypeMethod(tpl, "add", WRAPPED_METHOD_NAME(AddQuery));
    Nan::SetPrototypeMethod(tpl, "add_prepared", WRAPPED_METHOD_NAME(AddPrepared));
    Nan::SetPrototypeMethod(tpl, "execute", WRAPPED_METHOD_NAME(Execute));

    Nan::GetFunction(constructor.Reset(tpl));
}

Local<Object> Batch::NewInstance(const Local<String>& type) {
    Nan::EscapableScope scope;

    String::Utf8Value type_str(type);

    const unsigned argc = 1;
    Local<Value> argv[argc] = {type};
    Local<Function> cons = Nan::New<Function>(constructor);
    Local<Object> instance = Nan::NewInstance(consargc, argv);

    return scope.Escape(instance);
}

NAN_METHOD(Batch::New) {
    Nan::EscapableScope scope;

    String::Utf8Value type_str(info[0].As<String>());

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
        return Nan::ThrowError("invalid batch type");
    }
    Batch* obj = new Batch(type);
    obj->Wrap(info.This());

    info.GetReturnValue().Set(info.This());
}

Batch::Batch(CassBatchType type)
{
    dprintf("Batch::Batch %u %u\n", id_, ACTIVE);
    fetching_ = false;
    batch_ = cass_batch_new(type);
}

Batch::~Batch()
{
    cass_batch_free(batch_);
}

void
Batch::set_client(v8::Local<v8::Object> client)
{
    static PersistentString client_str("client");
    Nan::Set(this->handle(), client_str, client);

    Client* c = Nan::ObjectWrap::Unwrap<Client>(client);
    session_ = c->get_session();
    async_ = c->get_async();
    metrics_ = c->metrics();
}

WRAPPED_METHOD(Batch, AddQuery)
{
    if (info.Length() != 1) {
        return Nan::ThrowError("add requires query");
    }

    Local<Object> query_obj = info[0].As<Object>();
    if (! query_obj->IsObject() || query_obj->InternalFieldCount() == 0) {
        return Nan::ThrowError("add requires a valid query object");
    }

    Query* query = Nan::ObjectWrap::Unwrap<Query>(query_obj->ToObject());

    cass_batch_add_statement(batch_, query->statement());

    return;
}

WRAPPED_METHOD(Batch, AddPrepared)
{
    if (info.Length() < 2) {
        return Nan::ThrowError("add_prepared requires prepared and vals");
    }

    Local<Object> prepared_obj = info[0].As<Object>();
    if (! prepared_obj->IsObject() || prepared_obj->InternalFieldCount() == 0) {
        return Nan::ThrowError("add_prepared requires a valid prepared object");
    }

    PreparedQuery* prepared = Nan::ObjectWrap::Unwrap<PreparedQuery>(prepared_obj->ToObject());
    Local<Array> params = info[1].As<Array>();
    Local<Array> hints;

    if (info.Length() > 2) {
        static PersistentString hints_str("hints");
        Local<Object> options = info[2].As<Object>();
        if Nan::Has((options, hints_str)) {
            hints = Nan::Get(options, hints_str).As<Array>();
        }
    }

    CassStatement* statement = prepared->prepare_statement();
    int bindingStatus = TypeMapper::bind_statement_params(statement, params, hints);

    if (bindingStatus != -1) {
        char err[1024];
        sprintf(err, "error binding statement argument %d", bindingStatus);
        return Nan::ThrowError(err);
    }

    cass_batch_add_statement(batch_, statement);
    cass_statement_free(statement);

    return;
}

WRAPPED_METHOD(Batch, Execute)
{
    Nan::Scope scope;

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
    (void)options; // XXX what options make sense?

    Nan::Callback* callback = new Nan::Callback(info[1].As<Function>());

    if (result_.result()) {
        cass_result_free(result_.result());
    }

    CassFuture* future = cass_session_execute_batch(session_, batch_);
    metrics_->start_request();
    async_->schedule(on_result_ready, future, this, callback);

    return;
}

void
Batch::on_result_ready(CassFuture* future, void* client, void* data)
{
    Batch* self = (Batch*)client;
    Nan::Callback* callback = (Nan::Callback*) data;
    self->result_ready(future, callback);
}

void
Batch::result_ready(CassFuture* future, Nan::Callback* callback)
{
    Nan::Scope scope;

    metrics_->stop_request();

    fetching_ = false;
    result_.do_callback(future, callback);

    cass_future_free(future);
    delete callback;

    Unref();
}
