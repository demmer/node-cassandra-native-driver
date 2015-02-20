#include <cassandra.h>

#include "batch.h"
#include "client.h"
#include "persistent-string.h"
#include "prepared-query.h"
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
    NODE_SET_PROTOTYPE_METHOD(tpl, "add_prepared", WRAPPED_METHOD_NAME(AddPrepared));
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
    batch_ = cass_batch_new(type);
}

Batch::~Batch()
{
    cass_batch_free(batch_);
}

void
Batch::set_client(const Local<Object>& client)
{
    static PersistentString client_str("client");
    handle_->Set(client_str, client);

    Client* c = node::ObjectWrap::Unwrap<Client>(client);
    session_ = c->get_session();
    async_ = c->get_async();
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

WRAPPED_METHOD(Batch, AddPrepared)
{
    if (args.Length() < 2) {
        return NanThrowError("add_prepared requires prepared and vals");
    }

    Local<Object> prepared_obj = args[0].As<Object>();
    PreparedQuery* prepared = node::ObjectWrap::Unwrap<PreparedQuery>(prepared_obj->ToObject());
    Local<Array> params = args[1].As<Array>();
    Local<Array> hints;

    if (args.Length() > 2) {
        static PersistentString hints_str("hints");
        Local<Object> options = args[2].As<Object>();
        if (options->Has(hints_str)) {
            hints = options->Get(hints_str).As<Array>();
        }
    }

    CassStatement* statement = prepared->prepare_statement();
    int bindingStatus = TypeMapper::bind_statement_params(statement, params, hints);

    if (bindingStatus != -1) {
        char err[1024];
        sprintf(err, "error binding statement argument %d", bindingStatus);
        return NanThrowError(err);
    }

    cass_batch_add_statement(batch_, statement);
    cass_statement_free(statement);

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
    (void)options; // XXX what options make sense?

    NanCallback* callback = new NanCallback(args[1].As<Function>());

    if (result_.result()) {
        cass_result_free(result_.result());
    }

    CassFuture* future = cass_session_execute_batch(session_, batch_);
    async_->schedule(on_result_ready, future, this, callback);

    NanReturnUndefined();
}

void
Batch::on_result_ready(CassFuture* future, void* client, void* data)
{
    Batch* self = (Batch*)client;
    NanCallback* callback = (NanCallback*) data;
    self->result_ready(future, callback);
}

void
Batch::result_ready(CassFuture* future, NanCallback* callback)
{
    NanScope();

    fetching_ = false;
    result_.do_callback(future, callback);

    cass_future_free(future);
    delete callback;

    Unref();
}
