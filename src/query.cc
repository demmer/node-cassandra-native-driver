#include <cassandra.h>

#include "query.h"
#include "client.h"
#include "type-mapper.h"
#include "persistent-string.h"

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
Query::set_client(const Local<Object>& client)
{
    static PersistentString client_str("client");
    handle_->Set(client_str, client);

    Client* c = node::ObjectWrap::Unwrap<Client>(client);
    session_ = c->get_session();
    async_ = c->get_async();
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

    if (args.Length() < 1) {
        return NanThrowError("invalid arguments");
    }

    Local<String> query = args[0].As<String>();
    Local<Array> params;
    Local<Object> options;

    u_int32_t num_params = 0;
    if (args.Length() >= 2) {
        params = args[1].As<Array>();
        num_params = params->Length();
    }

    if (args.Length() > 2) {
        options = args[2].As<Object>();
    }

    // Stash the query so the client library can check it later.
    static PersistentString query_key("query");
    handle_->Set(query_key, query);

    String::AsciiValue query_str(query);
    statement_ = cass_statement_new(cass_string_init(*query_str), num_params);

    return bind(params, options);
}

WRAPPED_METHOD(Query, Bind)
{
    if (args.Length() != 2) {
        return NanThrowError("invalid arguments");
    }

    if (statement_ == NULL || prepared_ == false) {
        return NanThrowError("bind can only be called on a prepared query");
    }

    Local<Array> params = args[0].As<Array>();
    Local<Object> options = args[1].As<Object>();

    return bind(params, options);
}

_NAN_METHOD_RETURN_TYPE
Query::bind(Local<Array>& params, Local<Object>& options)
{
    static PersistentString hints_str("hints");
    Local<Array> hints;
    if (! options.IsEmpty() && options->Has(hints_str)) {
        hints = options->Get(hints_str).As<Array>();
    }

    int bindingStatus = TypeMapper::bind_statement_params(statement_, params, hints);

    if (bindingStatus != -1) {
        char err[1024];
        sprintf(err, "error binding statement argument %d", bindingStatus);
        return NanThrowError(err);
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
    static PersistentString fetchSize("fetchSize");
    if (options->Has(fetchSize)) {
        paging_size = options->Get(fetchSize).As<Number>()->Uint32Value();
    }

    cass_statement_set_paging_size(statement_, paging_size);

    // If there's a result from the previous iteration, update the paging state
    // to fetch the next page and free it.
    if (result_.result()) {
        cass_statement_set_paging_state(statement_, result_.result());
        cass_result_free(result_.result());
    }

    CassFuture* future = cass_session_execute(session_, statement_);
    async_->schedule(on_result_ready, future, this, callback);

    NanReturnUndefined();
}

// Callback on the main v8 thread when results have been posted
void
Query::on_result_ready(CassFuture* future, void* client, void* data)
{
    Query* self = (Query*)client;
    NanCallback* callback = (NanCallback*) data;
    self->result_ready(future, callback);
}

void
Query::result_ready(CassFuture* future, NanCallback* callback)
{
    NanScope();

    fetching_ = false;
    result_.do_callback(future, callback);

    cass_future_free(future);
    delete callback;

    Unref();
}
