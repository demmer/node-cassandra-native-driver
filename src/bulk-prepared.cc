#include <cassandra.h>

#include "bulk-prepared.h"
#include "client.h"
#include "persistent-string.h"
#include "prepared-query.h"
#include "query.h"
#include "type-mapper.h"

#define dprintf(...)
//#define dprintf printf

Persistent<Function> BulkPrepared::constructor;

void BulkPrepared::Init() {
    NanScope();

    // Prepare constructor template
    Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(New);
    tpl->SetClassName(NanNew("BulkPrepared"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    NODE_SET_PROTOTYPE_METHOD(tpl, "add", WRAPPED_METHOD_NAME(Add));
    NODE_SET_PROTOTYPE_METHOD(tpl, "done", WRAPPED_METHOD_NAME(Done));

    NanAssignPersistent(constructor, tpl->GetFunction());
}

Local<Object> BulkPrepared::NewInstance() {
    NanEscapableScope();

    const unsigned argc = 0;
    Local<Value> argv[argc] = {};
    Local<Function> cons = NanNew<Function>(constructor);
    Local<Object> instance = cons->NewInstance(argc, argv);

    return NanEscapeScope(instance);
}

NAN_METHOD(BulkPrepared::New) {
    NanEscapableScope();

    BulkPrepared* obj = new BulkPrepared();
    obj->Wrap(args.This());

    NanReturnValue(args.This());
}

BulkPrepared::BulkPrepared()
    : pending_(0),
      success_(0),
      callback_(NULL)
{

    uv_mutex_init(&lock_);

    async_ = new uv_async_t();
    uv_async_init(uv_default_loop(), async_, on_async_ready);
    async_->data = this;
}

static void
async_destroy(uv_handle_t* handle)
{
    uv_async_t* async = (uv_async_t*)handle;
    delete async;
}

BulkPrepared::~BulkPrepared()
{
    uv_mutex_destroy(&lock_);
    uv_close((uv_handle_t*)async_, async_destroy);
}

void
BulkPrepared::set_client(const Local<Object>& client)
{
    static PersistentString client_str("client");
    handle_->Set(client_str, client);

    Client* c = node::ObjectWrap::Unwrap<Client>(client);
    session_ = c->get_session();
}

WRAPPED_METHOD(BulkPrepared, Add)
{
    if (args.Length() < 2) {
        return NanThrowError("add_prepared requires prepared and vals");
    }

    if (callback_) {
        return NanThrowError("cannot add a query after done is called");
    }

    Local<Object> prepared_obj = args[0].As<Object>();
    if (! prepared_obj->IsObject() || prepared_obj->InternalFieldCount() == 0) {
        return NanThrowError("add_prepared requires a valid prepared object");
    }

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

    uv_mutex_lock(&lock_);
    pending_++;
    uv_mutex_unlock(&lock_);

    CassFuture* future = cass_session_execute(session_, statement);
    cass_future_set_callback(future, on_future_ready, this);

    NanReturnUndefined();
}

void
BulkPrepared::on_future_ready(CassFuture* future, void* data)
{
    BulkPrepared* self = (BulkPrepared*)data;
    self->future_ready(future);
}

void
BulkPrepared::future_ready(CassFuture* future)
{
    uv_mutex_lock(&lock_);

    CassError code = cass_future_error_code(future);
    if (code != CASS_OK) {
        CassString error = cass_future_error_message(future);
        std::string error_str = std::string(error.data, error.length);
        errors_.push_back(error_str);
    } else {
        success_++;
    }
    pending_--;

    // NOTE: check_if_done assumes the lock is held
    check_if_done();

    uv_mutex_unlock(&lock_);

    cass_future_free(future);
}

WRAPPED_METHOD(BulkPrepared, Done)
{
    NanScope();

    if (args.Length() != 1) {
        return NanThrowError("done requires 1 arguments: callback");
    }

    if (session_ == NULL) {
        return NanThrowError("client must be connected");
    }

    if (callback_) {
        return NanThrowError("cannot call done a second time");
    }

    // Need a reference while waiting
    Ref();

    uv_mutex_lock(&lock_);
    callback_ = new NanCallback(args[0].As<Function>());

    // NOTE: check_if_done assumes the lock is held
    check_if_done();

    uv_mutex_unlock(&lock_);

    NanReturnUndefined();
}

void
BulkPrepared::check_if_done()
{
    // NOTE: lock_ is already held
    if (callback_ == NULL || pending_ > 0) {
        return;
    }

    uv_async_send(async_);
}

void
BulkPrepared::on_async_ready(uv_async_t* handle, int status)
{
    BulkPrepared* self = (BulkPrepared*)handle->data;
    self->async_ready();
}

void
BulkPrepared::async_ready()
{
    NanScope();

    static PersistentString success_str("success");
    static PersistentString errors_str("errors");

    Local<Object> res = NanNew<Object>();
    res->Set(success_str, NanNew<Number>(success_));

    if (errors_.size() != 0) {
        Local<Array> errs = NanNew<Array>();
        for (size_t i = 0; i < errors_.size(); ++i) {
            errs->Set(i, NanNew<String>(errors_[i]));
        }
        res->Set(errors_str, errs);
    }

    Handle<Value> argv[] = {
        NanNull(),
        res
    };

    callback_->Call(2, argv);

    Unref();
}
