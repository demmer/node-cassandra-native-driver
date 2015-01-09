#include <cassandra.h>

#include "query.h"
#include "client.h"
#include "type-mapper.h"

Persistent<Function> Query::constructor;

void Query::Init() {
    NanScope();

    // Prepare constructor template
    Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(New);
    tpl->SetClassName(NanNew("Query"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

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

Query::Query()
{
    fetching_ = false;
    result_ = NULL;
    callback_ = NULL;

    uv_async_init(uv_default_loop(), &async_, Query::on_async_ready);
    async_.data = this;
}

Query::~Query()
{
    printf("Query::~Query\n");

    if (result_) {
        cass_result_free(result_);
    }
    cass_statement_free(statement_);
    if (callback_) {
        delete callback_;
    }

    for (size_t i = 0; i < column_info_.size(); ++i) {
        column_info_[i].name_.Dispose();
    }
    column_info_.empty();
    uv_close((uv_handle_t*)&async_, NULL);
}

void
Query::set_client(v8::Persistent<v8::Object>& client)
{
    client_ = client;
    session_ = node::ObjectWrap::Unwrap<Client>(client->ToObject())->get_session();
}

WRAPPED_METHOD(Query, Bind)
{
    NanScope();

    if (args.Length() != 2) {
        return NanThrowError("bind requires 2 arguments: query, params");
    }

    Local<String> query = args[0].As<String>();
    Local<Array> params = args[1].As<Array>();

    String::AsciiValue query_str(query);
    statement_ = cass_statement_new(cass_string_init(*query_str), params->Length());

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

    // Guard against running fetch multiple times in parallel
    if (fetching_) {
        return NanThrowError("fetch already in progress");
    }
    fetching_ = true;

    Local<Object> options = args[0].As<Object>();
    NanCallback* callback = new NanCallback(args[1].As<Function>());

    u_int32_t paging_size = 5000;
    Local<String> fetchSize = NanNew("fetchSize");
    if (options->Has(fetchSize)) {
        paging_size = options->Get(fetchSize).As<Number>()->Uint32Value();
    }

    cass_statement_set_paging_size(statement_, paging_size);

    // Need a reference while the operation is in progress
    Ref();

    callback_ = callback;
    fetching_ = true;

    // If there's a result from the previous iteration, update the paging state
    // to fetch the next page and free it.
    if (result_) {
        cass_statement_set_paging_state(statement_, result_);
        cass_result_free(result_);
    }

    CassFuture* future = cass_session_execute(session_, statement_);
    cass_future_set_callback(future, on_result_ready, this);
    NanReturnUndefined();
}

// Callback on the I/O thread when a result is ready from cassandra
void
Query::on_result_ready(CassFuture* future, void* data)
{
    Query* self = (Query*)data;
    self->result_ready(future);
}

void
Query::result_ready(CassFuture* future)
{
    fetching_ = false;
    result_ = cass_future_get_result(future);
    result_code_ = cass_future_error_code(future);
    if (result_code_ != CASS_OK) {
        CassString error = cass_future_error_message(future);
        result_error_ = std::string(error.data, error.length);
    }
    cass_future_free(future);

    uv_async_send(&async_);
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

    if (result_code_ != CASS_OK) {
        Handle<Value> argv[] = {
            NanError(result_error_.c_str())
        };
        callback_->Call(1, argv);
        return;
    }

    // Stash a reference to the query in the result object, then release the
    // reference that was held during the fetch to avoid leaking.
    Local<Array> res = NanNew<Array>();

    cass_bool_t more = cass_result_has_more_pages(result_);
    res->Set(NanNew("more"), more ? NanTrue() : NanFalse() );

    Local<Array> data = NanNew<Array>();
    res->Set(NanNew("rows"), data);
    CassIterator* iterator = cass_iterator_from_result(result_);

    // Stash the column info for the first batch of results.
    size_t num_columns = cass_result_column_count(result_);
    if (column_info_.size() == 0) {
        for (size_t i = 0; i < num_columns; ++i) {
            CassString name = cass_result_column_name(result_, i);
            CassValueType type = cass_result_column_type(result_, i);
            column_info_.push_back(Column(name, type));
        }
    }

    size_t n = 0;
    while (cass_iterator_next(iterator)) {
        const CassRow* row = cass_iterator_get_row(iterator);
        Local<Object> element = NanNew<Object>();
        for (size_t i = 0; i < num_columns; ++i) {
            Local<Value> value;
            const CassValue* rowValue = cass_row_get_column(row, i);
            if (TypeMapper::v8_from_cassandra(&value, column_info_[i].type_,
                                         rowValue, &buffer_pool_))
            {
                element->Set(column_info_[i].name_, value);
            }
            else
            {
                // XXX temporary until all the types are implemented in
                // TypeMapper.
                Handle<Value> argv[] = {
                    NanError("unable to obtain column value")
                };
                callback_->Call(1, argv);
                return;
            }
        }

        data->Set(n, element);
        n++;
    }
    cass_iterator_free(iterator);

    Handle<Value> argv[] = {
        NanNull(),
        res
    };
    callback_->Call(2, argv);
}
