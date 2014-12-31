
#include "client.h"
#include "type-mapper.h"

using namespace v8;

Persistent<Function> Client::constructor;

Client::Client() {
    printf("In Client constructor %p\n", this);
    cluster_ = cass_cluster_new();
    session_ = cass_session_new();
}

Client::~Client() {
    printf("In Client destructor %p\n", this);
    cass_session_free(session_);
    cass_cluster_free(cluster_);
}

void Client::Init() {
    NanScope();

    // Prepare constructor template
    Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(New);
    tpl->SetClassName(NanNew("Client"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    // Prototype
    NODE_SET_PROTOTYPE_METHOD(tpl, "connect", WRAPPED_METHOD_NAME(Connect));
    NODE_SET_PROTOTYPE_METHOD(tpl, "query", WRAPPED_METHOD_NAME(Query));

    NanAssignPersistent(constructor, tpl->GetFunction());
}

Local<Object> Client::NewInstance(Local<Value> arg) {
    NanEscapableScope();

    const unsigned argc = 1;
    Local<Value> argv[argc] = { arg };
    Local<Function> cons = NanNew<Function>(constructor);
    Local<Object> instance = cons->NewInstance(argc, argv);

    return NanEscapeScope(instance);
}

NAN_METHOD(Client::New) {
    NanScope();

    if (args.IsConstructCall()) {
        // Invoked as constructor: `new Client(...)`
        Client* obj = new Client();
        obj->Wrap(args.This());
        NanReturnValue(args.This());
    } else {
        // Invoked as plain function `Client(...)`, turn into construct call.
        const int argc = 1;
        Local<Value> argv[argc] = { args[0] };
        Local<Function> cons = NanNew<Function>(constructor);
        NanReturnValue(cons->NewInstance(argc, argv));
    }
}

WRAPPED_METHOD(Connect) {
    NanScope();

    // XXX/demmer:
    // 1. make this async
    // 2. parameterize contact points and other options
    cass_cluster_set_contact_points(cluster_, "127.0.0.1");

    CassFuture* future = cass_session_connect(session_, cluster_);
    cass_future_wait(future);

    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        CassString message = cass_future_error_message(future);
        std::string msg(message.data, message.length);
        cass_future_free(future);
        return NanThrowError(msg.c_str());
    }
    cass_future_free(future);

    NanReturnUndefined();
}
Client::QueryState::QueryState(Client* client)
{
    client_ = client;
    uv_mutex_init(&lock_);
    uv_async_init(uv_default_loop(), &async_, Client::on_async_ready);
    async_.data = this;
    statement_ = NULL;
    callback_ = NULL;
}

Client::QueryState::~QueryState()
{
    uv_mutex_destroy(&lock_);
    uv_close((uv_handle_t*)&async_, NULL);
    cass_statement_free(statement_);
    delete callback_;

    for (size_t i = 0; i < column_info_.size(); ++i) {
        column_info_[i].name_.Dispose();
    }
    column_info_.empty();
}

WRAPPED_METHOD(Query) {
    NanScope();

    if (args.Length() != 4) {
        return NanThrowError("query requires 4 arguments: query, params, options, callback");
    }

    QueryState* state = new QueryState(this);

    String::AsciiValue query_str(args[0].As<String>());
    Local<Array> params = args[1].As<Array>();
    Local<Object> options = args[2].As<Object>();

    state->callback_ = new NanCallback(args[3].As<Function>());

    CassString query = cass_string_init(*query_str);
    state->statement_ = cass_statement_new(query, params->Length());

    for (u_int32_t i = 0; i < params->Length(); ++i) {
        Local<Value> arg = params->Get(i);
        if (! TypeMapper::bind_statement_param(state->statement_, i, arg)) {
            delete state;
            return NanThrowError("unable to convert statement argument");
        }
    }

    u_int32_t paging_size = 5000;
    Local<String> fetchSize = NanNew("fetchSize");
    if (options->Has(fetchSize)) {
        paging_size = options->Get(fetchSize)->Uint32Value();
    }
    printf("using paging size of %u\n", paging_size);
    cass_statement_set_paging_size(state->statement_, paging_size);

    CassFuture* future = cass_session_execute(session_, state->statement_);
    cass_future_set_callback(future, Client::on_result_ready, state);

    Ref();

    NanReturnUndefined();
}

// Callback on the I/O thread when a result is ready from cassandra
void
Client::on_result_ready(CassFuture* future, void* data)
{
    QueryState* state = (QueryState*)data;
    state->client_->result_ready(future, state);
}

void
Client::result_ready(CassFuture* future, QueryState* state)
{
    Result result;
    result.result_ = cass_future_get_result(future);
    result.code_ = cass_future_error_code(future);
    if (result.code_ == CASS_OK) {
        result.more_ = cass_result_has_more_pages(result.result_);
    } else {
        CassString error = cass_future_error_message(future);
        result.error_ = std::string(error.data, error.length);
        cass_future_free(future);
        result.more_ = cass_false;
    }


    uv_mutex_lock(&state->lock_);
    state->results_.push(result);
    uv_mutex_unlock(&state->lock_);

    uv_async_send(&state->async_);

    if (result.more_) {
        cass_statement_set_paging_state(state->statement_, result.result_);
        CassFuture* future = cass_session_execute(session_, state->statement_);
        cass_future_set_callback(future, on_result_ready, state);
        cass_future_free(future);
    }
}

// Callback on the main v8 thread when results have been posted
void
Client::on_async_ready(uv_async_t* handle, int status)
{
    // XXX/demmer status?
    QueryState* state = (QueryState*)handle->data;
    state->client_->async_ready(state);
}

void
Client::async_ready(QueryState* state)
{
    cass_bool_t has_more_pages = cass_true;
    uv_mutex_lock(&state->lock_);
    while (!state->results_.empty()) {
        Result& result = state->results_.front();
        result_callback(result, state);
        has_more_pages = result.more_;
        cass_result_free(result.result_);
        state->results_.pop();
    }
    uv_mutex_unlock(&state->lock_);

    if (!has_more_pages) {
        printf("query done\n");
//        delete state;
        Unref();
    }
}

void
Client::result_callback(const Result& result, QueryState* state) {
    NanScope();

    if (result.code_ != CASS_OK) {
        Handle<Value> argv[] = {
            NanError(result.error_.c_str())
        };
        state->callback_->Call(1, argv);
        return;
    }

    Local<Array> data = NanNew<Array>();
    CassIterator* iterator = cass_iterator_from_result(result.result_);

    // Stash the column info for the first batch of results.
    size_t num_columns = cass_result_column_count(result.result_);
    if (state->column_info_.size() == 0) {
        for (size_t i = 0; i < num_columns; ++i) {
            CassString name = cass_result_column_name(result.result_, i);
            CassValueType type = cass_result_column_type(result.result_, i);
            state->column_info_.push_back(Column(name, type));
        }
    }

    size_t n = 0;
    while (cass_iterator_next(iterator)) {
        const CassRow* row = cass_iterator_get_row(iterator);
        Local<Object> element = NanNew<Object>();

        for (size_t i = 0; i < num_columns; ++i) {
            Local<Value> value;
            if (TypeMapper::column_value(&value, state->column_info_[i].type_,
                                         row, i, &state->buffer_pool_)) {
                element->Set(state->column_info_[i].name_, value);
            } else {
                Handle<Value> argv[] = {
                    NanError("unable to obtain column value")
                };
                state->callback_->Call(1, argv);
                return;
            }
        }
        data->Set(n, element);
        n++;
    }
    cass_iterator_free(iterator);

    Handle<Value> argv[] = {
        NanNull(),
        data,
        result.more_ ? NanTrue() : NanFalse()
    };
    state->callback_->Call(3, argv);
}
