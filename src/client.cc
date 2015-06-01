
#include "client.h"
#include "batch.h"
#include "error-callback.h"
#include "persistent-string.h"
#include "prepared-query.h"
#include "query.h"
#include "string.h"

using namespace v8;

Persistent<Function> Client::constructor;

Client::Client()
    : metrics_(),
      async_(&metrics_)
{
    cluster_ = cass_cluster_new();
    session_ = cass_session_new();
}

Client::~Client() {
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
    NODE_SET_PROTOTYPE_METHOD(tpl, "new_query", WRAPPED_METHOD_NAME(NewQuery));
    NODE_SET_PROTOTYPE_METHOD(tpl, "new_prepared_query", WRAPPED_METHOD_NAME(NewPreparedQuery));
    NODE_SET_PROTOTYPE_METHOD(tpl, "new_batch", WRAPPED_METHOD_NAME(NewBatch));
    NODE_SET_PROTOTYPE_METHOD(tpl, "metrics", WRAPPED_METHOD_NAME(GetMetrics));

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

    if (!args.IsConstructCall()) {
        return NanThrowError("non-constructor invocation not supported");
    }

    Client* obj = new Client();
    obj->Wrap(args.This());

    if (args.Length() == 1) {
        Local<Object> opts = args[0].As<Object>();
        obj->configure(opts);
    }

    NanReturnValue(args.This());
}

void
Client::configure(v8::Local<v8::Object> opts)
{
    static PersistentString keepalive_str("tcp_keepalive_delay");
    const Local<Array> props = opts->GetPropertyNames();
    const uint32_t length = props->Length();
    for (uint32_t i = 0; i < length; ++i)
    {
        const Local<Value> key = props->Get(i);
        const String::AsciiValue key_str(key);
        unsigned value = opts->Get(key)->Int32Value();

#define SET(_var) \
    if (strcmp(*key_str, #_var) == 0) { \
        cass_cluster_set_ ## _var(cluster_, value); \
    }

        SET(num_threads_io)
        SET(queue_size_io)
        SET(queue_size_event)
        SET(queue_size_log)
        SET(core_connections_per_host)
        SET(max_connections_per_host)
        SET(reconnect_wait_time)
        SET(max_concurrent_creation)
        SET(max_concurrent_requests_threshold)
        SET(max_requests_per_flush)
        SET(write_bytes_high_water_mark)
        SET(write_bytes_low_water_mark)
        SET(pending_requests_high_water_mark)
        SET(pending_requests_low_water_mark)
        SET(connect_timeout)
        SET(request_timeout)

        if (strcmp(*key_str, "tcp_keepalive") == 0) {
            if (value == 0) {
                cass_cluster_set_tcp_keepalive(cluster_, cass_false, value);
            } else if (opts->Has(keepalive_str)) {
                unsigned delay = opts->Get(keepalive_str)->Int32Value();
                cass_cluster_set_tcp_keepalive(cluster_, cass_true, delay);
            }
        }

        if (strcmp(*key_str, "tcp_nodelay") == 0) {
            if (value == 0) {
                cass_cluster_set_tcp_nodelay(cluster_, cass_false);
            } else {
                cass_cluster_set_tcp_nodelay(cluster_, cass_true);
            }
        }
    }
}

WRAPPED_METHOD(Client, Connect) {
    NanScope();

    if (args.Length() != 2) {
        return NanThrowError("connect requires 2 arguments: options and callback");
    }

    Local<Object> options = args[0].As<Object>();
    static PersistentString address_str("address");
    static PersistentString port_str("port");

    int port;

    if (options->Has(address_str)) {
        String::AsciiValue address(options->Get(address_str).As<String>());
        cass_cluster_set_contact_points(cluster_, *address);
    } else {
        return NanThrowError("connect requires a address");
    }

    if (options->Has(port_str)) {
        port = options->Get(port_str).As<Number>()->Int32Value();
        cass_cluster_set_port(cluster_, port);
    }

    NanCallback* callback = new NanCallback(args[1].As<Function>());

    CassFuture* future = cass_session_connect(session_, cluster_);
    async_.schedule(on_connected, future, this, callback);

    Ref();
    NanReturnUndefined();
}

void
Client::on_connected(CassFuture* future, void* client, void* data)
{
    Client* self = (Client*) client;
    NanCallback* callback = (NanCallback*) data;
    self->connected(future, callback);
}

void
Client::connected(CassFuture* future, NanCallback* callback)
{
    NanScope();

    CassError code = cass_future_error_code(future);
    if (code != CASS_OK) {
        error_callback(future, callback);
    } else {
        Handle<Value> argv[] = {
            NanNull(),
        };
        callback->Call(1, argv);
    }
    cass_future_free(future);
    delete callback;

    Unref();
}

WRAPPED_METHOD(Client, NewQuery) {
    NanScope();
    Local<Value> val = Query::NewInstance();

    Query* query = node::ObjectWrap::Unwrap<Query>(val->ToObject());

    Local<Object> self = Local<Object>::New(handle_);
    query->set_client(self);

    NanReturnValue(val);
}

WRAPPED_METHOD(Client, NewPreparedQuery) {
    NanScope();
    Local<Value> val = PreparedQuery::NewInstance();

    PreparedQuery* query = node::ObjectWrap::Unwrap<PreparedQuery>(val->ToObject());

    Local<Object> self = Local<Object>::New(handle_);
    query->set_client(self);

    NanReturnValue(val);
}

WRAPPED_METHOD(Client, NewBatch) {
    NanScope();

    if (args.Length() != 1) {
        return NanThrowError("must specify batch type");
    }

    Local<String> type = args[0].As<String>();
    Local<Value> val = Batch::NewInstance(type);
    if (! val.IsEmpty()) {
        Batch* batch = node::ObjectWrap::Unwrap<Batch>(val->ToObject());

        Local<Object> self = Local<Object>::New(handle_);
        batch->set_client(self);
    }

    NanReturnValue(val);
}

WRAPPED_METHOD(Client, GetMetrics) {
    NanScope();

    bool reset = false;
    if (args.Length() == 1) {
        reset = args[0]->IsTrue();
    }

    Local<Object> metrics = metrics_.get();
    if (reset) {
        metrics_.clear();
    }

    CassMetrics driver_metrics;
    cass_session_get_metrics(session_, &driver_metrics);

#define X(_name, _group, _metric) \
    PersistentString _group ## __ ## _metric ## __str(_name); \
    metrics->Set(_group ## __ ## _metric ## __str, NanNew<Number>(driver_metrics._group._metric));

    X("request_latency_min", requests, min);
    X("request_latency_max", requests, max);
    X("request_latency_avg", requests, mean);
    X("request_latency_stddev", requests, stddev);
    X("request_latency_median", requests, median);
    X("request_latency_p75", requests, percentile_75th);
    X("request_latency_p95", requests, percentile_95th);
    X("request_latency_p98", requests, percentile_98th);
    X("request_latency_p99", requests, percentile_99th);
    X("request_latency_p999", requests, percentile_999th);

    X("request_mean_rate", requests, mean_rate);
    X("request_1min_rate", requests, one_minute_rate);
    X("request_5min_rate", requests, five_minute_rate);
    X("request_15min_rate", requests, fifteen_minute_rate);

    X("total_connections", stats, total_connections);
    X("available_connections", stats, available_connections);
    X("exceeded_pending_requests_limit", stats, exceeded_pending_requests_water_mark);
    X("exceeded_write_bytes_limit", stats, exceeded_write_bytes_water_mark);

    X("connection_timeouts", errors, connection_timeouts);
    X("pending_request_timeouts", errors, pending_request_timeouts);
    X("request_timeouts", errors, request_timeouts);

#undef X
    NanReturnValue(metrics);
}
