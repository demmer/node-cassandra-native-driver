
#include "client.h"
#include "batch.h"
#include "error-callback.h"
#include "persistent-string.h"
#include "prepared-query.h"
#include "query.h"
#include "string.h"

using namespace v8;

Nan::Persistent<Function> Client::constructor;

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
    Nan::HandleScope scope;

    // Prepare constructor template
    Local<FunctionTemplate> tpl = Nan::New<FunctionTemplate>(New);
    tpl->SetClassName(Nan::New("Client").ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    // Prototype
    Nan::SetPrototypeMethod(tpl, "connect", WRAPPED_METHOD_NAME(Connect));
    Nan::SetPrototypeMethod(tpl, "new_query", WRAPPED_METHOD_NAME(NewQuery));
    Nan::SetPrototypeMethod(tpl, "new_prepared_query", WRAPPED_METHOD_NAME(NewPreparedQuery));
    Nan::SetPrototypeMethod(tpl, "new_batch", WRAPPED_METHOD_NAME(NewBatch));
    Nan::SetPrototypeMethod(tpl, "metrics", WRAPPED_METHOD_NAME(GetMetrics));

    constructor.Reset(tpl->GetFunction());

}

Local<Object> Client::NewInstance(Local<Value> arg) {
    Nan::EscapableHandleScope scope;

    const unsigned argc = 1;
    Local<Value> argv[argc] = { arg };
    Local<Function> cons = Nan::New<Function>(constructor);
    Local<Object> instance = Nan::NewInstance(cons).ToLocalChecked();

    return scope.Escape(instance);
}

NAN_METHOD(Client::New) {

    if (!info.IsConstructCall()) {
        return Nan::ThrowError("non-constructor invocation not supported");
    }

    Client* obj = new Client();
    obj->Wrap(info.This());

    if (info.Length() == 1) {
        Local<Object> opts = info[0].As<Object>();
        obj->configure(opts);
    }

    info.GetReturnValue().Set(info.This());
}

void
Client::configure(v8::Local<v8::Object> opts)
{
    static PersistentString keepalive_str("tcp_keepalive_delay");
    const Local<Array> props = Nan::GetPropertyNames(opts).ToLocalChecked();
    const uint32_t length = props->Length();
    for (uint32_t i = 0; i < length; ++i)
    {
        const Local<Value> key = Nan::Get(props, i).ToLocalChecked();
        const v8::String::Utf8Value key_str(key);
        unsigned value = Nan::Get(opts, key).ToLocalChecked()->Int32Value();

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
            } else if (Nan::Has(opts, keepalive_str).FromJust()) {
                unsigned delay = Nan::Get(opts, keepalive_str).ToLocalChecked()->Int32Value();
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
    Nan::HandleScope scope;

    if (info.Length() != 2) {
        return Nan::ThrowError("connect requires 2 arguments: options and callback");
    }

    Local<Object> options = info[0].As<Object>();
    static PersistentString address_str("address");
    static PersistentString port_str("port");

    int port;

    if (Nan::Has(options, address_str).FromJust()) {
        String::Utf8Value address(Nan::Get(options, address_str).ToLocalChecked());
        cass_cluster_set_contact_points(cluster_, *address);
    } else {
        return Nan::ThrowError("connect requires a address");
    }

    if (Nan::Has(options, port_str).FromJust()) {
        port = Nan::Get(options, port_str).ToLocalChecked()->Int32Value();
        cass_cluster_set_port(cluster_, port);
    }

    Nan::Callback* callback = new Nan::Callback(info[1].As<Function>());

    CassFuture* future = cass_session_connect(session_, cluster_);
    async_.schedule(on_connected, future, this, callback);

    Ref();
    return;
}

void
Client::on_connected(CassFuture* future, void* client, void* data)
{
    Client* self = (Client*) client;
    Nan::Callback* callback = (Nan::Callback*) data;
    self->connected(future, callback);
}

void
Client::connected(CassFuture* future, Nan::Callback* callback)
{
    Nan::HandleScope scope;

    CassError code = cass_future_error_code(future);
    if (code != CASS_OK) {
        error_callback(future, callback);
    } else {
        Handle<Value> argv[] = {
            Nan::Null(),
        };
        callback->Call(1, argv);
    }
    cass_future_free(future);
    delete callback;

    Unref();
}

WRAPPED_METHOD(Client, NewQuery) {
    Nan::HandleScope scope;
    Local<Value> val = Query::NewInstance();

    Query* query = Nan::ObjectWrap::Unwrap<Query>(val->ToObject());
    query->set_client(this->handle());

    info.GetReturnValue().Set(val);
}

WRAPPED_METHOD(Client, NewPreparedQuery) {
    Nan::HandleScope scope;
    Local<Value> val = PreparedQuery::NewInstance();

    PreparedQuery* query = Nan::ObjectWrap::Unwrap<PreparedQuery>(val->ToObject());
    query->set_client(this->handle());

    info.GetReturnValue().Set(val);
}

WRAPPED_METHOD(Client, NewBatch) {
    Nan::HandleScope scope;

    if (info.Length() != 1) {
        return Nan::ThrowError("must specify batch type");
    }

    Local<String> type = info[0].As<String>();
    Local<Value> val = Batch::NewInstance(type);
    if (! val.IsEmpty()) {
        Batch* batch = Nan::ObjectWrap::Unwrap<Batch>(val->ToObject());
        batch->set_client(this->handle());
    }

    info.GetReturnValue().Set(val);
}

WRAPPED_METHOD(Client, GetMetrics) {
    Nan::HandleScope scope;

    bool reset = false;
    if (info.Length() == 1) {
        reset = info[0]->IsTrue();
    }

    v8::Local<v8::Object> metrics = Nan::New<v8::Object>();
    metrics_.get(metrics);
    if (reset) {
        metrics_.clear();
    }

    CassMetrics driver_metrics;
    cass_session_get_metrics(session_, &driver_metrics);

#define X(_name, _group, _metric) \
    PersistentString _group ## __ ## _metric ## __str(_name); \
    Nan::Set(metrics, _group ## __ ## _metric ## __str, Nan::New<Number>(driver_metrics._group._metric));

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
    info.GetReturnValue().Set(metrics);
}
