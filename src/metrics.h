#ifndef __CASS_DRIVER_METRICS_H__
#define __CASS_DRIVER_METRICS_H__

#include "nan.h"
#include "persistent-string.h"

class Metrics {
public:
    Metrics() { clear(); }

    // Reset (zero-out) the current metrics
    void clear();

    // Retrieve the current metrics as a v8 object
    void get(v8::Local<v8::Object> metrics);

    // Increment the counter(s) for a new request;
    void start_request();

    // Decrement the counter(s) for a new request;
    void stop_request();

    uint32_t request_count_;
    uint32_t response_count_;
    uint32_t pending_request_count_max_;
    uint32_t response_queue_drain_count_max_;
    uint32_t response_queue_drain_time_max_;
};

inline void
Metrics::clear()
{
    request_count_ = 0;
    response_count_ = 0;
    pending_request_count_max_ = 0;
    response_queue_drain_count_max_ = 0;
    response_queue_drain_time_max_ = 0;
}

inline void
Metrics::start_request()
{
    request_count_++;
    uint32_t pending = request_count_ - response_count_;
    if (pending > pending_request_count_max_) {
        pending_request_count_max_ = pending;
    }
}

inline void
Metrics::stop_request()
{
    response_count_++;
}

inline void
Metrics::get(v8::Local<v8::Object> metrics)
{
#define GET(x) \
    static PersistentString x##str(#x); \
    Nan::Set(metrics, x##str, Nan::New(x##_));

    GET(request_count);
    GET(response_count);
    GET(pending_request_count_max);
    GET(response_queue_drain_count_max);
    GET(response_queue_drain_time_max);

#undef GET
}

#endif
