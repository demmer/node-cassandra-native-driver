
#include "logging.h"
#include "persistent-string.h"
#include <cassandra.h>
#include <vector>

using namespace v8;

std::vector<CassLogMessage> queue_;
uv_async_t* async_ = NULL;
uv_mutex_t lock_;
Nan::Callback* callback_;

void on_log_message(const CassLogMessage* message, void* data)
{
    uv_mutex_lock(&lock_);
    queue_.push_back(*message);
    uv_mutex_unlock(&lock_);
    uv_async_send(async_);
}

void
#if UV_VERSION_MAJOR == 0
async_ready(uv_async_t* handle, int status)
#else
async_ready(uv_async_t* handle)
#endif
{
    Nan::Scope scope;
    uv_mutex_lock(&lock_);

    for (size_t i = 0; i < queue_.size(); i++) {
        CassLogMessage* log = &queue_[i];
        Local<Object> info = Nan::New<Object>();

        static PersistentString time_ms("time_ms");
        static PersistentString severity("severity");
        static PersistentString message("message");

        Nan::Set(info, time_ms.handle(), Nan::New<Number>(log->time_ms));
        Nan::Set(info, severity.handle(), Nan::New<String>(cass_log_level_string(log->severity).ToLocalChecked()));
        Nan::Set(info, message.handle(), Nan::New<String>(log->message).ToLocalChecked());

        Handle<Value> argv[] = {
            Nan::Null(),
            info
        };
        callback_->Call(2, argv);
    }
    queue_.clear();

    uv_mutex_unlock(&lock_);
}

NAN_METHOD(SetLogCallback) {

    if (callback_) {
        return Nan::ThrowError("callback already registered");
    }

    if (info.Length() != 1) {
        return Nan::ThrowError("missing required argument: callback");
    }
    callback_ = new Nan::Callback(info[0].As<Function>());
    if (! callback_) {
        return Nan::ThrowError("callback is not a function");
    }

    uv_mutex_init(&lock_);

    async_ = new uv_async_t();
    uv_async_init(uv_default_loop(), async_, async_ready);

    cass_log_set_callback(on_log_message, NULL);
}

NAN_METHOD(SetLogLevel) {
    if (info.Length() != 1) {
        return Nan::ThrowError("missing required argument: level");
    }

    String::Utf8Value level_str(info[0].As<String>());

    CassLogLevel level = CASS_LOG_DISABLED;

    // The cassandra.h API includes a CASS_LOG_LEVEL_MAP macro that defines
    // the log level enum values and corresponding strings.
    //
    // Take advantage of that to verify and convert the level argument string.
    if (false) {}
#define CONVERT(_level, _str) \
    else if (!strcasecmp(*level_str, _str)) { \
        level = _level; \
    }
    CASS_LOG_LEVEL_MAP(CONVERT)
    else {
        return Nan::ThrowError("invalid level");
    }

    cass_log_set_level(level);
}
