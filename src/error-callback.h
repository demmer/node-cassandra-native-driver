#ifndef __CASS_DRIVER_ERROR_CALLBACK_H__
#define __CASS_DRIVER_ERROR_CALLBACK_H__

/*
 * Call the given callback with a new error wrapping the msg extracted
 * from the given future.
 */
inline void error_callback(CassFuture* future, Nan::Callback* callback)
{
    Nan::HandleScope scope;

    const char* msg;
    size_t msg_len;
    cass_future_error_message(future, &msg, &msg_len);

    std::string err(msg, msg_len);

    Handle<Value> argv[] = {
        Nan::Error(err.c_str())
    };

    callback->Call(1, argv);
}

#endif
