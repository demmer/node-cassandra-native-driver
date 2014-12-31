#ifndef _CASSANDRA_NATIVE_WRAPPED_METHOD_H_
#define _CASSANDRA_NATIVE_WRAPPED_METHOD_H_

#define WRAPPED_METHOD_NAME(_name) JS##_name

#define WRAPPED_METHOD_DECL(_name) \
    static NAN_METHOD(WRAPPED_METHOD_NAME(_name)); \
    NAN_METHOD(_name);

#define WRAPPED_METHOD(_name) \
    NAN_METHOD(Client::WRAPPED_METHOD_NAME(_name)) { \
        Client* obj = ObjectWrap::Unwrap<Client>(args.Holder()); \
        return obj->_name(args); \
    } \
    NAN_METHOD(Client::_name)

#endif
