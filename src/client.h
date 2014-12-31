#ifndef __CASS_DRIVER_CLIENT_H__
#define __CASS_DRIVER_CLIENT_H__

#include "cassandra.h"
#include "node.h"
#include "nan.h"
#include "wrapped-method.h"
#include "buffer-pool.h"
#include <queue>
#include <vector>

class Client : public node::ObjectWrap {
public:
    static void Init();
    static v8::Local<v8::Object> NewInstance(v8::Local<v8::Value> arg);

private:
    CassCluster* cluster_;
    CassSession* session_;

    // Encapsulation of a future result that can be queued and passed to the
    // main thread for processing.
    struct Result {
        Result() : code_(CASS_ERROR_LIB_NOT_IMPLEMENTED), error_(), result_(NULL) {}

        CassError code_;
        std::string error_;
        const CassResult* result_;
        cass_bool_t more_;
    };
    typedef std::queue<Result> ResultQueue;

    // Encapsulation of column metadata that can be cached for each row in the
    // results.
    struct Column {
        Column(CassString name, CassValueType type) {
            NanAssignPersistent(name_, NanNew(name.data));
            type_ = type;
        };

        v8::Persistent<v8::String> name_;
        CassValueType type_;
    };
    typedef std::vector<Column> ColumnInfo;

    // Structure to wrap an in-progress query to the back end
    struct QueryState {
        QueryState(Client* client);
        ~QueryState();

        Client* client_;
        CassStatement* statement_;
        NanCallback* callback_;

        uv_mutex_t lock_;
        ResultQueue results_;

        uv_async_t async_;

        BufferPool buffer_pool_;
        ColumnInfo column_info_;
    };

    explicit Client();
    ~Client();

    static NAN_METHOD(New);

    WRAPPED_METHOD_DECL(Connect);
    WRAPPED_METHOD_DECL(Query);

    static void on_async_ready(uv_async_t* handle, int status);
    void async_ready(QueryState* state);

    static void on_result_ready(CassFuture* future, void* data);
    void result_ready(CassFuture* future, QueryState* state);
    void result_callback(const Result& result, QueryState* state);

    bool bind_param(CassStatement* statement, u_int32_t i, v8::Local<v8::Value>& val);
    bool get_column(const CassRow* row, size_t i, v8::Local<v8::Object>& element, QueryState* state);

    static v8::Persistent<v8::Function> constructor;
};

#endif
