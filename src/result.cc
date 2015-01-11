#include <cassandra.h>

#include "result.h"
#include "type-mapper.h"

Result::Result()
{
    result_ = NULL;
}

Result::~Result()
{
    if (result_) {
        cass_result_free(result_);
    }

    for (size_t i = 0; i < column_info_.size(); ++i) {
        column_info_[i].name_.Dispose();
    }
    column_info_.empty();
}

// Callback on the I/O thread when a result is ready from cassandra
void
Result::on_ready(CassFuture* future, void* data)
{
    Result* self = (Result*)data;
    self->ready(future);
}

void
Result::ready(CassFuture* future)
{
    result_ = cass_future_get_result(future);
    result_code_ = cass_future_error_code(future);
    if (result_code_ != CASS_OK) {
        CassString error = cass_future_error_message(future);
        result_error_ = std::string(error.data, error.length);
    }
    cass_future_free(future);

    uv_async_send(async_);
}

void
Result::do_callback(NanCallback* callback)
{
    NanScope();

    if (result_code_ != CASS_OK) {
        Handle<Value> argv[] = {
            NanError(result_error_.c_str())
        };
        callback->Call(1, argv);
        return;
    }

    Local<Array> res = NanNew<Array>();
    fetching_ = false;

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
            if (TypeMapper::column_value(&value, column_info_[i].type_,
                                         row, i, &buffer_pool_))
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
                callback->Call(1, argv);
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

    callback->Call(2, argv);
}
