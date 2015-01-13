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

void
Result::do_callback(CassFuture* future, NanCallback* callback)
{
    NanScope();

    CassError code = cass_future_error_code(future);
    if (code != CASS_OK) {
        CassString error = cass_future_error_message(future);
        std::string error_str = std::string(error.data, error.length);

        Handle<Value> argv[] = {
            NanError(error_str.c_str())
        };
        callback->Call(1, argv);
        return;
    }

    result_ = cass_future_get_result(future);

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
            Local<Value> result;
            const CassValue* value = cass_row_get_column(row, i);
            if (TypeMapper::v8_from_cassandra(&result, column_info_[i].type_,
                                              value, &buffer_pool_))
            {
                element->Set(column_info_[i].name_, result);
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
