#include <cassandra.h>

#include "result.h"
#include "error-callback.h"
#include "persistent-string.h"
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
        error_callback(future, callback);
        return;
    }

    result_ = cass_future_get_result(future);

    Local<Object> res = NanNew<Object>();

    static PersistentString more_str("more");
    cass_bool_t more = cass_result_has_more_pages(result_);
    res->Set(more_str, more ? NanTrue() : NanFalse() );

    static PersistentString rows_str("rows");
    Local<Array> data = NanNew<Array>();
    res->Set(rows_str, data);
    CassIterator* iterator = cass_iterator_from_result(result_);

    // Stash the column info for the first batch of results.
    size_t num_columns = cass_result_column_count(result_);
    if (column_info_.size() == 0) {
        for (size_t i = 0; i < num_columns; ++i) {
            const char* name;
            size_t name_length;
            cass_result_column_name(result_, i, &name, &name_length);
            CassValueType type = cass_result_column_type(result_, i);
            column_info_.push_back(Column(name, name_length, type));
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
