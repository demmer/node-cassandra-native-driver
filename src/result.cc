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
        column_info_[i]->name_.Reset();
        delete column_info_[i];
    }
    column_info_.empty();
}

void
Result::do_callback(CassFuture* future, Nan::Callback* callback)
{
    Nan::HandleScope scope;

    CassError code = cass_future_error_code(future);
    if (code != CASS_OK) {
        error_callback(future, callback);
        return;
    }

    result_ = cass_future_get_result(future);

    Local<Object> res = Nan::New<Object>();

    static PersistentString more_str("more");
    cass_bool_t more = cass_result_has_more_pages(result_);
    Nan::Set(res, more_str, more ? Nan::True() : Nan::False() );

    static PersistentString rows_str("rows");
    Local<Array> data = Nan::New<Array>();
    Nan::Set(res, rows_str, data);
    CassIterator* iterator = cass_iterator_from_result(result_);

    // Stash the column info for the first batch of results.
    size_t num_columns = cass_result_column_count(result_);
    if (column_info_.size() == 0) {
        for (size_t i = 0; i < num_columns; ++i) {
            const char* name;
            size_t name_length;
            cass_result_column_name(result_, i, &name, &name_length);
            CassValueType type = cass_result_column_type(result_, i);
            column_info_.push_back(new Column(name, name_length, type));
        }
    }

    size_t n = 0;
    while (cass_iterator_next(iterator)) {
        const CassRow* row = cass_iterator_get_row(iterator);
        Local<Object> element = Nan::New<Object>();
        for (size_t i = 0; i < num_columns; ++i) {
            Local<Value> result;
            const CassValue* value = cass_row_get_column(row, i);
            if (TypeMapper::v8_from_cassandra(&result, column_info_[i]->type_, value))
            {
                Nan::Set(element, Nan::New(column_info_[i]->name_), result);
            }
            else
            {
                // XXX temporary until all the types are implemented in
                // TypeMapper.
                Local<Value> argv[] = {
                    Nan::Error("unable to obtain column value")
                };
                callback->Call(1, argv);
                return;
            }
        }

        Nan::Set(data, n, element);
        n++;
    }
    cass_iterator_free(iterator);

    Local<Value> argv[] = {
        Nan::Null(),
        res
    };

    callback->Call(2, argv);
}
