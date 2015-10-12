# Index

* [Client](#client)
* [Query](#query)
* [Prepared](#prepared)
* [Batch](#batch)
* [Types](#types)
* [Logging](#logging)

# <a name="client"></a> Client

## new Client(options)

Creates a new instantiation of the driver to connect to a given cluster. This is the primary entry point into the system.

Options exposes a number of configuration options used to tune the connection to the cluster. Detailed descriptions for the various values can be found in the C++ driver documentation:

* num_threads_io
* queue_size_io
* queue_size_event
* queue_size_log
* core_connections_per_host
* max_connections_per_host
* reconnect_wait_time
* max_concurrent_creation
* max_concurrent_requests_threshold
* max_requests_per_flush
* write_bytes_high_water_mark
* write_bytes_low_water_mark
* pending_requests_high_water_mark
* pending_requests_low_water_mark
* connect_timeout
* request_timeout
* tcp_keepalive -- if 0 this disables keepalives. if non-zero it sets the keepalive time to the given value
* tcp_nodelay -- enabled if 1, disabled if 0

## connect(options, cb)

Connect to the cluster specified by the given options and call the given callback when completed.

* address: (required) comma separated list of one or more addresses to connect to
* port: port on which to make the connection

## execute(query, params, options, callback)

Execute the specified query.

* query: (required) the CQL query
* params: (optional) data to bind to the query
* options: (optional) options for the query
* callback: (required) callback function

Supported options include:

* param_types: Type codes to indicate how to convert the params. See [Types](#types).
* result_types: Type codes to indicate how to convert the results. See [Types](#types).
* fetchSize: Maximum number of rows to return in a single query
* pageState: State from a prior invocation to indicate where to continue processing results.
* autoPage: Flag to indicate whether the library should page through all the results before triggering the callback.

On completion, will execute `callback(err, results)`. If there is no error, then `results.rows` contains an array with the resulting data. If an error occurred, then `err` contains the error and `results` is undefined.

If `fetchSize` was specified and the results were truncated, then `results.pageState` contains a handle that can be passed as an option to a subsequent invocation to have it continue processing results.

## eachRow(query, params, options, rowCallback, callback)

Execute the specified query.

* query: (required) the CQL query
* params: (optional) data to bind to the query
* options: (optional) options for the query
* callback: (required) callback function

Supported options include:

* param_types: Type codes to indicate how to convert the params. See [Types](#types).
* result_types: Type codes to indicate how to convert the results. See [Types](#types).
* fetchSize: Maximum number of rows to return in a single query
* pageState: State from a prior invocation to indicate where to continue processing results.
* autoPage: Flag to indicate whether the library should page through all the results before triggering the callback.

On completion, will execute `callback(err, results)`. If there is no error, then `results.rows` contains the requested data. If `fetchSize` was specified and the results may have been truncated, then `results.pageState` contains a handle that can be passed as an option to a subsequent invocation to have it continue processing results.

## new_query()

Low level API to create a query object.

Returns an instance of a [Query](#query) object.

## prepare(query, callback)

Prepare the query for more efficient execution.

* query: (required) the CQL query
* callback: (required) callback function

On completion, calls `callback(err, prepared)`, returning an instance of a [Prepared](#prepared) query.

## new_batch(type)

Create a new batch query.

* type: (required) Either "logged", "unlogged", or "counter".

Returns an instance of a [Batch](#batch) query.

## metrics(reset)

Return performance metrics about the driver's operation.

* reset: (required) If true, then the counters are cleared after being returned.

# <a name="query"></a> Query

Query is the underlying object exposed by the C++ node driver for executing queries, obtained from `Client.new_query(...)`

## parse(query, params, options)

Parse the given expression.

* query: (required) the CQL query
* params: (optional) data to bind to the query
* options: (optional) options for the query

Supported options include:

* param_types: Type codes to indicate how to convert the params. See [Types](#types).

## bind(params, options)

Bind the given parameters to the prepared query.

* params: (required) data to bind to the query
* options: (optional) options for the query

Supported options include:

* param_types: Type codes to indicate how to convert the params. See [Types](#types).

## execute(options, callback)

Execute the query, calling the callback when completed.

* options: (optional) options for the query
* callback: (required) callback function

Supported options include:

* result_types: Type codes to indicate how to convert the results. See [Types](#types).
* fetchSize: Maximum number of rows to return in a single query
* pageState: If given a reference to the query object, will continue processing from the previous invocation.

On completion, will execute `callback(err, results)`. If there is no error, then `results.rows` contains an array with the resulting data and `results.more` contains a boolean to indicate whether the result was truncated due to `fetchSize` limitations. If an error occurred, then `err` contains the error and `results` is undefined.

# <a name="prepared"></a> Prepared

Prepared queries are never instantiated directly but are returned from `Client.prepare(...)`.

A prepared can be used either to obtain a query object or can be included in a batch.

## query()

Returns a new instance of a [Query](#query) object. The caller must call `bind()` on that object to set the query parameters and then `execute` to actually evaluate the query.

# <a name="batch"></a> Batch

Batch queries can be used to amortize the cost of multiple round trips to the cassandra cluster. They are never instantiated directly but are returned from `Client.new_batch(...)`.

## add(query)

Add the given query to the batch. The query must have been parsed previously.

## add_prepared(prepared, params, options)

Add a given prepared and bind the query parameters to the batch.

* prepared: (required) the prepared query
* params: (required) data to bind to the query
* options: (optional) options for the batch

Supported options include:

* param_types: Type codes to indicate how to convert the params. See [Types](#types).

Note that `batch.add_prepared(prepared, params)` is functionally identical to:

```
var query = prepared.query();
query.bind(params);
batch.add(query);
```

However, `add_prepared` is significantly more efficient since it doesn't require allocating a Javascript object for each statement that is added to the batch.

## execute(options, callback)

Execute the batch operation, calling the callback when completed.

* options: (required) options for the query
* callback: (required) callback function

On completion, will execute `callback(err, results)`. If the operation succeeded then err is null. If the operation failed then the error is passed in err.

# <a name="types"></a> Types

## Supported Data Types

Currently the driver supports only a subset of the cassandra data types, and the the following table lists the types that are supported and their corresponding Javascript value types. 


| **Cassandra Type**        | **Javascript Type**
|:--------------------------|:-------------------
| CASS_VALUE_TYPE_ASCII     | String |
| CASS_VALUE_TYPE_BIGINT    | Number (*) |
| CASS_VALUE_TYPE_BLOB      | Buffer |
| CASS_VALUE_TYPE_BOOLEAN   | Boolean |
| CASS_VALUE_TYPE_COUNTER   | Number (*) |
| CASS_VALUE_TYPE_CUSTOM    | *Unsupported* |
| CASS_VALUE_TYPE_DECIMAL   | *Unsupported* |
| CASS_VALUE_TYPE_DOUBLE    | Number |
| CASS_VALUE_TYPE_FLOAT     | Number |
| CASS_VALUE_TYPE_INET      | *Unsupported* |
| CASS_VALUE_TYPE_INT       | Number |
| CASS_VALUE_TYPE_LIST      | *Unsupported* |
| CASS_VALUE_TYPE_MAP       | *Unsupported* |
| CASS_VALUE_TYPE_SET       | *Unsupported* |
| CASS_VALUE_TYPE_TEXT      | String |
| CASS_VALUE_TYPE_TIMESTAMP | Number (*) |
| CASS_VALUE_TYPE_TIMEUUID  | *Unsupported* |
| CASS_VALUE_TYPE_UUID      | *Unsupported* |
| CASS_VALUE_TYPE_VARCHAR   | String |
| CASS_VALUE_TYPE_VARINT    | *Unsupported* |

(*) Large number values can also be encoded in a javascript object for full precision. See [handling large numbers](#handling_large_numbers) below.

## Type inference and param_types

When binding data parameters to a query, it is not always possible to determine the correct cassandra type by introspecting the Javascript data type since there are more than one mapping. Therefore a caller can pass an array of `param_types` containing the above type codes to indicate how to encode the various parameters.

For example, given the following table:

```
CREATE TABLE test (
  key text,
  doubleval double,
  floatval float,
  intval int,
  PRIMARY KEY (key)
  WITH ...
)
```

Inserting values can be done with the following logic:

```
var cassandra = require('cassandra-native-driver');
var client = new cassandra.Client();

client.connect({...}, function() {
    var types = cassandra.types;
    client.execute('INSERT INTO test (key, doubleval, floatval, intval) values (?, ?, ?, ?)',
        ["testing", 1.1000000001, 1.25, 100],
        {
            param_types: [
                    types.CASS_VALUE_TYPE_VARCHAR,
                    types.CASS_VALUE_TYPE_DOUBLE,
                    types.CASS_VALUE_TYPE_FLOAT,
                    types.CASS_VALUE_TYPE_INT
                ]
        },
        function(err, results) {
            console.log('inserted');
        }
    );
});
```

If the param_types are not supplied, then the driver tries to infer the cassandra type based on the Javascript type. Specifically, Javascript Strings are treated as VARCHAR, Number as DOUBLE, Boolean as BOOL, and Buffer as BLOB.

<a name="handling_large_numbers"></a>
## Handling large numbers

Javascript numbers can only hold 53 bits of precision, which may not be sufficient for some applications that need to access the full 64 bits of precision of some cassandra types. To handle this, an application can include a `BIGINT_AS_OBJECT` encoding flag along with the type code to cause the driver to split the 64 bit value into a javascript object of the form `{low: <lowInt>, 'high': <highInt>}`.

To use this support, type codes can to be passed to the driver to control the conversion of data both for the parameters and for the results of a query. For example:

```
var cassandra = require('cassandra-native-driver');
var client = new cassandra.Client();

client.connect({...}, function() {
    var types = cassandra.types;
    var encodings = cassandra.encodings;
    client.execute('INSERT INTO test (key, value) values (?, ?)',
        ["testing", {low: 0x12345678, high: 0x12345678}],
        {
            param_types: [
                    types.CASS_VALUE_TYPE_VARCHAR,
                    types.CASS_VALUE_TYPE_BIGINT | encodings.BIGINT_AS_OBJECT
                ]
        },
        function(err, results) {
            console.log('inserted data... now querying');
            client.execute('SELECT key, value from test where key = ?',
                ["testing"],
                {
                    param_types: [
                        types.CASS_VALUE_TYPE_VARCHAR,
                    ],
                    result_types: [
                        types.CASS_VALUE_TYPE_VARCHAR,
                        types.CASS_VALUE_TYPE_BIGINT | encodings.BIGINT_AS_OBJECT
                        ]
                },
            function(err, results) {
                console.log('got data', results.rows[0].value);
            }
        }
    );
});
```

# <a name="logging"></a> Logging

The driver exposes the logging capabilities of the underlying C++ driver can be displayed or logged from Javascript.

## set_log_callback(callback)

Registers a javascript function `callback(err, event)` that is called for each emitted log event.

Currently `err` is always null and `event` will contain an object containing `severity`, `time_ms`, and `message`.

## set_log_level(level)

Sets the logging severity for the driver. Level must be one of "CRITICAL", "ERROR", "WARN", "INFO", "DEBUG", or "TRACE".
