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

* contactPoints: (required) comma separated string containing addresses of one or more hosts in the cluster
* port: port on which to make the connection

## execute(query, params, options, callback)

Execute the specified query.

* query: (required) CQL query string
* params: (optional) data to bind to the query
* options: (optional) options for the query
* callback: (required) callback function

Supported options include:

* hints: Type hints to indicate how to convert the params. See [Types](#types).
* fetchSize: Maximum number of rows to return in a single query
* pageState: State from a prior invocation to indicate where to continue processing results.
* autoPage: Flag to indicate whether the library should page through all the results before triggering the callback.

On completion, will execute `callback(err, results)`. If there is no error, then `results.rows` contains an array with the resulting data. If an error occurred, then `err` contains the error and `results` is undefined.

If `fetchSize` was specified and the results were truncated, then `results.pageState` contains a handle that can be passed as an option to a subsequent invocation to have it continue processing results.

## eachRow(query, params, options, rowCallback, callback)

Execute the specified query.

* query: (required) CQL query string
* params: (optional) data to bind to the query
* options: (optional) options for the query
* callback: (required) callback function

Supported options include:

* hints: Type hints to indicate how to convert the params. See [Types](#types).
* fetchSize: Maximum number of rows to return in a single query
* pageState: State from a prior invocation to indicate where to continue processing results.
* autoPage: Flag to indicate whether the library should page through all the results before triggering the callback.

On completion, will execute `callback(err, results)`. If there is no error, then `results.rows` contains the requested data. If `fetchSize` was specified and the results may have been truncated, then `results.pageState` contains a handle that can be passed as an option to a subsequent invocation to have it continue processing results.

## new_query()

Low level API to create a query object.

Returns an instance of a [Query](#query) object.

## prepare(query, callback)

Prepare the query for more efficient execution.

* query: (required) CQL query string
* callback: (required) callback function

On completion, calls `callback(err, prepared)`. If there is no error then prepared contains an instance of a [Prepared](#prepared) query, otherwise `err` indicates the error.

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

* query: (required) CQL query string
* params: (optional) data to bind to the query
* options: (optional) options for the query

Supported options include:

* hints: Type hints to indicate how to convert the params. See [Types](#types).

## bind(params, options)

Bind the given parameters to the prepared query.

* params: (required) data to bind to the query
* options: (optional) options for the query

Supported options include:

* hints: Type hints to indicate how to convert the params. See [Types](#types).

## execute(options, callback)

Execute the query, calling the callback when completed.

* options: (optional) options for the query
* callback: (required) callback function

Supported options include:

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

* hints: Type hints to indicate how to convert the params. See [Types](#types).

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

Currently the driver supports only a subset of the cassandra data types. The following table lists the types that are supported and their corresponding Javascript value types:

| **Cassandra Type**        | **Javascript Type**
|:--------------------------|:-------------------
| CASS_VALUE_TYPE_CUSTOM    | *Unsupported*
| CASS_VALUE_TYPE_ASCII     | String |
| CASS_VALUE_TYPE_BIGINT    | Object of the form `{'low': <lowInt>, 'high': <highInt>}` |
| CASS_VALUE_TYPE_BLOB      | Buffer |
| CASS_VALUE_TYPE_BOOLEAN   | Boolean |
| CASS_VALUE_TYPE_COUNTER   | Number |
| CASS_VALUE_TYPE_DECIMAL   | *Unsupported* |
| CASS_VALUE_TYPE_DOUBLE    | Number |
| CASS_VALUE_TYPE_FLOAT     | Number |
| CASS_VALUE_TYPE_INT       | Number |
| CASS_VALUE_TYPE_TEXT      | String |
| CASS_VALUE_TYPE_TIMESTAMP | Number |
| CASS_VALUE_TYPE_UUID      | *Unsupported* |
| CASS_VALUE_TYPE_VARCHAR   | String |
| CASS_VALUE_TYPE_VARINT    | *Unsupported* |
| CASS_VALUE_TYPE_TIMEUUID  | *Unsupported* |
| CASS_VALUE_TYPE_INET      | *Unsupported* |
| CASS_VALUE_TYPE_LIST      | *Unsupported* |
| CASS_VALUE_TYPE_MAP       | *Unsupported* |
| CASS_VALUE_TYPE_SET       | *Unsupported* |

## Type inference and hints

When binding data parameters to a query, it is not always possible to determine the correct cassandra type by introspecting the Javascript data type. Therefore a caller can pass an array of `hints` containing the above type codes to indicate how to encode the various parameters.

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
            hints: [types.CASS_VALUE_TYPE_VARCHAR,
                    types.CASS_VALUE_TYPE_DOUBLE,
                    types.CASS_VALUE_TYPE_FLOAT,
                    types.CASS_VALUE_TYPE_INT]
        },
        function(err, results) {
            console.log('inserted');
        }
    );
});
```

If the hints are not supplied, then the driver tries to infer the cassandra type based on the Javascript type. Specifically, Javascript Strings are treated as VARCHAR, Number as DOUBLE, Boolean as BOOL, and Buffer as BLOB.

# <a name="logging"></a> Logging

The driver exposes the logging capabilities of the underlying C++ driver can be displayed or logged from Javascript.

## set_log_callback(callback)

Registers a javascript function `callback(err, event)` that is called for each emitted log event.

Currently `err` is always null and `event` will contain an object containing `severity`, `time_ms`, and `message`.

## set_log_level(level)

Sets the logging severity for the driver. Level must be one of "CRITICAL", "ERROR", "WARN", "INFO", "DEBUG", or "TRACE".
