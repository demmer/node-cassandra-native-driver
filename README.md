node-cassandra-native-driver
============================

Node.js addon that wraps the [DataStax C++ cassandra driver](https://github.com/datastax/cpp-driver).

Functionally, this project is similar to [https://github.com/datastax/nodejs-driver] except that it offers better performance due to the fact that the protocol processing is performed in C++ and can thereby leverage other threads instead of just the main loop thread.

Installation
============

    npm install cassandra-native-driver

Features
========

* Exposes much of the native driver functionality, including regular, prepared, and batch statements as well as various configuration options.
* Tuned for high performance, capable of 100,000s of operations per second under some circumstances.
* Fully parallel execution model, including support for paging results.
* Asynchronous logging handler that exposes C++ driver logs as Javascript callbacks.
* Supports most basic cql types, including int, varchar, double, boolean, blob, counter, timestamp including a convenient type based on the Javascript type as well as an API to explicitly select the type.

Basic Usage
===========

```
var cassandra = require('cassandra-native-driver');
var client = new cassandra.Client(options);
client.connect({address: "10.0.0.1,10.0.0.2"}, function(err) {
    var cql = "SELECT email from profile where username = ?";
    client.execute(cql, 'joe', function(err, results) {
        console.log('got email', results.rows[0]);
    });
}
```

Documentation
=============

TBD...

License
=======

The wrapper library and addon are licensed for distribution by the MIT License.

This repository also includes a copy of the DataStax C++ driver from https://github.com/datastax/cpp-driver which is covered by the Apache License.

This code has been slightly modified to include conditional compilation directives to enable it to be used within the context of a node.js addon.
