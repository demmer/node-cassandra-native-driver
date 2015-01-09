var cassandra = require('../index');
var Promise = require('bluebird');

var client = new cassandra.Client();
client.connect(function() {});

Promise.promisifyAll(client);

client.executeAsync('use kairosdb;')
.then(function() {
    return client.executeAsync('create table int_type_test (key int, value int, primary key(key));');
})
.then(function() {
    return client.executeAsync('insert into int_type_test (key, value) values (?, ?);', [123, 456]);
})
.then(function() {
    return client.executeAsync('select * from int_type_test where key=?;', [123]);
})
.then(function(results) {
    console.log('got results', results);
})
.catch(function(err) {
    console.log('XXX got error', err.stack);
})
.finally(function() {
    return client.executeAsync('drop table int_type_test;');
});
