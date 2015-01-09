var cassandra = require('../index');
var Promise = require('bluebird');

var client = new cassandra.Client();
client.connect(function() {});

Promise.promisifyAll(client);

client.executeAsync('use kairosdb;')
.then(function() {
    return client.executeAsync('create table string_type_test (key varchar, value varchar, primary key(key));');
})
.then(function() {
    return client.executeAsync('insert into string_type_test (key, value) values (?, ?);', ["foo", "bar"]);
})
.then(function() {
    return client.executeAsync('select * from string_type_test where key=?;', ['foo']);
})
.then(function(results) {
    console.log('got results', results);
})
.catch(function(err) {
    console.log('XXX got error', err.stack);
})
.finally(function() {
    return client.executeAsync('drop table string_type_test;');
});
