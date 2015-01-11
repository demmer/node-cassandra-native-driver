var cassandra = require('../index');
var Promise = require('bluebird');
var _ = require('underscore');
var expect = require('chai').expect;
var client = new cassandra.Client();

Promise.promisifyAll(client);
var tests = {
    varchar: ['foo', 'bar'],
    int: [123, 456],
    boolean: [true, false],
    double: [1.23, 4.56],
}

client.connectAsync('127.0.0.1')
.then(function() {
client.executeAsync('use kairosdb;')
.then(function() {
    _.each(tests, function(testValues, testType) {
        var table = testType + '_test_table';
        client.executeAsync('create table ' + table + ' (key ' + testType + ', value ' + testType + ', primary key(key));')
        .then(function() {
            return client.executeAsync('insert into ' + table + ' (key, value) values (?, ?);', testValues);
        })
        .then(function() {
            return client.executeAsync('select * from ' + table + ' where key=?;', [testValues[0]]);
        })
        .then(function(results) {
            var res = results.rows[0];
            expect(res).deep.equal({key: testValues[0], value: testValues[1]});
            console.log('passed', testType);
        })
        .catch(function(err) {
            console.log('XXX got error', err.stack);
        })
        .finally(function() {
            return client.executeAsync('drop table ' + table + ';');
        });
    });
})
.then(function() {
    var maps = [{keyType: 'varchar', valueType: 'int', map: {a: 11, b: 2}},
                {keyType: 'varchar', valueType: 'varchar', map: {a: 'bananas', b: 'pajamas'}},
                {keyType: 'varchar', valueType: 'double', map: {pi: 3.142, e: 2.718}}];
    _.each(maps, function(mapInfo, index) {
        var mapType = '<' + mapInfo.keyType + ', ' + mapInfo.valueType + '>';
        var table = 'map_test_table' + index;
        client.executeAsync('create table ' + table + '(key varchar, value map' + mapType + ', primary key(key));')
        .then(function() {
            return client.executeAsync('insert into ' + table + ' (key, value) values (?, ?);', ['myMap', mapInfo.map]);
        })
        .then(function() {
            return client.executeAsync('select * from ' + table + ' where key=?;', ['myMap']);
        })
        .then(function(results) {
            var res = results.rows[0];
            expect(res).deep.equal({key: 'myMap', value: mapInfo.map});
            console.log('passed', mapInfo.map);
        })
        .catch(function(err) {
            console.error('whoops', err);
        })
        .finally(function() {
            return client.executeAsync('drop table ' + table + ';');
        });
    });
});
});
