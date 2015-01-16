var Promise = require('bluebird');
var TestClient = require('./test-client');
var types = TestClient.types;
var expect = require('chai').expect;
var ks = 'prepared_test';
var table = 'test';
var _ = require('underscore');
var util = require('util');

var client = new TestClient();
client.connect('127.0.0.1')
.then(function() {
    return client.cleanKeyspace(ks);
})
.then(function() {
    return client.createKeyspace(ks);
})
.then(function() {
    return client.execute("USE " + ks);
})
.then(function() {
    _.each(['insertRows', 'insertRowsPrepared', 'insertRowsPreparedBatch'], function(method, index) {
        var table = 'hint_test' + index;
        var data = [{key: 1, value: 1}];
        return client.createTable(table, {key: 'int', value: 'double'}, 'key')
        .then(function() {
            return client[method](table, data, {hints: {key: types.CASS_VALUE_TYPE_INT, value: types.CASS_VALUE_TYPE_DOUBLE}, batch_size: 1});
        })
        .then(function() {
            return client.execute('select * from ' + table + ' where key=?;', [data[0].key]);
        })
        .then(function(results) {
            var res = results.rows;
            expect(res).deep.equal(data);
            console.log('passed', method);
        })
        .finally(function(err) {
            return client.execute('drop table ' + table + ';');
        });
    });
});
