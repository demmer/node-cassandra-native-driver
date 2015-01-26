var Promise = require('bluebird');
var TestClient = require('./test-client');
var types = TestClient.types;
var expect = require('chai').expect;
var table = 'test';
var _ = require('underscore');
var util = require('util');
var setup_environment = require('./test-utils').setup_environment;

var client;

describe('hints', function() {
    before(function() {
        client = new TestClient();
        return setup_environment(client);
    });

    _.each(['insertRows', 'insertRowsPrepared', 'insertRowsPreparedBatch'], function(method, index) {
        it(method + ' storing ints in a table whose value type is double', function() {
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
            })
            .finally(function(err) {
                return client.execute('drop table ' + table + ';');
            });
        });
    });

    after(function() {
        return client.cleanup();
    });
});
