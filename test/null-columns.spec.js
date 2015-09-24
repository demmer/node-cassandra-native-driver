var TestClient = require('./test-client');
var Promise = require('bluebird');
var expect = require('chai').expect;
var _ = require('underscore');
var util = require('util');
var test_utils = require('./test-utils');

describe('columns with null values', function() {

    var ks = 'null_test';
    var table = 'nulltest';
    var fields = {
        'key': 'varchar',
        'col1': 'int',
        'col2': 'int',
        'col3': 'int'
    };

    var key = 'key';
    var data = [{
        'key': 'row-1',
        'col1': 1,

        // Explicitly pass in `null` for the `col2` column so we can verify that null values can be set
        'col2': null

        // Do not pass in any value for `col3`
    }];
    var client;

    before(function() {
        client = new TestClient();
        return test_utils.setup_environment(client)
            .then(function() {
                return client.createTable(table, fields, key);
            })
            .then(function() {
                return client.insertRows(table, data);
            });
    });

    it('retrieves null columns', function() {
        var cql = util.format('SELECT * FROM %s where key = ?', table);
        return client.prepare(cql)
            .then(function(prepared) {
                var q = prepared.query();
                q.bind(['row-1'], {});
                Promise.promisifyAll(q);
                return q.executeAsync({});
            })
            .then(function(results) {
                expect(results.rows.length).equal(1);
                var row = results.rows[0];

                // All keys should be returned
                var keys = Object.keys(row);
                expect(keys.length).equal(4);

                // Verify each column's value
                expect(row.key).equal('row-1');
                expect(row.col1).equal(1);
                expect(row.col2).equal(null);
                expect(row.col3).equal(null);
            });
    });

    after(function() {
        return client.cleanup();
    });
});
