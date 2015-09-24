var TestClient = require('./test-client');
var Promise = require('bluebird');
var expect = require('chai').expect;
var _ = require('underscore');
var util = require('util');
var test_utils = require('./test-utils');

var ks = 'bigint_test';
var table = 'biginttest';

var fields = {
    'key': 'varchar',
    'col1': 'text',
    'col2': 'int'
};

var key = 'key';
var data = [
    {
        'key': 'row-1',
        'col1': 'foo',
        'col2': 42
    }
];

var client;

describe('bigint support', function() {
    before(function() {
        client = new TestClient();
        return test_utils.setup_environment(client)
        .then(function() {
            return client.createTable(table, fields, key);
        });
    });

    it('supports writetime values', function() {
        // Insert some data into a table
        return client.insertRows(table, data)
            .then(function() {
                // Get the writetime value for a column
                var cql = util.format('SELECT writetime(col1) AS wt FROM %s WHERE key = ?', table);
                return client.execute(cql, ['row-1']);
            })
            .then(function(results) {
                expect(results.rows.length).equal(1);

                // Confirm the bigint that is returned by writetime is returned as an object with a
                // low and high integer
                var row = results.rows[0];
                expect(row).to.have.property('wt');
                expect(row).to.have.deep.property('wt.low');
                expect(row).to.have.deep.property('wt.high');
            });
    });

    after(function() {
        return client.cleanup();
    });
});
