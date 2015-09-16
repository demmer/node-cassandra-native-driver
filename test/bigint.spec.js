var TestClient = require('./test-client');
var Promise = require('bluebird');
var expect = require('chai').expect;
var _ = require('underscore');
var util = require('util');
var test_utils = require('./test-utils');

var ks = 'paging_test';
var table = 'biginttest';

var fields = {
    'key': 'varchar',
    'bi': 'bigint',
    'i': 'int'
};

var key = 'key';
var data = [
    {
        'key': 'row-1',
        'bi': {
            'low': 0,
            'high': 0
        },
        'i': 0
    },
    {
        'key': 'row-2',
        'bi': {
            'low': 1546411504,
            'high': 0
        },
        'i': 1546411504
    },
    {
        'key': 'row-3',
        'bi': {
            'low': 0,
            'high': 286668
        },
        'i': 0
    },
    {
        'key': 'row-4',
        'bi': {
            'low': 1546411504,
            'high': 286668
        },
        'i': 0
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

    it('inserts data with bigint', function() {
        return client.insertRows(table, data);
    });

    it('retrieves all data', function() {
        var cql = util.format('SELECT * FROM %s', table);
        return client.execute(cql)
            .then(function(results) {
                expect(results.rows.length).equal(data.length);

                var rows = results.rows.sort(function(rowA, rowB) {
                    return rowA.key.localeCompare(rowB.key);
                });
                expect(rows).to.deep.equal(data);

            });
    });

    it('supports writetime values', function() {
        var cql = util.format('SELECT writetime(i) AS wt FROM %s WHERE key = ?', table);
        return client.execute(cql, ['row-1'])
            .then(function(results) {
                expect(results.rows.length).equal(1);

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
