var TestClient = require('./test-client');
var Promise = require('bluebird');
var expect = require('chai').expect;
var table = 'test';
var _ = require('underscore');
var util = require('util');
var test_utils = require('./test-utils');

var fields = {
    'day': 'int',
    'usage': 'counter'
};

var NUM_POINTS = 50;
var msInDay = 1000 * 60 * 60 * 24;
var data = _.times(NUM_POINTS, function(n) {
    var d = new Date();
    d.setUTCDate(d.getUTCDate() - NUM_POINTS + n);
    return {
        day: Math.floor(d.getTime() / msInDay),
        usage: Math.pow(2, n)
    };
});

var key = 'day';
var client;

describe('counter support', function() {
    before(function() {
        client = new TestClient();
        return test_utils.setup_environment(client)
        .then(function() {
            return client.createTable(table, fields, key);
        });
    });

    it('inserts data with a counter', function() {
        var cqlBase = 'UPDATE %s SET usage = usage + %d WHERE day = %s';
        return Promise.each(data, function(pt) {
            var cql = util.format(cqlBase, table, pt.usage, pt.day);
            return client.execute(cql, [], {});
        });
    });

    it('retrieves all the data', function() {
        var cql = util.format('SELECT * FROM %s', table);
        return client.execute(cql)
            .then(function(results) {
                var received = _.sortBy(results.rows, 'day');
                expect(received).deep.equal(data);
            });
    });

    it('updates existing counters', function() {
        var cqlBase = 'UPDATE %s SET usage = usage + %d WHERE day = %s';
        return Promise.each(data, function(pt) {
            var cql = util.format(cqlBase, table, 5, pt.day);
            return client.execute(cql, [], {});
        });
    });

    it('retrieves the updated data', function() {
        var cql = util.format('SELECT * FROM %s', table);
        return client.execute(cql)
            .then(function(results) {
                var received = _.sortBy(results.rows, 'day');
                var expected = data.map(function(pt) {
                    var clone = _.clone(pt);
                    clone.usage += 5;
                    return clone;
                });
                expect(received).deep.equal(expected);
            });
    });

    after(function() {
        return client.cleanup();
    });
});
