var TestClient = require('./test-client');
var Promise = require('bluebird');
var expect = require('chai').expect;
var ks = 'timestamp_test';
var table = 'timestamp_test';
var _ = require('underscore');
var util = require('util');
var test_utils = require('./test-utils');

var fields = {
    'row': 'varchar',
    'col': 'int',
    'val': 'int'
};

var key = 'row, col';
var client;

describe('timestamp support', function() {
    before(function() {
        client = new TestClient();
        return test_utils.setup_environment(client)
        .then(function() {
            return client.createTable(table, fields, key);
        });
    });

    _.each(['insertRows', 'insertRowsPrepared', 'insertRowsPreparedBatch'], function(method, index) {
        var data = test_utils.generate(100);
        describe(method, function() {
            it('inserts data with timestamp', function() {
                var now = new Date().getTime();
                return client[method](table, data, {timestamp: now, ttl: 1});
            });

            it('retrieves all the data when immediately querying', function() {
                var rows = _.times(10, function (i) { return 'row-' + i; });

                var cql = util.format('SELECT * FROM %s', table);
                return client.execute(cql)
                    .then(function(results) {
                        expect(results.rows.length).equal(data.length);

                        var cols = _.pluck(results.rows, 'col');

                        cols.sort(function(a,b) { return a - b; });

                        for (i = 0; i < cols.length; ++i) {
                            expect(cols[i]).equal(i);
                        }
                    });
            });

            it('waits for a few seconds', function() {
                this.timeout(5000);
                return Promise.delay(2000);
            });

            it('queries again and should get no data', function() {
                var rows = _.times(10, function (i) { return 'row-' + i; });

                var cql = util.format('SELECT * FROM %s', table);
                return client.execute(cql)
                    .then(function(results) {
                        expect(results.rows.length).equal(0);
                    });
            });
        });
    });

    after(function() {
        return client.cleanup();
    });
});
