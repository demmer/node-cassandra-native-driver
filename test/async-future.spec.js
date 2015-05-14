var TestClient = require('./test-client');
var Promise = require('bluebird');
var expect = require('chai').expect;
var ks = 'async_future_test';
var table = 'test';
var _ = require('underscore');
var util = require('util');
var test_utils = require('./test-utils');

var fields = {
    'row': 'varchar',
    'col': 'int',
    'val': 'int'
};

var key = 'row, col';
var data = test_utils.generate(100);
var client;

describe('async-future callbacks', function() {
    before(function() {
        client = new TestClient();
        return test_utils.setup_environment(client)
            .then(function() {
                return client.createTable(table, fields, key);
            });
    });

    it('inserts data using a batch of prepared queries', function() {
        var rows = _.times(10, function (i) { return 'row-' + i; });

        var cql = util.format('INSERT INTO %s (row, col, val) VALUES (?, ?, ?)', table);
        return client.prepare(cql)
            .then(function(prepared) {
                var batch = client.new_batch('unlogged');
                _.each(rows, function(row) {
                    batch.add_prepared(prepared, [row, 10, 1000000]);
                });

                Promise.promisifyAll(batch);
                return batch.executeAsync({});
            })
            .then(function(results) {
                return client.execute('SELECT * FROM ' + table, [], {});
            })
            .then(function(results) {
                expect(results.rows.length).equal(10);
            });
    });

    function spin(n) {
        var start = process.hrtime();

        while (true) {
            var delta = process.hrtime(start);
            var nanosec = delta[0] * 1e9 + delta[1];
            var ms = nanosec / 1e6;
            if (ms > n) {
                return;
            }
        }
    }

    it('properly emits all callbacks even if handlers are slow', function(done) {
        this.timeout(30000);
        var cql = util.format('SELECT * FROM %s where ROW = \'row-1\'', table);

        var N = 10;
        var Q = 0;
        var T = 1;

        function query() {
            client.client.execute(cql, [], function(err, result) {
                console.log('query', Q, 'done');
                Q++;

                if (Q == N) {
                    expect(T).equal(N);
                    return done();
                }

                query();
                spin(1000);
            });
        }
        query();

        var interval;
        function timer() {
            console.log('timer', T, 'done');
            T++;
            if (T == N) {
                clearInterval(interval);
            }
        }
        interval = setInterval(timer, 900);
    });


    after(function() {
        return client.cleanup();
    });
});
