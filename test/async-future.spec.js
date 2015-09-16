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
        client = new TestClient({result_loop_elapsed_max: 1500});
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

        client.metrics(true); // reset metrics

        function query() {
            client.client.execute(cql, [], function(err, result) {
                console.log('query', Q, 'done');
                Q++;
                expect(client.metrics().response_count).equal(Q);

                if (Q == N) {
                    expect(T).equal(N);
                    expect(client.metrics().pending_request_count_max).equals(1);
                    expect(client.metrics().response_queue_drain_count_max).equals(1);
                    expect(client.metrics().response_queue_drain_time_max).within(1000000, 11000000);
                    return done();
                }

                query();
                spin(1000);
            });

            expect(client.metrics().request_count).equal(Q + 1);
            expect(client.metrics().response_count).equal(Q);
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

    it('enforces result_loop_elapsed_max limit', function(done) {
        this.timeout(30000);
        var cql = util.format('SELECT * FROM %s where ROW = \'row-1\'', table);

        var N = 10;
        var Q = 0;
        var T = 0;

        client.metrics(true); // reset metrics

        function query() {
            client.client.execute(cql, [], function(err, result) {
                console.log('query', Q, 'done');
                Q++;

                if (Q == 1) {
                    for (var i = 0; i < 5; ++i) {
                        query();
                    }
                }

                expect(client.metrics().response_count).equal(Q);

                spin(500);
            });
        }

        // Issue five queries in parallel then wait to see that they all
        // complete. Once the first one returns complete, send another five.
        //
        // Since the driver is configured to spend no more than 1.5 seconds in
        // the loop, it should take it three passes through the queue to drain
        // everything, so three timer events should fire.
        for (var i = 0; i < 5; ++i) {
            query();
        }
        spin(500);

        function completed() {
            expect(T).equal(4);
            var metrics = client.metrics();
            expect(metrics.request_count).equal(10);
            expect(metrics.response_count).equal(10);
            expect(metrics.pending_request_count_max).equal(9);
            expect(metrics.response_queue_drain_count_max).equal(3);
            expect(metrics.response_queue_drain_time_max).within(1500000, 1510000);
            expect(metrics.response_queue_elapsed_limit_count).equal(2);

            done();
        }

        var interval;
        function timer() {
            console.log('timer', T, 'done');
            T++;

            if (Q == N) {
                clearInterval(interval);
                completed();
            }
        }
        interval = setInterval(timer, 100);
    });

    after(function() {
        return client.cleanup();
    });
});
