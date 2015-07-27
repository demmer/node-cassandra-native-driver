var Promise = require('bluebird');
var TestClient = require('../test/test-client');
var expect = require('chai').expect;
var ks = 'prepared_test';
var table = 'test';
var _ = require('underscore');
var util = require('util');

if (process.argv.length < 5) {
    throw new Error('usage: node insert-performance <count> <concurrency> <mode> <batch_size>');
}
var count = parseInt(process.argv[2]);
var concurrency = parseInt(process.argv[3]);
var mode = process.argv[4];

var check_results = false;

var prepared, batch;
if (mode === "prepared") {
    prepared = true;
    batch = false;
}
else if (mode === "batch") {
    prepared = true;
    batch = parseInt(process.argv[5]);

    if (! batch) { throw new Error('invalid batch size'); }
}
else {
    prepared = false;
    batch = false;
}

var fields = {
    "row": "varchar",
    "col": "int",
    "val": "int"
};

var key = "row, col";

function string(x) {
    return '\'' + x.toString() + '\'';
}
var col = 0;
function generate() {
    var row = col % 10;
    var ret = {
        row: string(row),
        col: col,
        val: Math.floor(Math.random() * 100)
    };
    col++;
    return ret;
}

var data = _.times(count, generate);

var start, end;
var client = new TestClient();
client.connect({contactPoints: '127.0.0.1'})
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
    var kairos_opts = "WITH COMPACT STORAGE AND bloom_filter_fp_chance=0.010000 AND caching='KEYS_ONLY' AND comment='' AND dclocal_read_repair_chance=0.000000 AND gc_grace_seconds=864000 AND index_interval=128 AND read_repair_chance=1.000000 AND replicate_on_write='true' AND populate_io_cache_on_flush='false' AND default_time_to_live=0 AND speculative_retry='NONE' AND memtable_flush_period_in_ms=0 AND compaction={'class': 'SizeTieredCompactionStrategy'} AND compression={'sstable_compression': 'LZ4Compressor'};";
    return client.createTable(table, fields, key, kairos_opts);
})
.then(function() {
    start = new Date();
    if (batch) {
        return client.insertRowsPreparedBatch(table, data, {concurrency: concurrency, batch_size: batch});
    } else if (prepared) {
        return client.insertRowsPrepared(table, data, {concurrency: concurrency});
    } else {
        return client.insertRows(table, data, {concurrency: concurrency});
    }
})
.then(function(result) {
    end = new Date();
    var elapsed = end - start;
    var N = count;
    console.log(util.format('inserted %d rows in %d ms (%d us / pt, %d points per second)',
        N, elapsed, 1000 * elapsed / N, Math.floor(1000 * N / elapsed)));
})
.then(function() {
    start = new Date();

    var N = 0;
    var cols = [];
    return client.client.queryAsync('select * from ' + table, [], {autoPage: true},
        function(results) {
            N += results.rows.length;
            if (check_results) {
                cols = cols.concat(_.pluck(results.rows, 'col'));
            }
        }
    )
    .then(function() {
        end = new Date();
        var elapsed = end - start;
        console.log(util.format('queried %d rows in %d ms (%d us / pt, %d points per second)',
            N, elapsed, 1000 * elapsed / N, Math.floor(1000 * N / elapsed)));

        expect(N).equal(count);
        if (check_results) {
            cols = _.sortBy(cols, function(c) { return parseInt(c); });
            for (i = 0; i < cols.length; ++i) {
                expect(cols[i]).equal(i);
            }
        }
    });
})
.then(function() {
    console.log(client.client.metrics())
})
.catch(function(err) {
    console.error(err.stack);
    throw err;
})
.finally(function() {
    client.cleanup();
    if (global.gc) {
        console.log('calling gc');
        global.gc();
    }
    process.exit();
});
