var TestClient = require('./test-client');
var expect = require('chai').expect;
var ks = 'prepared_test';
var table = 'test';
var _ = require('underscore');
var util = require('util');

if (process.argv.length != 5) {
    throw new Error('usage: node insert-performance <count> <concurrent> <prepared>');
}
var count = parseInt(process.argv[2]);
var concurrent = parseInt(process.argv[3]);
var prepared = !! parseInt(process.argv[4]);

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
client.connect()
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
    return client.createTable(table, fields, key);
})
.then(function() {
    start = new Date();
    if (prepared) {
        return client.insertRowsPrepared(table, data, concurrent);
    } else {
        return client.insertRows(table, data, concurrent);
    }
})
.then(function() {
    end = new Date();
    return client.execute('select * from ' + table, [], {autoPage: true});
})
.then(function(results) {
    var elapsed = end - start;
    var N = results.rows.length;
    console.log(util.format('got %d rows in %d ms (%d us / pt, %d points per second)',
        N, elapsed, 1000 * elapsed / N, Math.floor(1000 * N / elapsed)));
    expect(results.rows.length).equal(count);
    var rows = _.sortBy(results.rows, 'col');
    for (i = 0; i < rows.length; ++i) {
        expect(rows[i].col).equal(i);
    }
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
