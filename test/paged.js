var TestClient = require('./test-client');
var expect = require('chai').expect;
var ks = 'paging_test';
var table = 'test';
var _ = require('underscore');

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
    var row = "row-" + col % 10;
    var ret = {
        row: string(row),
        col: col,
        val: Math.floor(Math.random() * 100)
    };
    col++;
    return ret;
}

var data = _.times(10, generate);
console.log(data);

var client = new TestClient();
client.connect({address: '127.0.0.1'})
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
    return client.insertRows(table, data);
})
.then(function() {
    return client.execute('select * from ' + table, [],
                          {fetchSize: 2, autoPage: true});
})
.then(function(results) {
    expect(results.rows.length).equal(data.length);
    var rows = _.sortBy(results.rows, 'col');
    for (i = 0; i < rows.length; ++i) {
        expect(rows[i].col).equal(i);
    }
})
.then(function() {
    // test manual paging
    var cols = [];
    return client.execute('select * from ' + table, [], {fetchSize: 5})
    .then(function(results) {
        expect(results.rows.length).equal(5);
        cols = cols.concat(_.pluck(results.rows, 'col'));
        expect(results.meta.pageState).not.null();
        return client.execute('select * from ' + table, [],
            {fetchSize: 5, pageState: results.meta.pageState});
    })
    .then(function(results) {
        expect(results.rows.length).equal(5);
        cols = cols.concat(_.pluck(results.rows, 'col'));

        cols = cols.sort();
        for (i = 0; i < cols.length; ++i) {
            expect(cols[i]).equal(i);
        }
    });
})
.then(function() {
    console.log('success');
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
});
