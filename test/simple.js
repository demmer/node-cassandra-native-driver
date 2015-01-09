
var cassandra = require('../index');

var client = new cassandra.Client();
client.connect(function() {});

var aaa = [0x61, 0x61, 0x61, 0x00, 0x00, 0x00, 0x01, 0x48, 0x3d, 0xf5, 0xec, 0x00, 0x00, 0x0b, 0x6b, 0x61, 0x69, 0x72, 0x6f, 0x73, 0x5f, 0x6c, 0x6f, 0x6e, 0x67, 0x73, 0x70, 0x61, 0x63, 0x65, 0x3d, 0x64, 0x65, 0x66, 0x61, 0x75, 0x6c, 0x74, 0x3a];
var zzz = [0x7a, 0x7a, 0x7a, 0x00, 0x00, 0x00, 0x01, 0x49, 0x16, 0x41, 0x04, 0x00, 0x00, 0x0b, 0x6b, 0x61, 0x69, 0x72, 0x6f, 0x73, 0x5f, 0x6c, 0x6f, 0x6e, 0x67, 0x73, 0x70, 0x61, 0x63, 0x65, 0x3d, 0x64, 0x65, 0x66, 0x61, 0x75, 0x6c, 0x74, 0x3a];

var fetchSize = 10000;
var key = new Buffer(zzz);

var opts = {autoPage: true, fetchSize: fetchSize, typeHacks: true};
function paged() {
    console.log('paged');
    var start = new Date();
    var N = 0;
    var first, last, xxx;
    client.query('SELECT column1, value FROM data_points where key = ?', [key], opts,
    function(results) {
        if (N === 0) {
            first = results.rows[0];
        }
        if (results.rows.length > 0) {
            last = results.rows[results.rows.length - 1];
        }
        N += results.rows.length;
    },
    function(err) {
        if (err) { throw(err); }

        var elapsed = new Date() - start;
        console.log('got', N, 'results in', elapsed, 'ms (', 1000 * elapsed / N, 'us per point)');
        console.log(first);
        console.log(xxx);
        console.log(last);
    });
}

function execute() {
    console.log('execute');
    var start = new Date();

    client.execute('SELECT column1, value FROM data_points where key = ?', [key], {autoPage: true, fetchSize: fetchSize},
        function(err, results) {
            if (err) { throw err; }

            var elapsed = new Date() - start;
            var N = results.rows.length;
            console.log('got', N, 'results in', elapsed, 'ms (', 1000 * elapsed / N, 'us per point)');
            console.log('row 0:', results.rows[0]);
            console.log('row', fetchSize, results.rows[fetchSize]);
            console.log('row', N - 1, ':', results.rows[N - 1]);
        }
    );
}

function eachRow() {
    console.log('eachRow');
    var start = new Date();
    var first, last;
    N = 0;
    client.eachRow('SELECT column1, value FROM data_points where key = ?', [key], {autoPage: true, fetchSize: fetchSize},
    function(row) {
        if (N === 0) {
            first = row;
        }
        N++;
    },
    function(err) {
        if (err) { throw err; }
        var elapsed = new Date() - start;
        console.log('got', N, 'results in', elapsed, 'ms (', 1000 * elapsed / N, 'us per point)');
        console.log(first);
        console.log('last')
    }
);
}


function rowKey() {
    client.execute('SELECT * from row_key_index where key in ?', [[new Buffer([0x61, 0x61, 0x61])]], function(err, results) {
        console.log(err, results);
        eachRow();
    });
}

client.execute('USE kairosdb', paged);



/*
client.execute('SELECT * FROM system.schema_keyspaces where keyspace_name = \'system\'', function(err, results) {
    console.log(err);
});
*/
