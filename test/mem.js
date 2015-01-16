var cassandra = require('../build/Release/cassandra-native-driver');
var lib = require('..');

setTimeout(function() { console.log('Done');}, 1000);

var c = new cassandra.Client();
c.connect({address: '127.0.0.1'}, function() {
    console.log('connected');

    var q = c.new_query();

    try {
        q.parse('select * from kairosdb.string_index limit 1', []);
        q.execute({}, function(err, results) {
            console.log('execute done', err, results);
            global.gc();
            c = null;
        });
    } catch(err) {
        console.error('got err', err.stack);
    }

});

var c2 = new lib.Client();
c2.connect({address: '127.0.0.1'}, function() {
    console.log('connected');

    try {
        c2.execute('select * from kairosdb.string_index limit 1', function(err, results) {
            console.log('c2 execute done', err, results);
            c2.client = null;
            c2 = null;
            global.gc();
        });
    } catch(err) {
        console.error('got err', err);
    }
});

setTimeout(function() { console.log('gc timeout'); global.gc(); });
