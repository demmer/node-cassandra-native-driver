var cassandra = require('../build/Debug/cassandra-native');
var lib = require('..');

setTimeout(function() { console.log('Done');}, 1000);

var c = new cassandra.Client();
c.connect(function() { console.log('connected');  } );

var q = c.new_query();

try {
    q.parse('select * from kairosdb.string_index limit 1', []);
    q.execute({}, function(err, results) {
        console.log('execute done', err, results);
        global.gc();
    });
} catch(err) {
    console.error('got err', err.stack);
}


var c2 = new lib.Client();
c.connect(function() { console.log('connected');  } );

try {
    c2.execute('select * from kairosdb.string_index limit 1', function(err, results) {
        console.log('c2 execute done', err, results);
        global.gc();
    });
} catch(err) {
    console.error('got err', err);
}

setTimeout(function() { console.log('gc timeout'); global.gc(); });
