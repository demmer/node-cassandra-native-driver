var ks = 'test_keyspace';

// support testing against cassandra running on a different host through
// the environmnet
function cassandra_host() {
    return process.env.CASSANDRA_HOST || '127.0.0.1';
}

// connects the client to Cassandra and sets up the given keyspace
// assumes that Cassanra is running on port 9042 of localhost
function setup_environment(client) {
    return client.connect({address: cassandra_host()})
    .then(function() {
        return client.cleanKeyspace(ks);
    })
    .then(function() {
        return client.createKeyspace(ks);
    })
    .then(function() {
        return client.execute("USE " + ks);
    });
}

function generate(n) {
    var data = [];
    for (var col = 0; col < n; col++) {
        var row = 'row-' + col % 10;
        data.push({
            row: row,
            col: col,
            val: Math.floor(Math.random() * 100)
        });
    }

    return data;
}

module.exports = {
    cassandra_host: cassandra_host,
    setup_environment: setup_environment,
    generate: generate,
    ks: ks
};
