var ks = 'test_keyspace';

// connects the client to Cassandra and sets up the given keyspace
// assumes that Cassanra is running on port 9042 of localhost
function setup_environment(client) {
    return client.connect({address: '127.0.0.1'})
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
    setup_environment: setup_environment,
    generate: generate
};
