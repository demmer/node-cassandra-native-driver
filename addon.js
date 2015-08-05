if (process.env.CASSANDRA_DRIVER_DEBUG) {
    console.log('loading debug driver');
    module.exports = require('./build/Debug/cassandra-native-driver');
} else {
    module.exports = require('./build/Release/cassandra-native-driver');
}
