var TestClient = require('./test-client');
var Promise = require('bluebird');
var _ = require('underscore');
var expect = require('chai').expect;
var test_utils = require('./test-utils');

var client;
var tests = {
    varchar: ['foo', String.fromCharCode(160)],
    int: [123, 456],
    boolean: [true, false],
    double: [1.23, 4.56],
};

var maps = [{keyType: 'varchar', valueType: 'int', map: {a: 11, b: 2}},
            {keyType: 'varchar', valueType: 'varchar', map: {a: 'bananas', b: 'pajamas'}},
            {keyType: 'varchar', valueType: 'double', map: {pi: 3.142, e: 2.718}}];

describe('type inference', function() {
    before(function() {
        client = new TestClient();
        return test_utils.setup_environment(client);
    });

    _.each(tests, function(testValues, testType) {
        it('handles ' + testType, function() {
            var table = testType + '_test_table';
            return client.execute('create table ' + table + ' (key ' + testType + ', value ' + testType + ', primary key(key));')
                .then(function() {
                    return client.execute('insert into ' + table + ' (key, value) values (?, ?);', testValues);
                })
                .then(function() {
                    return client.execute('select * from ' + table + ' where key=?;', [testValues[0]]);
                })
                .then(function(results) {
                    var res = results.rows[0];
                    expect(res).deep.equal({key: testValues[0], value: testValues[1]});
                })
                .finally(function() {
                    return client.execute('drop table ' + table + ';');
                });
        });
    });

    _.each(maps, function(mapInfo, index) {
        it('handles a map with ' + mapInfo.keyType + 'keys and ' + mapInfo.valueType + ' values', function() {
            var mapType = '<' + mapInfo.keyType + ', ' + mapInfo.valueType + '>';
            var table = 'map_test_table' + index;
            return client.execute('create table ' + table + '(key varchar, value map' + mapType + ', primary key(key));')
                .then(function() {
                    return client.execute('insert into ' + table + ' (key, value) values (?, ?);', ['myMap', mapInfo.map]);
                })
                .then(function() {
                    return client.execute('select * from ' + table + ' where key=?;', ['myMap']);
                })
                .then(function(results) {
                    var res = results.rows[0];
                    expect(res).deep.equal({key: 'myMap', value: mapInfo.map});
                })
                .finally(function() {
                    return client.execute('drop table ' + table + ';');
                });
        });
    });

    after(function() {
        return client.cleanup();
    });
});
