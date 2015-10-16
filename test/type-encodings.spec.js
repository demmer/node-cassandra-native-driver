var Promise = require('bluebird');
var TestClient = require('./test-client');
var types = TestClient.types;
var encodings = TestClient.encodings;
var expect = require('chai').expect;
var table = 'test';
var _ = require('underscore');
var util = require('util');
var setup_environment = require('./test-utils').setup_environment;

var client;

var tests = [
    {
        type: 'int',
        value: 1,
        code: types.CASS_VALUE_TYPE_INT
    },
    {
        type: 'double',
        value: 1.1,
        code: types.CASS_VALUE_TYPE_DOUBLE
    },
    {
        type: 'float',
        value: 1.25,
        code: types.CASS_VALUE_TYPE_FLOAT
    },
    {
        type: 'timestamp',
        value: 1423515666128,
        code: types.CASS_VALUE_TYPE_TIMESTAMP
    },
    {
        test: 'timestamp as object',
        type: 'timestamp',
        value: {
            'low': 15666128,
            'high': 14235
        },
        code: types.CASS_VALUE_TYPE_TIMESTAMP,
        encoding: encodings.BIGINT_AS_OBJECT
    },
    {
        type: 'boolean',
        value: false,
        code: types.CASS_VALUE_TYPE_BOOLEAN
    },
    {
        type: 'ascii',
        value: 'hello momma',
        code: types.CASS_VALUE_TYPE_ASCII
    },
    {
        type: 'varchar',
        value: 'hello daddy',
        code: types.CASS_VALUE_TYPE_VARCHAR
    },
    {
        type: 'text',
        value: 'lorum ipsum',
        code: types.CASS_VALUE_TYPE_TEXT
    },
    {
        type: 'blob',
        value: new Buffer([1, 2, 3]),
        code: types.CASS_VALUE_TYPE_BLOB
    },
    {
        type: 'bigint',
        value: 2891546411504,
        code: types.CASS_VALUE_TYPE_BIGINT
    },
    {
        test: 'bigint as object',
        type: 'bigint',
        value: {
            'low': 1546411504,
            'high': 286668
        },
        code: types.CASS_VALUE_TYPE_BIGINT,
        encoding: encodings.BIGINT_AS_OBJECT
    }
];

// XXX should test all of the above inside a map.

describe('type conversions', function() {
    before(function() {
        client = new TestClient();
        return setup_environment(client);
    });

    var i = 0;
    _.each(tests, function(t) {
        describe('supports ' + t.test || t.type, function() {
            var table = 'hint_test_' + t.type;
            var fields = {key: 'varchar', value: t.type, other: 'int'};
            var key_type = types.CASS_VALUE_TYPE_VARCHAR;
            var value_type = t.code | (t.encoding || 0);
            var other_type = types.CASS_VALUE_TYPE_INT;

            before(function() {
                return client.createTable(table, fields, 'key, other');
            });

            _.each(['insertRows', 'insertRowsPrepared', 'insertRowsPreparedBatch'], function(method) {
                describe(method, function() {
                    var data = {key: 'test_key_' + method, value: t.value, other: i++};
                    it('can add data with ' + method, function() {
                        return client[method](table, [data], {
                            param_types: {key: key_type, value: value_type, other: other_type},
                            batch_size: 1});
                    });

                    it('can query data by key', function() {
                        var query = 'select key, value, other from ' + table + ' where key=?;';
                        return client.execute(query, [data.key], {
                                              param_types: [key_type],
                                              result_types: [key_type, value_type, other_type]})
                        .then(function(results) {
                            var res = results.rows;
                            expect(res[0]).deep.equal(data);
                        });
                    });

                    it('can query data by key / other', function() {
                        var query = 'select key, value, other from ' + table + ' where key=? AND other = ?;';
                        return client.execute(query, [data.key, data.other], {
                                              param_types: [key_type, other_type],
                                              result_types: [key_type, value_type, other_type]})
                        .then(function(results) {
                            var res = results.rows;
                            expect(res[0]).deep.equal(data);
                        });
                    });
                });
            });

            after(function() {
                return client.execute('drop table ' + table + ';');
            });
        });
    });

    after(function() {
        return client.cleanup();
    });
});
