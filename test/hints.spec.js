var Promise = require('bluebird');
var TestClient = require('./test-client');
var types = TestClient.types;
var expect = require('chai').expect;
var table = 'test';
var _ = require('underscore');
var util = require('util');
var setup_environment = require('./test-utils').setup_environment;

var client;

var types = [
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
    }
];

// XXX should test all of the above inside a map.

describe('hints', function() {
    before(function() {
        client = new TestClient();
        return setup_environment(client);
    });

    _.each(types, function(t) {
        describe('supports ' + t.type, function() {
            var table = 'hint_test_' + t.type;
            var fields = {key: 'varchar', data: t.type};
            var hints = {data: t.code};

            before(function() {
                return client.createTable(table, fields, 'key');
            });

            _.each(['insertRows', 'insertRowsPrepared', 'insertRowsPreparedBatch'], function(method, index) {
                var data = {key: 'test_key_' + method, data: t.value};
                it('can add data with ' + method, function() {
                    return client[method](table, [data], {hints: hints, batch_size: 1})
                    .then(function() {
                        return client.execute('select * from ' + table + ' where key=?;', [data.key])
                    })
                    .then(function(results) {
                        var res = results.rows;
                        expect(res[0]).deep.equal(data);
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
