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
        type: 'varchar',
        value: 'hello momma',
        code: types.CASS_VALUE_TYPE_VARCHAR
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

    _.each(['insertRows', 'insertRowsPrepared', 'insertRowsPreparedBatch'], function(method, index) {
        describe(method, function() {
            var table, data;
            it('creates the table', function() {
                table = 'hint_test' + index;

                var fields = {key: 'varchar'};
                _.each(types, function(t) {
                    fields[t.type + '_field'] = t.type;
                });

                return client.createTable(table, fields, 'key');
            });

            it('inserts some data', function() {
                var hints = {};
                data = {key: 'test_key'};

                _.each(types, function(t) {
                    hints[t.type + '_field'] = t.code;
                    data[t.type + '_field'] = t.value;
                });

                return client[method](table, [data], {hints: hints, batch_size: 1});
            });

            it('queries the data', function() {
                return client.execute('select * from ' + table + ' where key=?;', ['test_key'])
                .then(function(results) {
                    var res = results.rows;
                    expect(res[0]).deep.equal(data);
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
