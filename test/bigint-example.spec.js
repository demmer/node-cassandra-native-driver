var TestClient = require('./test-client');
var Promise = require('bluebird');
var _ = require('underscore');
var expect = require('chai').expect;
var test_utils = require('./test-utils');
var util = require('util')

var types = TestClient.types;
var encodings = TestClient.encodings;

var client;

describe('bigint example', function() {
    before(function() {
        client = new TestClient();
        return test_utils.setup_environment(client);
    });

    var value = {
        high: 0x12345678,
        low: 0x12345678
    };

    var type_codes = [
        types.CASS_VALUE_TYPE_VARCHAR,
        types.CASS_VALUE_TYPE_BIGINT | encodings.BIGINT_AS_OBJECT
    ];

    it('creates a table', function() {
        return client.execute('create table bigint_example (key varchar, value bigint, primary key(key));');
    });

    it('inserts some data using object encoding', function() {
        return client.execute('insert into bigint_example (key, value) values (?, ?)',
                              ["test_key", value], {param_types: type_codes});
    });

    it('queries the data using the default encoding (truncated value)', function() {
        return client.execute('select key, value from bigint_example')
        .then(function(results) {
            expect(results.rows[0].value).equal(0x1234567812345600);
        });
    });

    it('queries the data using the object encoding (full precision)', function() {
        return client.execute('select key, value from bigint_example', [], {result_types: type_codes})
        .then(function(results) {
            expect(results.rows[0].value).deep.equal(value);
        });
    });

    after(function() {
        return client.cleanup();
    });
});
