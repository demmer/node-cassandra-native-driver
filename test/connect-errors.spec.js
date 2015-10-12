var TestClient = require('./test-client');
var _ = require('underscore');
var Promise = require('bluebird');
var expect = require('chai').expect;

describe('connection error handling', function() {
    it('fails if connect is called without options', function() {
        var client = new TestClient();
        return Promise.try(function() {
            return client.client.connect();
        })
        .then(function() {
            throw new Error('connect should not have succeeded');
        })
        .catch(function(err) {
            expect(err.message).equal('connect requires options to be an object');
        });
    });

    it('fails if connect is called without callback', function() {
        var client = new TestClient();
        return Promise.try(function() {
            return client.client.connect({});
        })
        .then(function() {
            throw new Error('connect should not have succeeded');
        })
        .catch(function(err) {
            expect(err.message).equal('connect requires callback to be a function');
        });
    });

    it('fails if connect is called with non-object options', function() {
        var client = new TestClient();
        return client.connect(100)
        .then(function() {
            throw new Error('connect should not have succeeded');
        })
        .catch(function(err) {
            expect(err.message).equal('connect requires options to be an object');
        });
    });

    it('fails if connect is called with null options', function() {
        var client = new TestClient();
        return client.connect(null)
        .then(function() {
            throw new Error('connect should not have succeeded');
        })
        .catch(function(err) {
            expect(err.message).equal('connect requires options to be an object');
        });
    });

    it('fails if connect is called with no contactPoints', function() {
        var client = new TestClient();
        return client.connect({})
        .then(function() {
            throw new Error('connect should not have succeeded');
        })
        .catch(function(err) {
            expect(err.message).equal('connect requires contactPoints');
        });
    });

    it('fails if connect is called with empty contactPoints', function() {
        var client = new TestClient();
        return client.connect({contactPoints: ""})
        .then(function() {
            throw new Error('connect should not have succeeded');
        })
        .catch(function(err) {
            expect(err.message).equal('No hosts provided or no hosts resolved');
        });
    });

    it('fails if connect is called with invalid contactPoints', function() {
        var client = new TestClient();
        return client.connect({contactPoints: "bogus.host"})
        .then(function() {
            throw new Error('connect should not have succeeded');
        })
        .catch(function(err) {
            expect(err.message).equal('No hosts provided or no hosts resolved');
        });
    });

    it('fails to connect repeatedly to a non-listening contactPoints', function() {
        var client = new TestClient();
        this.timeout(30000);
        return Promise.each(_.range(10001, 10025), function(port) {
            return client.connect({contactPoints: '127.0.0.1', port: port})
            .then(function() {
                throw new Error('unexpected success');
            })
            .catch(function(err) {
                expect(err.message).equal('No hosts available for the control connection');
            })
            .delay(25);
        });
    });
});
