var TestClient = require('./test-client');
var _ = require('underscore');
var Promise = require('bluebird');
var expect = require('chai').expect;

describe('connection error handling', function() {
    it('fails to connect repeatedly to a non-listening address', function() {
        var client = new TestClient();
        this.timeout(30000);
        return Promise.each(_.range(10001, 10025), function(port) {
            return client.connect({address: '127.0.0.1', port: port})
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
