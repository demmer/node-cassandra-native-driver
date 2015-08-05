var Promise = require('bluebird');
var cassandra = Promise.promisifyAll(require('../index'));
var expect = require('chai').expect;

describe('log callback', function() {
    var messages = [];
    function log_callback(err, log) {
        console.log(log.severity, log.message);
        messages.push(log);
    }

    function reset() {
        while(messages.length > 0) {
            messages.pop();
        }
    }

    it('enables callback to obtain log messages', function() {
        cassandra.set_log_callback(log_callback);

        var c = new cassandra.Client();
        return c.connectAsync({address: '127.0.0.1', port: 9999})
        .catch(function(err) {
            expect(err.message).equal('No hosts available for the control connection');
            return Promise.delay(500); // return to the event loop
        })
        .then(function() {
            expect(messages.length).equal(3);
            expect(messages[0].message).match(/connection refused/);
            expect(messages[0].severity).equal('ERROR');
            expect(messages[1].message).match(/had the following error on startup/);
            expect(messages[1].severity).equal('ERROR');
            expect(messages[2].message).match(/Lost connection/);
            expect(messages[2].severity).equal('WARN');
            reset();
        });
    });

    it('supports configurable log level', function() {
        cassandra.set_log_level("TRACE");

        var c = new cassandra.Client();
        return c.connectAsync({address: '127.0.0.1', port: 9999})
        .catch(function(err) {
            expect(err.message).equal('No hosts available for the control connection');
            return Promise.delay(1); // return to the event loop
        })
        .then(function() {
            expect(messages.length).greaterThan(2);
            cassandra.set_log_level("WARN");

        });
    });

    it('errors if a second callback is registered', function() {
        return Promise.try(function() {
            function noop() {}
            cassandra.set_log_callback(noop);
        })
        .catch(function(err) {
            expect(err.message).equal('callback already registered');
        });
    });
});
