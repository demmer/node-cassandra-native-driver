var addon = require('./addon');

module.exports = {
    Client: require('./lib/client'),
    set_log_callback: addon.set_log_callback,
    set_log_level: addon.set_log_level,
    types: {
        CASS_VALUE_TYPE_UNKNOWN: 0xFFFF,
        CASS_VALUE_TYPE_CUSTOM: 0x0000,
        CASS_VALUE_TYPE_ASCII: 0x0001,
        CASS_VALUE_TYPE_BIGINT: 0x0002,
        CASS_VALUE_TYPE_BLOB: 0x0003,
        CASS_VALUE_TYPE_BOOLEAN: 0x0004,
        CASS_VALUE_TYPE_COUNTER: 0x0005,
        CASS_VALUE_TYPE_DECIMAL: 0x0006,
        CASS_VALUE_TYPE_DOUBLE: 0x0007,
        CASS_VALUE_TYPE_FLOAT: 0x0008,
        CASS_VALUE_TYPE_INT: 0x0009,
        CASS_VALUE_TYPE_TEXT: 0x000A,
        CASS_VALUE_TYPE_TIMESTAMP: 0x000B,
        CASS_VALUE_TYPE_UUID: 0x000C,
        CASS_VALUE_TYPE_VARCHAR: 0x000D,
        CASS_VALUE_TYPE_VARINT: 0x000E,
        CASS_VALUE_TYPE_TIMEUUID: 0x000F,
        CASS_VALUE_TYPE_INET: 0x0010,
        CASS_VALUE_TYPE_LIST: 0x0020,
        CASS_VALUE_TYPE_MAP: 0x0021,
        CASS_VALUE_TYPE_SET: 0x0022
    }
};
