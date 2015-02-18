#ifndef __CASS_DRIVER_LOGGING_H__
#define __CASS_DRIVER_LOGGING_H__

#include "nan.h"

// Register a callback to propagate log messages up to Javascript
NAN_METHOD(SetLogCallback);

// Configure the logging level
NAN_METHOD(SetLogLevel);

#endif
