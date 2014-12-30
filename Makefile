.PHONY: all build
all: build

rebuild:
	node-gyp rebuild

build:
	node-gyp build
