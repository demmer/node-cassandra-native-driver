.PHONY: all build
all: build

rebuild:
	node-gyp rebuild -d
	node-gyp rebuild

build:
	node-gyp build -d
	node-gyp build
