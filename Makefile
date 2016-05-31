CXX=g++ -std=c++0x

default: all

.DEFAULT:
	cd src && $(MAKE) $@
