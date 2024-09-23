#!/bin/bash

gcc -shared -fPIC -rdynamic -o libffi_test.so ffi_test_lib.c
g++ test_byvalue.cpp -L./ -lffi_test -o byvalue
