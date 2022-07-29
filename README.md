# dtype-next

Next generation high performance Clojure toolkit.


[![Clojars Project](https://clojars.org/cnuernber/dtype-next/latest-version.svg)](https://clojars.org/cnuernber/dtype-next)

[![Build Status](https://travis-ci.com/cnuernber/dtype-next.svg?branch=master)](https://travis-ci.com/cnuernber/dtype-next)

Checkout The:

* [Overview](https://cnuernber.github.io/dtype-next/overview.html)
* [API Documentation](https://cnuernber.github.io/dtype-next/)
* Java API [Documentation](https://cnuernber.github.io/dtype-next/javadoc/index.html) and [Sample](https://github.com/cnuernber/dtype-next/blob/master/java_test/java/jtest/Main.java).
* [Clojure Cheatsheet](https://cnuernber.github.io/dtype-next/cheatsheet.html).


## New Functionality
 * [Efficient 1d convolutions, correlations, gaussian correlations](https://cnuernber.github.io/dtype-next/tech.v3.datatype.convolve.html)
 * [Numeric gradient, elemwise difference](https://cnuernber.github.io/dtype-next/tech.v3.datatype.gradient.html)
 * [Variable rolling windows](https://cnuernber.github.io/dtype-next/tech.v3.datatype.rolling.html#var-variable-rolling-window-indexes)

## Description

`dtype-next` provides a unified pathway for dealing with contiguous containers of primitive datatypes such as
ints and floats.  In addition it defines the basis for `array` programming as found in APL or numpy and
a deep Java interface hierarchy with default methods to allow implementing new `array`s painless.  This
interface hierarchy integrates with Java Streams, Spliterators, and various members of the java.util.function
package.  In addition it extends these concepts to native-heap based containers.


There are namespaces to allow elementwise operations across scalars and arrays, highly optimized reductions
across index spaces, and algorithms that operate in index space for use when multiple buffers share
an index space.


This library forms the numeric basis that underlies the ['tech.ml.dataset'](https://github.com/techascent/tech.ml.dataset)
system.  It also defines a language independent ABI which allows zerocopy to C-based systems
such as [numpy](https://github.com/clj-python/libpython-clj), [OpenCV](https://github.com/techascent/tech.opencv),
[Julia](https://github.com/cnuernber/libjulia-clj), [TVM](https://github.com/techascent/tvm-clj)
and [neanderthal](https://github.com/uncomplicate/neanderthal).


Additional targets of this library:


*  Small runtime footprint.  This is harder than it looks but the main thing is that
   the system needs to produce the right answers with as little type specific code as
   necessary.
*  Full native memory support.  Malloc, free, memset, memcpy.  Just the basics but
   guaranteed to be available.
*  Graal Native support.
*  Support for JDK-8 through JDK-17+ - JDK-16 is *no longer* supported.  For jdk-17 usage, please see
   project.clj for required flags.
*  [Blogpost](https://techascent.com/blog/next-gen-native.html), [example](examples/clj-ffi) and [involved example](https://github.com/cnuernber/avclj) of using the FFI architecture across JNA, JDK-16 and GraalNative.


## Native Test

* In order to get mmap working on the native test I had to grab the larray .so from the
  uberjar and load it manually.  Not a big issue at the end of the day but I was having
  problems getting graal native to package resources.

Use the scripts to get graal and compile test.  The code is located under native-test; so far
reader/writing/copying all work for native and jvm heap datasets.  Tensors work.


## Graal Native

* https://github.com/lread/clj-graal-docs
* https://github.com/borkdude/clojure-rust-graalvm
* https://github.com/epiccastle/spire
* https://github.com/babashka/babashka-sql-pods

## Test Dependencies

* Intel® Math Kernel Library - Use your system package manager to install `libmkl-rt`

## License

Copyright © 2020 Chris Nuernberger

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.
