# dtype-next

Next generation high performance Clojure toolkit.


[![Clojars Project](https://clojars.org/cnuernber/dtype-next/latest-version.svg)](https://clojars.org/cnuernber/dtype-next)

[![Build Status](https://travis-ci.com/cnuernber/dtype-next.svg?branch=master)](https://travis-ci.com/cnuernber/dtype-next)


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
such as numpy, OpenCV, and TVM.


Targets of this library are:


*  Small runtime footprint.  This is harder than it looks but the main thing is that
   the system needs to produce the right answers with as little type specific code as
   necessary.
*  Full native memory support.  Malloc, free, memset, memcpy.  Just the basics but
   guaranteed to be available.
*  Graal Native support.


Checkout the [API Documentation](https://cnuernber.github.io/dtype-next/)


## Native Test

* In order to get mmap working on the native test I had to grab the larray .so from the
  uberjar and load it manually.  Not a big issue at the end of the day but I was having
  problems getting graal native to package resources.

Use the scripts to get graal and compile test.  The code is located under native-test; so far
reader/writing/copying all work for native and jvm heap datasets.  Tensors work but appear to
really make the executable size larger; potentially this is the insn bindings which perhaps could
be optional.


## Graal Native

* https://github.com/lread/clj-graal-docs
* https://github.com/borkdude/clojure-rust-graalvm
* https://github.com/epiccastle/spire
* https://github.com/babashka/babashka-sql-pods


## License

Copyright © 2020 Chris Nuernberger

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.
