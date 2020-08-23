# dtype-next

Next generation high performance Clojure numerics stack and toolkit.

tech.datatype has been a great library for TechAscent and has allowed us to produce
quite a lot of high performance software.  We were able to push the Clojure
compile time system to its limits and are generally happy with the architecture
and the results in terms of flexibility without compromising any performance.

I, however, need a space to produce v3 of the library and various components ideally
with less runtime code generation and supporting graal native out of the box.

Some expected items:

*  Smaller runtime footprint.  This is harder than it looks but the main thing is that
   the system needs to produce the right answers with less type specific code.
*  Full native memory support.  Malloc, free, memset, memcpy.  Just the basics but
   guaranteed to be available.
   
## Native Test

* In order to get mmap working on the native test I had to grab the larray .so from the 
  uberjar and load it manually.  Not a big issue at the end of the day but I was having
  problems getting graal native to package resources.
  

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
