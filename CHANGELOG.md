## 10.113
 * [issue-97](https://github.com/cnuernber/dtype-next/issues/97) - sorted output for tech.v3.datatype.casting/all-datatypes.
 * [issue-99](https://github.com/cnuernber/dtype-next/issues/99) - Unexpected behaviour when comparing certain numeric types.
 
## 10.112
 * [issue-94](https://github.com/cnuernber/dtype-next/issues/94) - Tensor slice would fail after a tensor select of a specific shape.
 
## 10.111
 * [issue-68](https://github.com/cnuernber/dtype-next/issues/68) - structs can be defined with :pointer, :size-t, :offset-t datatypes.  These types will differ on 32-bit and 64-bit systems.
   
## 10.110
 * jdk-21 support including pass-by-value ffi.  tmducken bindings can now work with jdk-21.
 * Faster hamf compose-reducers pathway especially when there are a lot of reducers.
 
## 10.108
 * hamf (2.009) typed nth operations.

## 10.107
 * hamf (2.008) perf upgrades.

## 10.106
 * hamf (2.007) perf upgrades and additional functionality.

## 10.105
 * hamf perf upgrades and additional functionality.

## 10.104
 * hamf perf upgrades.

## 10.103
 * Important hamf fix in compose-reducers.

## 10.102
 * small hamf updates.

## 10.100
 * Large hamf upgrade - update to hamf 2.0.

## 10.014
 * Fix serious issue with fastruct equiv pathway.

## 10.013
 * hamf-upgrade for faster reducer composition, mmin-idx and mmax-idx pathways.
## 10.012
 * hamf bugfix in pmap - return value correctly (and efficiently) implements seq and IReduceInit.

## 10.011
 * Additional *as fast as possible* routines for copying byte data in UsafeUtil.

## 10.010
 * optimization for larger strings in native-buffer->string

## 10.009
 * major fix in native-buffer->string pathway

## 10.008
 * various optimizations running duckdb and reading very large datasets.

## 10.006
 * fix to quartile-X, median.
 * added local-time->microseconds for duckdb integration.

## 10.005
 * fix to round
 * small perf enhancement to native-buffer/reduce

## 10.004
 * Optimizations around a performance case using duckdb.
 * tech.resource now uses arraylists for stack tracking and reverses them just before release.
 * native-buffer has optimized pathways for reduction if the datatype is :int8

## 10.003
 * Pass and return by value are implemented for the JNA backend.  This is an initial
   implementation but the API will not change in that arguments will be copied from
   something expected to implement java.util.Map and return values will be instances
   of java.util.Map.  Currently return values are datatype structs and the struct
   implementation's get and reduce pathways have been optimized.

## 10.000-beta-50
 * MAJOR API CHANGES - last before 10.000 release!!
   * See [issue-82](https://github.com/cnuernber/dtype-next/issues/82).
   * See [issue-81](https://github.com/cnuernber/dtype-next/issues/81).
   * See [issue-80](https://github.com/cnuernber/dtype-next/issues/80).
 * Nearly certainly the descriptive-statistics change will break your stuff if you are using it.

## 10.000-beta-49
 * Major fix to ham-fisted's hashtables.  This should be considered a must-have upgrade.

## 10.000-beta-47
 * tensor `reduce-axis` and `map-axis` have had their argument order changed to be tensor-first as this
   allows a nice `->` application.
 * `map-axis` received some optimization attention.

## 10.000-beta-46
 * Fixes related to upgrading TMD to latest ham-fisted.
 * added `map-axis` to tensor api.  This is similar to `reduce-axis` except it does not reduce
   the dimensions of the matrix but returns a new one.

## 10.000-beta-44
 * Pushing refactoring/simplification of hamf through system.  Breaking changes
   for projects relying specifically on hamf.  These are specifically changes
   related to bringing the whole system out of beta!

## 10.000-beta-43
 * Small update to hamf to do certain reductions in java.

## 10.000-beta-42
 * major speed increase making java double, float, int, and long arrays from
   existing dtype-next buffers.

## 10.000-beta-40
 * Minor ham-fisted update.

## 10.000-beta-38
 * Major ham-fisted update.

## 10.000-beta-36
 * m-1 macs get an extra search path as homebrew doesn't install libs in standard
   directories.

## 10.000-beta-35
 * Various ham-fisted updates - faster mapv, etc.

## 10.000-beta-30
 * combined emap operations were producing NAN in some cases.
 * ham-fisted contains some fancy helpers so you can implement custom ireduceinit pathways.

## 10.000-beta-28
 * bit-test is now fixed - a binary predicate, not a binary operation.

## 10.000-beta-27
 * hamf-helpers for very high performance scenarios.  See ham-fisted changelog.

## 10.000-beta-25
 * arggroup is now heavily optimized and near optimal.

## 10.000-beta-24
 * Docs for dfn/sq.

## 10.000-beta-23
 * Optimization of arggroup by turning into a set of reductions over subbuffers of the input
   data.  This avoids some indexing costs.

## 10.000-beta-22
 * arggroup must respect the order in which it encounters keys.  This was done via using group-by-consumer
   in hamf instead of group-by-reducer.   See documentation for group-by-consumer.

## 10.000-beta-21
 * Fix for issue 74 - default result of arggoup is a map that is difficult
   to use in normal Clojure workflows.

## 10.000-beta-20
 * JNA has to be the default ffi provider for now.  It just works better - it
 finds shared libraries more reliably.  IF you want this to change, then
 get libpython-clj to work and load numpy with the jdk-19 provider.

## 10.000-beta-19
 * JDK-19 Support.  Should be automatic; if you want to get rid of a warning see deps.edn file
   for jdk-19 alias.

## 10.000-beta-18
 * hamf bugfix - map compute.

## 10.000-beta-17
 * HUGE HAMF UPGRADE - see hamf changelog, this one changes the default base hamf map type.
 * moved away from google guava, it is no longer a dependency.  We now use caffeine and
   more java.util implementations.

## 10.000-beta-14
 * Faster `mode`.

## 10.000-beta-13
 * First deps.edn based deployment.
 * [issue 72](https://github.com/cnuernber/dtype-next/issues/72) - really support `:mode`.

## 10.000-beta-10
 * Brand new experimental jdk-19 support for the ffi layer.


## 10.000-beta-9
 * Faster native buffer specific pathways.  Very fast native buffer clone pathway.

## 10.000-beta-8
 * All buffers are comparable.
 * index reducers reduce correctly on empty.

## 10.000-beta-6
 * lots more hamf integration work - better native buffer implementation - fixed bug converting
   1xY and Yx1 neanderthal matrixes.

## 10.000-beta-1
 * Major changes - hamf integration.  Reductions are now first class, type array lists are faster.  All
   container types now support fast reduction via clojure.core/reduce.

## 9.033
 * [issue 65](https://github.com/cnuernber/dtype-next/issues/61) - Upgrade guava to remove conflict with
   clojurescript compiler.
 * `pmap`, `upmap` guaranteed *not* to require `shutdown-agents`.

## 9.032
 * unary version of tech.v3.datatype.functional min and max.
 * [issue 61](https://github.com/cnuernber/dtype-next/issues/61) - Fix annoying warnings for Clojure 1.11.

## 9.030
 * Documentation fixes (and correctness fixes) for gradient1d and readme note.
 * Added datatype entries for `:big-integer` and `:big-decimal`.


## 9.028
 * JSON parsing in this library is deprecated.  Please use the charred library for fast json and
   csv parsing.
 * The Tensor deftype is changed to DataTensor as the auto-generated ->Tensor function causes
   issues with the compilation of the `->tensor` public API function.  Also the function to
   create the data classes concretely have now been private.


## 9.022
 * fast json parsing - as fast as anything on the JVM and faster than most - with options for
   even faster if you don't need everything to be immutable.

## 9.021
 * `:trim-leading-whitespace?`, `:trim-trailing-whitespace?`, and `:nil-empty-values?` are
    all supported in csv parsing to bring feature set up to par with univocity.  These changes
	allow all tech.ml.dataset csv-based unit tests to succeed.

## 9.020
 * `:column-whitelist` and `:column-blacklist` are now supported for csv parsing.

## 9.019
 * Move to cached thread pool for queue-iter.

## 9.018
 * Fast [csv parsing](https://cnuernber.github.io/dtype-next/tech.v3.datatype.char-input.html).
 * Related, function to take an iterator and realize it into a queue in an offline thread
   returning a new iterator - [queue-iter](https://cnuernber.github.io/dtype-next/tech.v3.parallel.queue-iter.html).

## 9.017
 * `(argops/argfilter)` respects boolean/double-nan rules below.

## 9.016
 * Double/NaN, when casted to a boolean, evaluates to false.  This make the behavior of
   `(ds/filter-column col identity)` and `(dtype/elemwise-cast item :boolean)` consistent
   across float, double and all object datatypes.
 * insn is not loaded until required with requiring-resolve from the FFI system.  This
   allows you to pre-generate FFI bindings during build time but not require insn or
   `org.ow2.asm/asm` at runtime enabling greater compatibilty with hadoop-based systems
   which include a legacy version of `org.ow2.asm/asm` which is incompatible with
   insn.

## 9.015
 * tech.v3.parallel - adds ForkJoinPool/commonPool-based pmap, and upmap parallelism
 constructs that will bail and use map variants if already in a fork join thread.

## 9.013
 * `epoch-nanosecond` is an int64-aliased datatype.

## 9.012
 * Removed insn optimizations for tensor ops for now.  No one is using those pathways and
   it just increases startup time.
 * Removed logging around JDK-17 specific optimizations for vms < 17.
 * Rolling now can handle variables windows that are left, right, and centered relative
   window positions.

## 9.009
 * Added google analytics to documentation so we know which pages are getting referenced
   the most.

## 9.007
 * Removed DType's custom vector class - creating vectors with (vector) is fine up
   to 6 args which works for now.

## 9.006
 * Base Clj java API has faster map/vector creation.
 * DType now exposes map factory and fast vector creation.
 * A slightly optimized FastStruct is now a datatype java object.

## 9.005
 * Expose one of the most useful Clojure functions, repeatedly.

## 9.004
 * Expose tensor to neanderthal and nippy freeze/thaw of tensors.

## 9.002
 * Expose map,filter from clj interface.

## 9.001
 * Added tech.v3.datatype.Pred package to allow fast (in terms of typing) creation of typed
   unary predicates from java.

## 9.000
 * Changed API to rolling so that you can pass in a normal subtraction operator
   and get the result you expect.  This is reverse of how it worked in version
   8.064.


## 8.064
 * More niceties in the Clojure api.
 * Tensor broadcast is fixed which fixed mset!.

## 8.063
 * Added tech.v3.datatype.VecMath and tech.v3.datatype.Stats packages to the
   public Java API..

## 8.062
 * More types exposed in Clj.java.
 * compile pathway exposed in Clj.java.
 * indexMapReduce exposed in DType.java and an example given in java_test.

## 8.060
 * Fixed issue with makeTensor

## 8.059
 * Added simple descriptor buffer types for asArrayBuffer and asNativeBuffer so
   users can get to the data in the simplest possible way.

## 8.058
 * Java api includes nio buffer support by default.

## 8.057
 * Start of changelog.
 * Initial cut at java api.
