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
