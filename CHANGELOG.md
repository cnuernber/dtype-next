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
