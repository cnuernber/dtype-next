# dtype-next Overview


## Setup

```clojure
(require '[tech.v3.datatype :as dtype])
```

## Containers

There are two different types of containers in tech.v3.datatype; jvm-heap containers
and native-heap containers.  Object datatypes are only supported in jvm-heap containers
and native-heap containers have mmap support and offer zero-copy pathways to toolkits like
Python's numpy and Clojure's neanderthal.


```clojure
user> (require '[tech.v3.datatype :as dtype])
nil
user> (dtype/make-container :jvm-heap :float32 5)
#array-buffer<float32>[5]
[0.000, 0.000, 0.000, 0.000, 0.000, ]
user> (dtype/make-container :jvm-heap :float32 (range 5))
#array-buffer<float32>[5]
[0.000, 1.000, 2.000, 3.000, 4.000, ]
user> (dtype/make-container :native-heap :float32 (range 5))
#native-buffer@0x00007F665489BAE0<float32>[5]
[0.000, 1.000, 2.000, 3.000, 4.000, ]


user> ;;These containers support Indexed and read-only access via IFn
user> (def data (dtype/make-container :jvm-heap :float32 (range 5)))
#'user/data
user> (data 0)
0.0
user> (nth data 2)
2.0
```

## Copy

Heavily optimized copying for bulk transfers.   Use this also for completing
lazy operations.


```clojure
;; You will not see major efficiency gains until both sides the copy
;; operation is backed by
user> (dtype/copy! (range 10) (float-array 10))
[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
user> (type *1)
[F


;; Fastest (by factor of 100 discounting array creation)
user> (dtype/copy! (float-array (range 10)) (float-array 10))
[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]


;; Also extremely fast! native->jvm-array transfer is an optimized
;; operation supported by graal native pathways via the sun.misc.Unsafe pathway
user> (dtype/copy! (dtype/make-container :native-buffer :float32  (range 10)) (float-array 10))
[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]


;; Copy also supports marshalling data between containers using C casting rules - floating values
;; are floored if ceiled depending on which size of zero they are on.

user> (dtype/copy! (dtype/make-container :native-heap :float32  [0 1 254])
                   (dtype/make-container :jvm-heap :uint8 3))
#array-buffer<uint8>[3]
[0, 1, 254, ]

user>
```

## Buffers, Readers, Writers

Efficent primitive-datatype-aware access is provided via the [buffer interface](https://github.com/cnuernber/dtype-next/blob/d04c309bd565292c1c3d9880b4bbb80b6ff9478e/java/tech/v3/datatype/Buffer.java).  A reader is a buffer
that can be read from and a writer is a buffer that can be written to -- note the buffer interface has 'canRead' and 'canWrite' methods.

Buffers have many transformations such as adding a scalar or element-wise adding the elements of another buffer.  A few of these operations
are exposed via the [tech.v3.datatype.functional](https://cnuernber.github.io/dtype-next/tech.v3.datatype.functional.html) interface and those
are discussed in depth below.

Most things are convertible to a reader of a specific type.  So things like persistent vectors, java arrays, numpy arrays, etc. are convertible
to readers.  Some things are convertible to concrete buffers such as jvm heap containers (arrays) and native heap containers.  Any reader
can be `reshape`d into a tensor.


## Math

A small sample of what is available.  Basic elementwise operations along
with some statistics and a fixed rolling windowing facility.


### Container Coersion Rules

Rules are applied in order.


1. If the arguments contain an iterable, an iterable is returned.
1. If the arguments contain a reader, a reader is returned.  If the input reader had a shape with more
   than one dimension, a tensor is returned.
1. Else a scalar is returned.

1,2,and many argument versions of these rules part of the public tech.v3.datatype api so they are
reused for other generalized functions.  These functions are:

* `vectorized-dispatch-1` - single argument dispatch pathway.
* `vectorized-dispatch-2` - double argument dispatch pathway.
* `emap` - generalized n-dimension implementation for n arguments.


### Other Details

* All arguments are casted to the widest datatype present.
* Readers implement List and RandomAccess so they look like persistent vectors in the repl.


```clojure
user> (require '[tech.v2.datatype.functional :as dtype-fn])
nil
user> (def test-data (dtype-fn/+ (range 10 0 -1) 5))
#'user/test-data
user> test-data
(15 14 13 12 11 10 9 8 7 6)


user> ;;Working in index space is often ideal when dealing with datasets described this way.
user> ;;To help with that we have a few tools.
user> (require '[tech.v3.datatype.argops :as argops])
nil
user> (argops/argsort > test-data )
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
user> (argops/argsort < test-data )
[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]



user> ;;once you have a set of indexes, you can reindex your original reader
user> (dtype/indexed-buffer indexes test-data)
[6 7 8 9 10 11 12 13 14 15]


user> ;;dfn has several functions for descriptive statistics
user> (dfn/variance test-data)
9.166666666666666
user> (dfn/descriptive-statistics test-data)
{:min 6.0,
 :mean 10.5,
 :standard-deviation 3.0276503540974917,
 :max 15.0,
 :n-values 10}
```

## Dates & Times

All of the constructors for the types are in `tech.v3.datatype.datetime`.  All your
favorite `java.time` types are there.
```clojure
user> (require '[tech.v3.datatype.datetime :as dtype-dt])
user> (dtype-dt/zoned-date-time)
#object[java.time.ZonedDateTime 0x190aea6a "2020-09-24T16:00:27.211-06:00[America/Denver]"]
user> (dtype-dt/local-date)
#object[java.time.LocalDate 0x39080aab "2020-09-24")
user> (dtype-dt/plus-temporal-amount (dtype-dt/local-date) 2 :days)
#object[java.time.LocalDate 0x680586f8 "2020-09-26"]



user> (dtype-dt/plus-temporal-amount (dtype-dt/local-date) (range 10) :days)
[#object[java.time.LocalDate 0x56bd7a17 "2020-09-24"] #object[java.time.LocalDate 0x47edcb55 "2020-09-25"] #object[java.time.LocalDate 0x3ab1a911 "2020-09-26"] #object[java.time.LocalDate 0x5a75b76 "2020-09-27"] #object[java.time.LocalDate 0x38f7e417 "2020-09-28"] #object[java.time.LocalDate 0x1169273f "2020-09-29"] #object[java.time.LocalDate 0x26a7ed22 "2020-09-30"] #object[java.time.LocalDate 0x19646e6b "2020-10-01"] #object[java.time.LocalDate 0x1eb4e60c "2020-10-02"] #object[java.time.LocalDate 0x463c6a3a "2020-10-03"]]
user> (def offset-local-dates (dtype-dt/plus-temporal-amount (dtype-dt/local-date) (range 10) :days))
#'user/offset-local-dates
user> (dtype-dt/long-temporal-field :days offset-local-dates)
[24 25 26 27 28 29 30 1 2 3]
user> (dtype-dt/long-temporal-field :day-of-week offset-local-dates)
[4 5 6 7 1 2 3 4 5 6]
user> (dtype-dt/long-temporal-field :day-of-year offset-local-dates)
[268 269 270 271 272 273 274 275 276 277]


;;there is also a generalized conversion mechanism to milliseconds-since-epoch and back.  This conversion
;;can be timezone aware when one is passed in and it uses UTC when no timezone is passed.

user> (dtype-dt/datetime->milliseconds offset-local-dates)
[1600905600000 1600992000000 1601078400000 1601164800000 1601251200000 1601337600000 1601424000000 1601510400000 1601596800000 1601683200000]
user> (dtype-dt/system-zone-id)
#object[java.time.ZoneRegion 0x10bdee8e "America/Denver"]
user> (dtype-dt/datetime->milliseconds (dtype-dt/system-zone-id) offset-local-dates)
[1600927200000 1601013600000 1601100000000 1601186400000 1601272800000 1601359200000 1601445600000 1601532000000 1601618400000 1601704800000]
```


## ND Buffers

Generic N-dimensional support is provied both based on raw containers and based on functions from N indexes to a value.
These objects implement the [NDBuffer interface](https://github.com/cnuernber/dtype-next/blob/d04c309bd565292c1c3d9880b4bbb80b6ff9478e/java/tech/v3/datatype/NDBuffer.java) and the support is provided
primarily via the 'tensor' namespace.  The basic ways of dealing with tensor are:


* `->tensor` - Make a new tensor by copying data into a container.  This can take sequences, persistent vectors, etc. and
  will copy the data into a contiguous buffer and return a new implementation of NDBuffer based on that data.
* `ensure-tensor`- Attempt in-place transformation of data falling back to `->tensor` when not available.
* `compute-tensor` - Create a new tensor via a function that takes `rank` long indexes and returns a value.
* `reshape` - reshape a buffer or reader into a different dimension and rank via linear reinterpretation of the data.  Use
this to convert a normal bit of data such as a java array or a persistent vector into a tensor without copying the data.
* `transpose` - Permute data via reordering dimensions.
* `select` - Select a subrect of data.  Select can also take indexes in order to do ad-hoc reordering of the data.
* `broadcast` - Create a larger tensor via duplication along one or more dimensions.


These tech.v3.datatype functions are also useful for tensors:


* `elemwise-cast` - change datatype of tensor.
* `clone` - Create a new tensor of same shape and data.  This can be used to realize lazy operations.
* `->buffer,->reader` - Get the data in 1D row-major vector.


```clojure
user>  (require '[tech.v3.tensor :as dtt])
nil
user> (dtt/->tensor (partition 3 (range 9)))
#tech.v3.tensor<object>[3 3]
[[0 1 2]
 [3 4 5]
 [6 7 8]]
```

Tensor implement various java/clojure interfaces they interact naturally with the language.
```clojure
user> (def tens (dtt/->tensor (partition 3 (range 9))))
#'user/tens
user> (tens 0)
#tech.v3.tensor<object>[3]
[0 1 2]
user> (tens 1)
#tech.v3.tensor<object>[3]
[3 4 5]
user> (nth tens 2)
#tech.v3.tensor<object>[3]
[6 7 8]
[#tech.v3.tensor<object>[3]
[0 3 6] #tech.v3.tensor<object>[3]
[1 4 7] #tech.v3.tensor<object>[3]
[2 5 8]]
user> (dtt/transpose tens [1 0])
#tech.v3.tensor<object>[3 3]
[[0 3 6]
 [1 4 7]
 [2 5 8]]
```

You can get/set subrects at a given time using mget/mset! pathways.  If you provide fewer indexes
than the required indexes they will either return or assign values to those sections.

```clojure
user> (def tens (dtt/->tensor (partition 3 (range 9))))
#'user/tens
user> (dtt/mget tens 0)
#tech.v3.tensor<object>[3]
[0 1 2]
user> (dtt/mset! tens 0 25)
#tech.v3.tensor<object>[3 3]
[[25 25 25]
 [ 3  4  5]
 [ 6  7  8]]
user> (dtt/mset! tens 0 [25 26 27])
#tech.v3.tensor<object>[3 3]
[[25 26 27]
 [ 3  4  5]
 [ 6  7  8]]
user> (dtt/transpose tens [1 0])
#tech.v3.tensor<object>[3 3]
[[25 3 6]
 [26 4 7]
 [27 5 8]]
user> (dtt/mset! (dtt/transpose tens [1 0]) 0 [25 26 27])
#tech.v3.tensor<object>[3 3]
[[25 26 27]
 [26  4  7]
 [27  5  8]]
```


If you got this far be sure to checkout the [cheatsheet](https://cnuernber.github.io/dtype-next/cheatsheet.html).
