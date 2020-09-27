# Why dtype-next?


## tech.datatype


tech.datatype as a numerics stack fulfills our technical needs at TechAscent in regards
to scientific computing, data science, and machine learning.  It enables a completely
unified interface between native heap and JVM heap datastructures with a base level
of datatype support and various simple accelerated operations.


In order to fulfill unified support for efficient random access across many datatypes
it uses many type specific constructions that enable primitive reading/writing to
buffers with disparate storage backends.


One drawback of this style of implementaion is an explosion of compile-time
interfaces; interfaces that efficiently allow, for example, reading or writing a
primitive double at a particular address in the buffer.  On top of this is a
marshalling stack, so roughly n^2 implementation of type transformations from, for
example, float to integer.  Each operation, such as `+` or `indexed-reader`, is
specialized to precisely one datatype with a specific interface implementation for each
specialization.


These small specialization classes end up having a disproportionately high cost in terms
of both uberjar size and in terms of application startup time; essentially the compile
time programming to specialize things to a specific datatype is costly at require
time *even with AOT*.  Just the classloader time required to load the class explosion
caused by the roughly cartesian join of Y operations by X classes is substantial not
to mention the understanding of compile time programming required to efficiently
generate specializations of new operations.


What this design *did* achieve, however, was substantial generalized performance as
exhibited in our professional engagements of the type that is very expensive to
achieve with any JVM language.  It also enabled the efficient integration with tools
such as TVM, Numpy, and the creation of a dataframe library with competitive performance
with C toolkits.


## Towards A Different Implementation Pathway


The problem is specifically how to implement type specific code that is both efficient
in runtime *and* compile time performance.  Having achieved runtime performance now it
is time to focus on compile time performance.  We want to produce the same result with
less code both in implementation and far smaller overall compile time results in this
case those results relate nearly directly to time to intial use which is important
in specific key use cases.


There are a few pathways to this result but the one that appears to have the most
potential is a simple one.  If, instead of a reader implementing only one
type-specific read method, readers implemented every type specific read method with
default casting rules between the primitive datatypes we can actually, for the same
implementation cost as measured by the cost to implement a reader of a particular
datatype, gain identical performance with far more runtime flexibility resulting in
less need in the first place for compile time case statements generating the
explosion of small classes that bloat both require time and uberjar size.


Here is an example.  In tech.datatype, a DoubleReader implements a single read method
that reads a long at a particular address:

```java
public interface DoubleReader
{
  long lsize();
  double read(long idx);
}
```

In dtype-next there exists a new concept called a Buffer:

```java
public interface Buffer extends IOBase, Iterable, IFn,
				             List, RandomAccess, Sequential,
                             Indexed
{
  boolean readBoolean(long idx);
  byte readByte(long idx);
  short readShort(long idx);
  char readChar(long idx);
  int readInt(long idx);
  long readLong(long idx);
  float readFloat(long idx);
  double readDouble(long idx);
  Object readObject(long idx);
  void writeBoolean(long idx, boolean val);
  void writeByte(long idx, byte val);
  void writeShort(long idx, short val);
  void writeChar(long idx, char val);
  void writeInt(long idx, int val);
  void writeLong(long idx, long val);
  void writeFloat(long idx, float val);
  void writeDouble(long idx, double val);
  void writeObject(long idx, Object val);
  default boolean allowsRead() { return true; }
  default boolean allowsWrite() { return false; }
  //Lots of implementation of the above interfaces based on these methods.
  ...
}
```

With this concept in mind, a DoubleIO implements this interface using a
combination of checked runtime casting and the original `double read(long idx)`
method:

```java
public interface DoubleIO extends Buffer
{
  default Object elemwiseDatatype () { return Keyword.intern(null, "float64"); }
  default boolean readBoolean(long idx) {return readDouble(idx) != 0.0;}
  default byte readByte(long idx) {return RT.byteCast(readDouble(idx));}
  default short readShort(long idx) {return RT.byteCast(readDouble(idx));}
  default char readChar(long idx) {return RT.charCast(readDouble(idx));}
  default int readInt(long idx) {return RT.intCast(readDouble(idx));}
  default long readLong(long idx) {return RT.longCast(readDouble(idx));}
  default float readFloat(long idx) {return (float)readDouble(idx);}
  default Object readObject(long idx) {return readDouble(idx);}

  //Write interaces implemented below
  ...
}
```

This means we don't need to create a specific reader to convert a double reader into
a long reader.  Thus we no longer have a cartesian join of required
interfaces but rather we have 'wider' default interface implementations.

For many situations, due to the wider interfaces, we can now implement 1 interface
with many methods as opposed to many interfaces with 1 method.


## Additional Optimizations


Java classes have to be a minimum of 4K in size regardless of the amount of code
required to implement them and they grow in 4K amounts.  A back-of-the-hand
measurement indicated that loading more classes was relatively more expensive than
loading a few, larger classes.


Knowing this, we can slowly move out in the datatype world being extremely careful
at each step to only generate classes where it is essential for either performance
or correctness.  Here are a set of further optimizations found so far:



*  Buffer-specific implementations are mimimized and buffer's implement a single
class that provides the Buffer implementation.  For example
there is a single class implementation that provides typesafe access to byte buffers
and another that provides typesafe access for byte buffers that are to be interpreted
as unsigned byte data.  That cuts out half the implementations of readers and writers.
*  There can be a single const-reader implementation as opposed to N implementations,
one for each datatype.  Ditto for indexed Buffer implementations which are one
of the most heavily used items in tech.datatype.
*  The arithmetic math vectorization implementation implements 3 overloads - one for
double, one for long, and one for object.
*  The + operator implements one class.  In tech.datatype it implemented a class
for each individual numeric datatype plus one override for Object.


## Benchmarks


### API Require Time - No AOT

```clojure
user> (time (require 'tech.v2.datatype))
"Elapsed time: 6752.785449 msecs"
nil
```

```clojure
user> (time (require 'tech.v3.datatype))
"Elapsed time: 2142.680631 msecs"
nil
```


### Tensor Require time - No AOT

```clojure
user> (time (require 'tech.v2.tensor))
"Elapsed time: 8696.394848 msecs"
nil
```

```clojure
user> (time (require 'tech.v3.tensor))
"Elapsed time: 3516.401243 msecs"
nil
```

### API Require Time - With AOT

```clojure
user> (time (require 'tech.v2.datatype))
"Elapsed time: 1231.607731 msecs"
nil
```

```clojure
user> (time (require 'tech.v3.datatype))
"Elapsed time: 525.53414 msecs"
nil
```

### Tensor Require Time - With AOT

```clojure
user> (time (require 'tech.v2.tensor))
"Elapsed time: 1478.459059 msecs"
nil
```

```clojure
user> (time (require 'tech.v3.tensor))
"Elapsed time: 713.795019 msecs"
nil
```

### Tech Platform AOT Uberjar Size

```console
chrisn@chrisn-lt-01:~/dev/tech.all/tech.datatype$ du -hs target/classes/tech
26M     target/classes/tech
```

```console
chrisn@chrisn-lt-01:~/dev/cnuernber/dtype-next$ du -hs target/classes/tech
9.1M    target/classes/tech
```
