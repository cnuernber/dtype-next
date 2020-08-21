(ns tech.v3.datatype.typecast
  (:require [tech.v3.datatype.casting :as casting]))


(defn datatype->array-cls
  [datatype]
  (case datatype
    :boolean (Class/forName "[Z")
    :int8 (Class/forName "[B")
    :int16 (Class/forName "[S")
    :character (Class/forName "[C")
    :int32 (Class/forName "[I")
    :int64 (Class/forName "[J")
    :float32 (Class/forName "[F")
    :float64 (Class/forName "[D")
    (Class/forName "[Ljava.lang.Object;")))


(defn is-array-type?
  [item]
  (when item
    (.isArray (.getClass ^Object item))))


(defn as-boolean-array ^booleans [item] item)
(defn as-byte-array ^bytes [item] item)
(defn as-short-array ^shorts [item] item)
(defn as-int-array ^ints [item] item)
(defn as-long-array ^longs [item] item)
(defn as-float-array ^floats [item] item)
(defn as-double-array ^doubles [item] item)
(defn as-object-array ^objects [item] item)


(defmacro datatype->array
  [dtype java-ary]
  (case dtype
    :boolean `(as-boolean-array ~java-ary)
    :int8 `(as-byte-array ~java-ary)
    :int16 `(as-short-array ~java-ary)
    :int32 `(as-int-array ~java-ary)
    :int64 `(as-long-array ~java-ary)
    :float32 `(as-float-array ~java-ary)
    :float64 `(as-double-array ~java-ary)
    :object `(as-object-array ~java-ary)))


(defn datatype->reader-type
  [dtype]
  (case (casting/safe-flatten dtype)
    :boolean 'tech.v3.datatype.BooleanReader
    :int8 'tech.v3.datatype.ByteReader
    :int16 'tech.v3.datatype.ShortReader
    :int32 'tech.v3.datatype.IntReader
    :int64 'tech.v3.datatype.LongReader
    :float32 'tech.v3.datatype.FloatReader
    :float64 'tech.v3.datatype.DoubleReader
    :object 'tech.v3.datatype.ObjectReader))
