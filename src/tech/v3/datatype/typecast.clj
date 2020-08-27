(ns tech.v3.datatype.typecast
  (:require [tech.v3.datatype.casting :as casting])
  (:import [tech.v3.datatype BooleanList LongList DoubleList ObjectList]
           [java.util ArrayList List]))


(defn datatype->array-cls
  [datatype]
  (case (casting/host-flatten datatype)
    :boolean (Class/forName "[Z")
    :int8 (Class/forName "[B")
    :int16 (Class/forName "[S")
    :char (Class/forName "[C")
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
(defn as-char-array ^chars [item] item)
(defn as-int-array ^ints [item] item)
(defn as-long-array ^longs [item] item)
(defn as-float-array ^floats [item] item)
(defn as-double-array ^doubles [item] item)
(defn as-object-array ^objects [item] item)


(defmacro datatype->array
  [dtype java-ary]
  (case (casting/host-flatten dtype)
    :boolean `(as-boolean-array ~java-ary)
    :int8 `(as-byte-array ~java-ary)
    :int16 `(as-short-array ~java-ary)
    :char `(as-char-array ~java-ary)
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
    :char 'tech.v3.datatype.CharReader
    :int32 'tech.v3.datatype.IntReader
    :int64 'tech.v3.datatype.LongReader
    :float32 'tech.v3.datatype.FloatReader
    :float64 'tech.v3.datatype.DoubleReader
    :object 'tech.v3.datatype.ObjectReader))


(defn datatype->writer-type
  [dtype]
  (case (casting/safe-flatten dtype)
    :boolean 'tech.v3.datatype.BooleanWriter
    :int8 'tech.v3.datatype.ByteWriter
    :int16 'tech.v3.datatype.ShortWriter
    :char 'tech.v3.datatype.CharWriter
    :int32 'tech.v3.datatype.IntWriter
    :int64 'tech.v3.datatype.LongWriter
    :float32 'tech.v3.datatype.FloatWriter
    :float64 'tech.v3.datatype.DoubleWriter
    :object 'tech.v3.datatype.ObjectWriter))



(defn datatype->list-type
  "Returns a symbol that results to the list type"
  [datatype]
  (let [dtype (casting/host-flatten datatype)]
    (cond
      (= :boolean dtype) 'tech.v3.datatype.BooleanList
      (casting/integer-type? dtype) 'tech.v3.datatype.LongList
      (casting/float-type? dtype) 'tech.v3.datatype.DoubleList
      :else
      'tech.v3.datatype.ObjectList)))


(defn as-boolean-list ^BooleanList [item] item)
(defn as-long-list ^LongList [item] item)
(defn as-double-list ^DoubleList [item] item)
(defn as-object-list ^ObjectList [item] item)


(defmacro datatype->list
  [dtype java-list]
  (case (casting/host-flatten dtype)
    :boolean `(as-boolean-list ~java-list)
    :int8 `(as-long-list ~java-list)
    :int16 `(as-long-list ~java-list)
    :char `(as-long-list ~java-list)
    :int32 `(as-long-list ~java-list)
    :int64 `(as-long-list ~java-list)
    :float32 `(as-double-list ~java-list)
    :float64 `(as-double-list ~java-list)
    :object `(as-object-list ~java-list)))
