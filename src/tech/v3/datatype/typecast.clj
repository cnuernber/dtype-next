(ns tech.v3.datatype.typecast
  (:require [tech.v3.datatype.casting :as casting])
  (:import [tech.v3.datatype Buffer]
           [java.util Map]))


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


(defn datatype->io-type
  [dtype]
  (case (casting/safe-flatten dtype)
    :boolean 'tech.v3.datatype.BooleanBuffer
    :int8 'tech.v3.datatype.LongBuffer
    :int16 'tech.v3.datatype.LongBuffer
    :char 'tech.v3.datatype.LongBuffer
    :int32 'tech.v3.datatype.LongBuffer
    :int64 'tech.v3.datatype.LongBuffer
    :float32 'tech.v3.datatype.DoubleBuffer
    :float64 'tech.v3.datatype.DoubleBuffer
    :object 'tech.v3.datatype.ObjectBuffer))


(defn datatype->reader-type
  [dtype]
  (case (casting/safe-flatten dtype)
    :boolean 'tech.v3.datatype.BooleanReader
    :int8 'tech.v3.datatype.LongReader
    :int16 'tech.v3.datatype.LongReader
    :char 'tech.v3.datatype.ObjectReader
    :int32 'tech.v3.datatype.LongReader
    :int64 'tech.v3.datatype.LongReader
    :float32 'tech.v3.datatype.DoubleReader
    :float64 'tech.v3.datatype.DoubleReader
    :object 'tech.v3.datatype.ObjectReader))


(defn datatype->writer-type
  [dtype]
  (case (casting/safe-flatten dtype)
    :boolean 'tech.v3.datatype.BooleanWriter
    :int8 'tech.v3.datatype.LongWriter
    :int16 'tech.v3.datatype.LongWriter
    :char 'tech.v3.datatype.ObjectWriter
    :int32 'tech.v3.datatype.LongWriter
    :int64 'tech.v3.datatype.LongWriter
    :float32 'tech.v3.datatype.DoubleWriter
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


(defn as-buffer-list ^Buffer [item] item)


(defn as-java-map
  ^Map [item]
  (when (and item (instance? Map item))
    item))


(defn ->java-map
  ^Map [item]
  (if-let [jmap (as-java-map item)]
    jmap
    (throw (Exception. (str "Item is not a map: %s" item)))))
