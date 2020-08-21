(ns tech.v3.datatype.typecast)

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

(defn as-double-array ^doubles [item] item)

(defmacro datatype->array
  [dtype java-ary]
  (case dtype
    :float64 `(as-double-array ~java-ary)))

(defn datatype->reader-type
  [dtype]
  (case dtype
    :float64 'tech.v3.datatype.DoubleReader))
