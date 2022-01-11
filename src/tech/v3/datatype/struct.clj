(ns tech.v3.datatype.struct
  "Structs are datatypes composed of primitive datatypes or other structs.
  Similar to records except they do not support string or object columns,
  only numeric values.  They have memset-0 initialization, memcpy copy
  semantics.  For correct equals, hashing, convert struct into a normal
  persistent map via `into`.

  Example:

```clojure
user> (require '[tech.v3.datatype :as dtype])
nil
user> (require '[tech.v3.datatype.struct :as dt-struct])
nil
user> (dt-struct/define-datatype! :vec3 [{:name :x :datatype :float32}
                                         {:name :y :datatype :float32}
                                         {:name :z :datatype :float32}])
{:datatype-size 12,
 :datatype-width 4,
 :data-layout
 [{:name :x, :datatype :float32, :offset 0, :n-elems 1}
  {:name :y, :datatype :float32, :offset 4, :n-elems 1}
  {:name :z, :datatype :float32, :offset 8, :n-elems 1}],
 :layout-map
 {:x {:name :x, :datatype :float32, :offset 0, :n-elems 1},
  :y {:name :y, :datatype :float32, :offset 4, :n-elems 1},
  :z {:name :z, :datatype :float32, :offset 8, :n-elems 1}},
 :datatype-name :vec3}
user> (dt-struct/new-struct :vec3)
{:x 0.0, :y 0.0, :z 0.0}
user> (.put *1 :x 2.0)
nil
user> *2
{:x 2.0, :y 0.0, :z 0.0}
```"
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [com.github.ztellman.primitive-math :as pmath])
  (:import [tech.v3.datatype BinaryBuffer ObjectBuffer BooleanBuffer
            LongBuffer DoubleBuffer]
           [java.util.concurrent ConcurrentHashMap]
           [java.util RandomAccess List Map LinkedHashSet Collection]
           [clojure.lang MapEntry IObj IFn ILookup]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defonce ^:private struct-datatypes (ConcurrentHashMap.))


(defn datatype-size
  "Return the size, in bytes, of a datatype."
  ^long [datatype]
  (if-let [struct-dtype (.get ^ConcurrentHashMap struct-datatypes datatype)]
    (long (:datatype-size struct-dtype))
    (-> (casting/datatype->host-type datatype)
        (casting/numeric-byte-width))))


(defn datatype-width
  "Return the width or the of a datatype.  The width dictates what address
  the datatype can start at when embedded in another datatype."
  ^long [datatype]
  (if-let [struct-dtype (.get ^ConcurrentHashMap struct-datatypes datatype)]
    (long (:datatype-width struct-dtype))
    (-> (casting/datatype->host-type datatype)
        (casting/numeric-byte-width))))


(defn- widen-offset
  "Ensure the offset starts at the appropriate boundary for the datatype width."
  ^long [^long offset ^long datatype-width]
  (let [rem-result (rem offset datatype-width)]
    (if (== 0 rem-result)
      offset
      (+ offset (- datatype-width rem-result)))))


(defn struct-datatype?
  "Returns true of this datatype denotes a struct datatype."
  [datatype]
  (.containsKey ^ConcurrentHashMap struct-datatypes datatype))


(defn- layout-datatypes
  [datatype-seq]
  (let [[datatype-seq widest-datatype current-offset]
        (->> datatype-seq
             (reduce (fn [[datatype-seq
                           widest-datatype
                           current-offset]
                          {:keys [name datatype n-elems] :as entry}]
                       (let [n-elems (long (if n-elems n-elems 1))
                             dtype-width (datatype-width datatype)
                             dtype-size (* (datatype-size datatype) n-elems)
                             dtype-width (min 8 dtype-width)
                             current-offset (long current-offset)
                             widest-datatype (max (long widest-datatype) dtype-width)
                             current-offset (widen-offset current-offset dtype-width)]
                         (when-not name
                           (throw (Exception.
                                   "Datatypes must all be named at this point.")))
                         [(conj datatype-seq (assoc entry
                                                    :offset current-offset
                                                    :n-elems n-elems))
                          widest-datatype
                          (+ current-offset dtype-size)]))
                     [[] 1 0]))
        current-offset (long current-offset)
        widest-datatype (long widest-datatype)
        datatype-size (widen-offset current-offset widest-datatype)]
    {:datatype-size datatype-size
     :datatype-width widest-datatype
     :data-layout datatype-seq
     :layout-map (->> datatype-seq
                      (map (juxt :name identity))
                      (into {}))}))


(defn get-struct-def
  "Get a previously constructed struct definition."
  [datatype]
  (if-let [retval (.get ^ConcurrentHashMap struct-datatypes datatype)]
    retval
    (throw (Exception. (format "Datatype %s is not a struct definition." datatype)))))


(defn- get-datatype
  [datatype-name]
  (.getOrDefault ^ConcurrentHashMap struct-datatypes
                 datatype-name datatype-name))


(defn offset-of
  "Returns a tuple of [offset dtype]."
  [struct-dtype-or-struct-def property-vec]
  (let [{:keys [layout-map] :as struct-def}
        (if (map? struct-dtype-or-struct-def)
          struct-dtype-or-struct-def
          (get-struct-def struct-dtype-or-struct-def))]
    (if-not (instance? RandomAccess property-vec)
      (if-let [retval (get layout-map property-vec)]
        [(:offset retval) (:datatype retval)]
        (throw (Exception. (format "Property not found: %s" property-vec))))
      (let [^List property-vec (if-not (instance? RandomAccess property-vec)
                                 [property-vec]
                                 property-vec)
            n-lookup (count property-vec)]
        (loop [idx 0
               n-prop-elems 0
               prop-datatype nil
               struct-def struct-def
               offset 0]
          (if (< idx n-lookup)
            (let [next-val (.get property-vec idx)
                  [offset
                   struct-def
                   n-prop-elems
                   prop-datatype]
                  (if (number? next-val)
                    (let [next-val (long next-val)]
                      (when-not (< next-val n-prop-elems)
                        (throw (Exception. "Indexed property access out of range")))
                      [(+ offset (* next-val (long (datatype-size prop-datatype))))
                       (get-datatype prop-datatype)
                       0
                       prop-datatype])
                    (if-let [data-val (get-in struct-def [:layout-map next-val])]
                      [(+ offset (long (:offset data-val)))
                       (get-datatype (:datatype data-val))
                       (long (:n-elems data-val))
                       (:datatype data-val)]
                      (throw (Exception.
                              (format "Could not find property %s in %s"
                                      next-val (:datatype-name struct-def))))))]
              (recur (inc idx) (long n-prop-elems) prop-datatype
                     struct-def (long offset)))
            [offset prop-datatype]))))))


(defn define-datatype!
  "Define a new struct datatype.

  * `datatype-name` - keyword datatype name.
  * `datatype-seq` - Sequence of maps with the keys `{:name :datatype}`
     which describe the new datatype.

  Returns the new struct defintion.

  Example:

```clojure
(define-datatype! :vec3 [{:name :x :datatype :float32}
                         {:name :y :datatype :float32}
                         {:name :z :datatype :float32}])

(define-datatype! :segment [{:name :begin :datatype :vec3}
                            {:name :end :datatype :vec3}])
```"
  [datatype-name datatype-seq]
  (let [new-datatype (-> (layout-datatypes datatype-seq)
                         (assoc :datatype-name datatype-name))]
    (.put ^ConcurrentHashMap struct-datatypes datatype-name new-datatype)
    new-datatype))


(declare inplace-new-struct)


(defmacro ^:private ensure-binary-buffer!
  []
  `(do
     (when-not ~'cached-buffer
       (set! ~'cached-buffer (dtype-base/->binary-buffer ~'buffer)))
     ~'cached-buffer))


(deftype ^{:doc "Struct instance.  Derives most-notably from `java.util.Map` and
 `clojure.lang.ILookup`.  Metadata (meta, with-meta, vary-meta) is supported.
  Imporant member variables are:

 * `.struct-def - The struct definition of this instance.
 * `.buffer - The underlying backing store of this instance."}
 Struct [struct-def
         buffer
         ^{:unsynchronized-mutable true
           :tag BinaryBuffer} cached-buffer
         metadata]
  dtype-proto/PDatatype
  (datatype [_m] (:datatype-name struct-def))
  dtype-proto/PECount
  (ecount [_m] (dtype-proto/ecount buffer))
  dtype-proto/PEndianness
  (endianness [_m] (dtype-proto/endianness buffer))
  dtype-proto/PClone
  (clone [_m]
    (let [new-buffer (dtype-proto/clone buffer)]
      (inplace-new-struct (:datatype-name struct-def) new-buffer
                          {:endianness
                           (dtype-proto/endianness buffer)})))

  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [_this]
    (dtype-proto/convertible-to-native-buffer? buffer))
  (->native-buffer [_this]
    (dtype-proto/->native-buffer buffer))

  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [_this]
    (dtype-proto/convertible-to-array-buffer? buffer))
  (->array-buffer [_this]
    (dtype-proto/->array-buffer buffer))

  IObj
  (meta [_this] metadata)
  (withMeta [_this m]
    (Struct. struct-def buffer cached-buffer m))

  ILookup
  (valAt [this k] (.get this k))
  (valAt [this k not-found] (.getOrDefault this k not-found))

  IFn
  (invoke [this k] (.get this k))
  (applyTo [this args]
    (errors/when-not-errorf
     (= 1 (count args))
     "only 1 arg is acceptable; %d provided" (count args))
    (.get this (first args)))

  Map
  (size [_m] (count (:data-layout struct-def)))
  (containsKey [_m k] (.containsKey ^Map (:layout-map struct-def) k))
  (entrySet [m]
    (let [map-entry-data (map (comp #(MapEntry. % (.get m %)) :name)
                              (:data-layout struct-def))]
      (LinkedHashSet. ^Collection map-entry-data)))
  (keySet [_m] (.keySet ^Map (:layout-map struct-def)))
  (get [_m k]
    (when-let [[offset dtype :as _data-vec] (offset-of struct-def k)]
      (if-let [struct-def (.get ^ConcurrentHashMap struct-datatypes dtype)]
        (let [new-buffer (dtype-proto/sub-buffer
                          buffer
                          offset
                          (:datatype-size struct-def))]
          (inplace-new-struct dtype new-buffer
                              {:endianness (dtype-proto/endianness buffer)}))
        (let [host-dtype (casting/host-flatten dtype)
              reader (ensure-binary-buffer!)
              value
              (case host-dtype
                :int8 (.readBinByte reader offset)
                :int16 (.readBinShort reader offset)
                :int32 (.readBinInt reader offset)
                :int64 (.readBinLong reader offset)
                :float32 (.readBinFloat reader offset)
                :float64 (.readBinDouble reader offset))]
          (if (= host-dtype dtype)
            value
            (casting/unchecked-cast value dtype))))))
  (getOrDefault [m k d]
    (or (.get m k) d))
  (put [m k v]
    (let [writer (ensure-binary-buffer!)]
      (when-not (.allowsBinaryWrite writer)
        (throw (Exception. "Item is immutable")))
      (if-let [[offset dtype :as _data-vec] (offset-of struct-def k)]
        (if-let [struct-def (.get ^ConcurrentHashMap struct-datatypes dtype)]
          (let [_ (when-not (and (instance? Struct v)
                                 (= dtype (dtype-proto/datatype v)))

                    (throw (Exception. (format "non-struct or datatype mismatch: %s %s"
                                               dtype (dtype-proto/datatype v)))))]
            (dtype-cmc/copy! (.buffer ^Struct v)
                             (dtype-proto/sub-buffer buffer offset
                                                     (:datatype-size struct-def)))
            nil)
          (let [v (casting/cast v dtype)
                host-dtype (casting/host-flatten dtype)]
            (case host-dtype
              :int8 (.writeBinByte writer offset (pmath/byte v))
              :int16 (.writeBinShort writer offset (pmath/short v))
              :int32 (.writeBinInt writer offset (pmath/int v))
              :int64 (.writeBinLong writer offset (pmath/long v))
              :float32 (.writeBinFloat writer offset (pmath/float v))
              :float64 (.writeBinDouble writer offset (pmath/double v)))))
        (throw (Exception. (format "Datatype %s does not containt field %s"
                                   (dtype-proto/datatype m) k)))))))


(defn inplace-new-struct
  "Create a new struct in-place in the backing store.  The backing store must
  either be convertible to a native buffer or a byte-array.

  Returns a new Struct datatype."
  (^Struct [datatype backing-store _options]
   (let [struct-def (get-struct-def datatype)]
     (Struct. struct-def backing-store nil {})))
  (^Struct [datatype backing-store]
   (inplace-new-struct datatype backing-store {})))


(defn new-struct
  "Create a new struct.  By default this will use a byte array (:jvm-heap memory).
  Returns a new struct.

  Options are passed into dtype/make-container so container-specific options apply.

  Options:

  * `:container-type` - Defaults to `:jvm-heap` but often you want `:native-heap`
  * `:resource-type` - If `:native-heap` `:container-type` is chosen, this dictates
    the resource strategy.  Options are the same as
    `tech.v3.datatype.native-buffer/malloc`."
  (^Struct [datatype options]
   (let [struct-def (get-struct-def datatype)
         ;;binary read/write to nio buffers is faster than our writer-wrapper
         backing-data (dtype-cmc/make-container
                       (:container-type options :jvm-heap)
                       :int8
                       options
                       (long (:datatype-size struct-def)))]
     (Struct. struct-def backing-data nil options)))
  (^Struct [datatype]
   (new-struct datatype {})))


(declare inplace-new-array-of-structs)


(defn- assign-struct!
  [value struct-def dst-buf]
  (let [value-type (dtype-proto/datatype value)
        array-type (:datatype-name struct-def)]
    (when-not (= value-type (:datatype-name struct-def))
      (throw (Exception. (format "Array %s/value %s mismatch"
                                 array-type value-type))))
    (when-not (instance? Struct value)
      (throw (Exception. (format "Value does not appear to be a struct: %s"
                                 (type value)))))
    (let [^Struct value value]
      (dtype-cmc/copy! (.buffer value) dst-buf))))


;;Work in progress.  This creates a set of columns, not unlike a dataset from an
;;array of structs.
(deftype ArrayOfStructs [struct-def
                         ^long elem-size
                         ^long n-elems
                         buffer
                         metadata]
  dtype-proto/PEndianness
  (endianness [_ary] (dtype-proto/endianness buffer))

  dtype-proto/PClone
  (clone [_ary]
    (inplace-new-array-of-structs (:datatype-name struct-def)
                                  (dtype-proto/clone buffer)
                                  metadata))
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [_this]
    (dtype-proto/convertible-to-native-buffer? buffer))
  (->native-buffer [_this]
    (dtype-proto/->native-buffer buffer))

  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [_this]
    (dtype-proto/convertible-to-array-buffer? buffer))
  (->array-buffer [_this]
    (dtype-proto/->array-buffer buffer))

  dtype-proto/PSubBuffer
  (sub-buffer [ary offset len]
    (let [offset (* (long offset) elem-size)
          len (long len)
          byte-len (* len elem-size)]
      (if (and (== 0 offset)
               (== len n-elems))
        ary
        (inplace-new-array-of-structs (:datatype-name struct-def)
                                      (dtype-proto/sub-buffer buffer offset byte-len)
                                      metadata))))

  ObjectBuffer
  (elemwiseDatatype [_ary] (:datatype-name struct-def))
  (lsize [_ary] n-elems)
  (readObject [_ary idx]
    (let [sub-buffer (dtype-proto/sub-buffer
                      buffer
                      (* idx elem-size)
                      elem-size)]
      (inplace-new-struct (:datatype-name struct-def) sub-buffer metadata)))
  (writeObject [_ary idx value]
    (assign-struct! value struct-def (dtype-proto/sub-buffer buffer
                                                             (* idx elem-size)
                                                             elem-size))))


(defn ^:no-doc inplace-new-array-of-structs
  ([datatype buffer options]
   (let [struct-def (get-struct-def datatype)
         elem-size (long (:datatype-size struct-def))
         buf-size (dtype-base/ecount buffer)
         _ (when-not (== 0 (rem buf-size elem-size))
             (throw (Exception. "Buffer size is not an even multiple of dtype size.")))
         n-elems (quot buf-size elem-size)]
     (ArrayOfStructs. struct-def
                      elem-size
                      n-elems
                      buffer
                      options)))
  ([datatype buffer]
   (inplace-new-array-of-structs datatype buffer {})))


(defn ^:no-doc new-array-of-structs
  ([datatype n-elems options]
   (let [struct-def (get-struct-def datatype)
         n-elems (long n-elems)
         elem-size (long (:datatype-size struct-def))
         buf-size (* n-elems elem-size)
         buffer (byte-array buf-size)]
     (inplace-new-array-of-structs
      datatype buffer
      options)))
  ([datatype n-elems]
   (new-array-of-structs datatype n-elems {})))


(defn array-of-structs->column
  [^ArrayOfStructs ary-of-structs propname]
  (let [dtype-def (.struct-def ary-of-structs)
        stride (unchecked-long (dtype-def :datatype-size))
        {:keys [datatype offset]} (get-in dtype-def [:layout-map propname])
        _ (errors/when-not-errorf offset
            "Unable to find property '%s'" propname)
        n-structs (.n-elems ary-of-structs)
        offset (long offset)
        buffer (dtype-base/->binary-buffer (.buffer ary-of-structs))]
    (case (casting/simple-operation-space datatype)
      :boolean (reify
                 BooleanBuffer
                 (lsize [this] n-structs)
                 (readBoolean [this idx]
                   (if (== 0 (.readBinByte buffer (+ offset (* idx stride))))
                     false
                     true))
                 (writeBoolean [this idx val]
                   (.writeBinByte buffer (+ offset (* idx stride))
                                  (unchecked-byte (if val 0 1))))
                 dtype-proto/PEndianness
                 (endianness [_m] (dtype-proto/endianness buffer)))
      ;;Reading data involves unchecked casts.  Writing involves checked-casts
      :int64
      (let [[read-fn write-fn]
            (case datatype
              :int8 [#(.readBinByte buffer (unchecked-long %))
                     #(.writeBinByte buffer (unchecked-long %1) (byte %2))]
              :uint8 [#(-> (.readBinByte buffer (unchecked-long %))
                           (pmath/byte->ubyte))
                      #(.writeBinByte buffer (unchecked-long %1)
                                      (unchecked-byte
                                       (casting/datatype->cast-fn :int64 :uint8
                                                                  (unchecked-long %2))))]
              :int16 [#(.readBinShort buffer (unchecked-long %))
                      #(.writeBinShort buffer (unchecked-long %1) (short %2))]
              :uint16 [#(-> (.readBinShort buffer (unchecked-long %))
                            (pmath/short->ushort))
                       #(.writeBinShort buffer (unchecked-long %1)
                                        (unchecked-short
                                         (casting/datatype->cast-fn :int64 :uint16
                                                                    (unchecked-long %2))))]
              :int32 [#(.readBinInt buffer (unchecked-long %))
                      #(.writeBinInt buffer (unchecked-long %1) (int %2))]
              :uint32 [#(-> (.readBinInt buffer (unchecked-long %))
                            (pmath/int->uint))
                       #(.writeBinInt buffer (unchecked-long %1)
                                      (unchecked-int
                                       (casting/datatype->cast-fn :int64 :uint32
                                                                  (unchecked-long %2))))]
              :int64 [#(.readBinLong buffer (unchecked-long %))
                      #(.writeBinLong buffer (unchecked-long %1) (long %2))]
              :uint64 [#(.readBinLong buffer (unchecked-long %))
                       #(.writeBinLong buffer (unchecked-long %1)
                                       (unchecked-long
                                        (casting/datatype->cast-fn :int64 :uint64
                                                                   (unchecked-long %2))))])]
               (reify LongBuffer
                 (lsize [this] n-structs)
                 (elemwiseDatatype [this] datatype)
                 (readLong [this idx]
                   (unchecked-long (read-fn (+ offset (* idx stride)))))
                 (writeLong [this idx value]
                   (write-fn (+ offset (* idx stride)) value))
                 dtype-proto/PEndianness
                 (endianness [_m] (dtype-proto/endianness buffer))))
      :float64 (let [[read-fn write-fn]
                     (case datatype
                       :float32 [#(.readBinFloat buffer (unchecked-long %))
                                 #(.writeBinFloat buffer (unchecked-long %1) (float %2))]
                       :float64 [#(.readBinDouble buffer (unchecked-long %))
                                 #(.writeBinDouble buffer (unchecked-long %1) (double %2))])]
                 (reify
                   DoubleBuffer
                   (lsize [this] n-structs)
                   (elemwiseDatatype [this] datatype)
                   (readDouble [this idx]
                     (unchecked-double (read-fn (+ offset (* idx stride)))))
                   (writeDouble [this idx value]
                     (write-fn (+ offset (* idx stride)) value))
                   dtype-proto/PEndianness
                   (endianness [_m] (dtype-proto/endianness buffer))))
      ;;sub struct reading is a bit harder
      :object (throw (Exception.
                      "Making a column out of a struct property isn't supported yet.")))))


(defn column-map
  "Given an array of structs, return a map that maps the property to a primitive buffer for
  that column.  This is appropriate for passing directly into ->dataset."
  [^ArrayOfStructs ary]
  (let [dtype-def (.struct-def ary)]
    (->> (dtype-def :data-layout)
         (map (fn [{:keys [name] :as prop-def}]
                [name (array-of-structs->column ary name)]))
         (into (array-map)))))


(comment
  (require '[tech.v3.datatype :as dtype])
  (define-datatype! :vec3 [{:name :x :datatype :float32}
                           {:name :y :datatype :float32}
                           {:name :z :datatype :float32}])

  (define-datatype! :segment [{:name :begin :datatype :vec3}
                              {:name :end :datatype :vec3}])

  (define-datatype! :date-thing [{:name :date :datatype :packed-local-date}
                                 {:name :amount :datatype :uint32}])

  (define-datatype! :vec3-uint8 [{:name :x :datatype :uint8}
                                 {:name :y :datatype :uint8}
                                 {:name :z :datatype :uint8}])

  (def test-vec3 (new-struct :vec3))
  (.put test-vec3 :x 3.0)
  test-vec3
  (def line-segment (new-struct :segment))
  (.put line-segment [:begin :x] 6.0)
  line-segment
  (.put line-segment :end test-vec3)
  (.get line-segment [:end :x])
  (.get line-segment [:end 0])

  )
