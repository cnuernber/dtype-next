(ns tech.v3.datatype.struct
  "Structs are datatypes composed of primitive datatypes or other structs.
  Similar to records except they do not support string or object columns,
  only numeric values.  They have memset-0 initialization, memcpy copy,
  and defined equals and hash parameters all based on the actual binary
  representation of the data in the struct."
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype BinaryBuffer Buffer ObjectBuffer]
           [java.util.concurrent ConcurrentHashMap]
           [java.util RandomAccess List Map LinkedHashSet Collection]
           [clojure.lang MapEntry IObj IFn ILookup]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defonce struct-datatypes (ConcurrentHashMap.))


(defn datatype-size
  ^long [datatype]
  (if-let [struct-dtype (.get ^ConcurrentHashMap struct-datatypes datatype)]
    (long (:datatype-size struct-dtype))
    (-> (casting/datatype->host-type datatype)
        (casting/numeric-byte-width))))


(defn datatype-width
  ^long [datatype]
  (if-let [struct-dtype (.get ^ConcurrentHashMap struct-datatypes datatype)]
    (long (:datatype-width struct-dtype))
    (-> (casting/datatype->host-type datatype)
        (casting/numeric-byte-width))))


(defn widen-offset
  "Ensure the offset starts at the appropriate boundary for the datatype width."
  ^long [^long offset ^long datatype-width]
  (let [rem-result (rem offset datatype-width)]
    (if (== 0 rem-result)
      offset
      (+ offset (- datatype-width rem-result)))))


(defn struct-datatype?
  [datatype]
  (.containsKey ^ConcurrentHashMap struct-datatypes datatype))


(defn layout-datatypes
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
  [datatype]
  (if-let [retval (.get ^ConcurrentHashMap struct-datatypes datatype)]
    retval
    (throw (Exception. (format "Datatype %s is not a struct definition." datatype)))))


(defn get-datatype
  [datatype-name]
  (.getOrDefault ^ConcurrentHashMap struct-datatypes
                 datatype-name datatype-name))


(defn offset-of
  "Returns a tuple of [offset dtype]."
  [{:keys [layout-map] :as struct-def} property-vec]
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
          [offset prop-datatype])))))


(defn define-datatype!
  [datatype-name datatype-seq]
  (let [new-datatype (-> (layout-datatypes datatype-seq)
                         (assoc :datatype-name datatype-name))]
    (.put ^ConcurrentHashMap struct-datatypes datatype-name new-datatype)
    new-datatype))


(declare inplace-new-struct)


(defmacro ensure-binary-buffer!
  []
  `(do
     (when-not ~'cached-buffer
       (set! ~'cached-buffer (dtype-base/->binary-buffer ~'buffer)))
     ~'cached-buffer))


(deftype Struct [struct-def
                 buffer
                 ^{:unsynchronized-mutable true
                   :tag BinaryBuffer} cached-buffer
                 metadata]
  dtype-proto/PDatatype
  (datatype [m] (:datatype-name struct-def))
  dtype-proto/PECount
  (ecount [m] (dtype-proto/ecount buffer))
  dtype-proto/PEndianness
  (endianness [m] (dtype-proto/endianness buffer))
  dtype-proto/PClone
  (clone [m]
    (let [new-buffer (dtype-proto/clone buffer)]
      (inplace-new-struct (:datatype-name struct-def) new-buffer
                          {:endianness
                           (dtype-proto/endianness buffer)})))

  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this]
    (dtype-proto/convertible-to-native-buffer? buffer))
  (->native-buffer [this]
    (dtype-proto/->native-buffer buffer))

  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [this]
    (dtype-proto/convertible-to-array-buffer? buffer))
  (->array-buffer [this]
    (dtype-proto/->array-buffer buffer))

  IObj
  (meta [this] metadata)
  (withMeta [this m]
    (Struct. struct-def buffer cached-buffer m))

  ILookup
  (valAt [this k] (.get this k))
  (valAt [this k not-found] (.getOrDefault this k not-found))

  IFn
  (invoke [this k] (.get this k))
  (applyTo [this args]
    (errors/when-not-errorf
     (= 1 (count args))
     "only 1 arg is acceptable; %d provided" (count args)))

  Map
  (size [m] (count (:data-layout struct-def)))
  (containsKey [m k] (.containsKey ^Map (:layout-map struct-def) k))
  (entrySet [m]
    (let [map-entry-data (map (comp #(MapEntry. % (.get m %)) :name)
                              (:data-layout struct-def))]
      (LinkedHashSet. ^Collection map-entry-data)))
  (keySet [m] (.keySet ^Map (:layout-map struct-def)))
  (get [m k]
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
                                   (dtype-proto/datatype m)) k))))))


(defn inplace-new-struct
  (^Struct [datatype backing-store options]
   (let [struct-def (get-struct-def datatype)]
     (Struct. struct-def backing-store nil {})))
  (^Struct [datatype backing-store]
   (inplace-new-struct datatype backing-store {})))


(defn new-struct
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


(deftype ArrayOfStructs [struct-def
                         ^long elem-size
                         ^long n-elems
                         buffer
                         metadata]
  dtype-proto/PEndianness
  (endianness [ary] (dtype-proto/endianness buffer))

  dtype-proto/PClone
  (clone [ary]
    (inplace-new-array-of-structs (:datatype-name struct-def)
                                  (dtype-proto/clone buffer)
                                  metadata))
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this]
    (dtype-proto/convertible-to-native-buffer? buffer))
  (->native-buffer [this]
    (dtype-proto/->native-buffer buffer))

  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [this]
    (dtype-proto/convertible-to-array-buffer? buffer))
  (->array-buffer [this]
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
  (elemwiseDatatype [ary] (:datatype-name struct-def))
  (lsize [ary] n-elems)
  (readObject [ary idx]
    (let [sub-buffer (dtype-proto/sub-buffer
                      buffer
                      (* idx elem-size)
                      elem-size)]
      (inplace-new-struct (:datatype-name struct-def) sub-buffer metadata)))
  (writeObject [ary idx value]
    (assign-struct! value struct-def (dtype-proto/sub-buffer buffer
                                                             (* idx elem-size)
                                                             elem-size))))


(defn inplace-new-array-of-structs
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


(defn new-array-of-structs
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


(comment
  (require '[tech.v3.datatype :as dtype])
  (define-datatype! :vec3 [{:name :x :datatype :float32}
                           {:name :y :datatype :float32}
                           {:name :z :datatype :float32}])

  (define-datatype! :segment [{:name :begin :datatype :vec3}
                              {:name :end :datatype :vec3}])
  (require '[tech.v3.datatype.datetime :as datetime])
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
