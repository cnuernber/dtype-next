(ns tech.v3.datatype.struct
  "Structs are datatypes composed of primitive datatypes or other structs.
  Similar to records except they do not support string or object columns,
  only numeric values.  They have memset-0 initialization, memcpy copy,
  and defined equals and hash parameters all based on the actual binary
  representation of the data in the struct."
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.monotonic-range :as dtype-range]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype BinaryReader BinaryWriter ObjectBuffer]
           [java.util.concurrent ConcurrentHashMap]
           [java.util RandomAccess List Map LinkedHashSet Collection]
           [clojure.lang MapEntry]))

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


(deftype Struct [struct-def
                 buffer
                 ^BinaryReader reader
                 ^BinaryWriter writer]
  dtype-proto/PDatatype
  (get-datatype [m] (:datatype-name struct-def))
  dtype-proto/PEndianness
  (endianness [m] (dtype-proto/endianness reader))
  dtype-proto/PClone
  (clone [m]
    (let [new-buffer (dtype-proto/clone buffer)]
      (inplace-new-struct (:datatype-name struct-def) new-buffer
                          {:endianness
                           (dtype-proto/endianness reader)})))
  dtype-proto/PBufferType
  (buffer-type [item] :struct)
  Map
  (size [m] (count (:data-layout struct-def)))
  (containsKey [m k] (.containsKey ^Map (:layout-map struct-def) k))
  (entrySet [m]
    (let [map-entry-data
          (->> (map (comp #(MapEntry. % (.get m %))
                          :name)
                    (:data-layout struct-def)))]
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
                              {:endianness (dtype-proto/endianness reader)}))
        (let [host-dtype (casting/host-flatten dtype)
              value
              (case host-dtype
                :int8 (.readByte reader offset)
                :int16 (.readShort reader offset)
                :int32 (.readInt reader offset)
                :int64 (.readLong reader offset)
                :float32 (.readFloat reader offset)
                :float64 (.readDouble reader offset))]
          (if (= host-dtype dtype)
            value
            (casting/unchecked-cast value dtype))))))
  (getOrDefault [m k d]
    (or (.get m k) d))
  (put [m k v]
    (when-not writer
      (throw (Exception. "Item is immutable")))
    (if-let [[offset dtype :as _data-vec] (offset-of struct-def k)]
      (if-let [struct-def (.get ^ConcurrentHashMap struct-datatypes dtype)]
        (let [_ (when-not (and (instance? Struct v)
                               (= dtype (dtype-proto/get-datatype v)))

                  (throw (Exception. (format "non-struct or datatype mismatch: %s %s"
                                             dtype (dtype-proto/get-datatype v)))))]
          (dtype-base/copy! (.buffer ^Struct v)
                            (dtype-proto/sub-buffer buffer offset
                                                    (:datatype-size struct-def)))
          nil)
        (let [v (casting/cast v dtype)
              host-dtype (casting/host-flatten dtype)]
          (case host-dtype
            :int8 (.writeByte writer (pmath/byte v) offset)
            :int16 (.writeShort writer (pmath/short v) offset)
            :int32 (.writeInt writer (pmath/int v) offset)
            :int64 (.writeLong writer (pmath/long v) offset)
            :float32 (.writeFloat writer (pmath/float v) offset)
            :float64 (.writeDouble writer (pmath/double v) offset)
            )))
      (throw (Exception. (format "Datatype %s does not containt field %s"
                                 (dtype-proto/get-datatype m)) k)))))


(defmethod dtype-proto/copy! [:struct :struct]
  [dst src _options]
  (let [^Struct src src
        ^Struct dst dst]
    (when-not (= (dtype-proto/get-datatype src)
                 (dtype-proto/get-datatype dst))
      (throw (Exception. (format "src datatype %s and dst datatype %s do not match"
                                 (dtype-proto/get-datatype src)
                                 (dtype-proto/get-datatype dst)))))
    (dtype-base/copy! (.buffer src) (.buffer dst))
    dst))


(defn inplace-new-struct
  ([datatype backing-store options]
   (let [struct-def (get-struct-def datatype)]
     (Struct. struct-def
              backing-store
              (->binary-reader backing-store options)
              (->binary-writer backing-store options))))
  ([datatype backing-store]
   (inplace-new-struct datatype backing-store {})))


(defn new-struct
  ([datatype options]
   (let [struct-def (get-struct-def datatype)
         ;;binary read/write to nio buffers is faster than our writer-wrapper
         backing-data (dtype-proto/->buffer-backing-store
                       (byte-array (long (:datatype-size struct-def))))]
     (Struct. struct-def
              backing-data
              (->binary-reader backing-data options)
              (->binary-writer backing-data options))))
  ([datatype]
   (new-struct datatype {})))


(defn struct->buffer
  [^Struct struct]
  (.buffer struct))


(defn struct->binary-reader
  ^BinaryReader [^Struct struct]
  (.reader struct))


(defn struct->binary-writer
  ^BinaryWriter [^Struct struct]
  (.writer struct))


(declare inplace-new-array-of-structs)


(defn- assign-struct!
  [value struct-def dst-buf]
  (let [value-type (dtype-proto/get-datatype value)
        array-type (:datatype-name struct-def)]
    (when-not (= value-type (:datatype-name struct-def))
      (throw (Exception. (format "Array %s/value %s mismatch"
                                 array-type value-type))))
    (when-not (instance? Struct value)
      (throw (Exception. (format "Value does not appear to be a struct: %s" (type value)))))
    (let [^Struct value value]
      (dtype-base/copy! (.buffer value) dst-buf))))


(deftype ArrayOfStructs [struct-def
                         ^long elem-size
                         ^long n-elems
                         buffer
                         options]
  dtype-proto/PEndianness
  (endianness [ary] (dtype-proto/default-endianness
                     (:endianness options)))
  dtype-proto/PClone
  (clone [ary]
    (inplace-new-array-of-structs (:datatype-name struct-def)
                                  (dtype-proto/clone buffer)
                                  options))
  dtype-proto/PBufferType
  (buffer-type [item] :array-of-structs)
  dtype-proto/PBuffer
  (sub-buffer [ary offset len]
    (let [offset (* (long offset) elem-size)
          len (long len)
          byte-len (* len elem-size)]
      (if (and (== 0 offset)
               (== len n-elems))
        ary
        (inplace-new-array-of-structs (:datatype-name struct-def)
                                      (dtype-proto/sub-buffer buffer offset byte-len)
                                      options))))
  ObjectReader
  (getDatatype [ary] (:datatype-name struct-def))
  (lsize [ary] n-elems)
  (read [ary idx]
    (let [sub-buffer (dtype-proto/sub-buffer
                      buffer
                      (* idx elem-size)
                      elem-size)]
      (inplace-new-struct (:datatype-name struct-def) sub-buffer options)))
  ObjectWriter
  (write [ary idx value]
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
      datatype (dtype-proto/->buffer-backing-store buffer)
      options)))
  ([datatype n-elems]
   (new-array-of-structs datatype n-elems {})))


(defmethod dtype-proto/copy! [:array-of-structs :array-of-structs]
  [dst src _options]
  (let [^ArrayOfStructs src src
        ^ArrayOfStructs dst dst]
    (when-not (= (dtype-proto/get-datatype src)
                 (dtype-proto/get-datatype dst))
      (throw (Exception. (format "src datatype %s and dst datatype %s do not match"
                                 (dtype-proto/get-datatype src)
                                 (dtype-proto/get-datatype dst)))))
    (dtype-base/copy! (.buffer src) (.buffer dst))
    dst))


(defn- as-binary-reader
  ^BinaryReader [item] item)


(defmacro binary-read
  [datatype reader offset]
  (case datatype
    :boolean `(.readBoolean ~reader ~offset)
    :int8 `(.readByte ~reader ~offset)
    :int16 `(.readShort ~reader ~offset)
    :int32 `(.readInt ~reader ~offset)
    :int64 `(.readLong ~reader ~offset)
    :float32 `(.readFloat ~reader ~offset)
    :float64 `(.readDouble ~reader ~offset)))


(defmacro make-primitive-column-reader
  [datatype binary-reader n-elems stride]
  (let [host-dtype (casting/datatype->host-type datatype)]
    `(let [~'reader (as-binary-reader ~binary-reader)
           n-elems# (long ~n-elems)
           stride# (long ~stride)]
       (reify ~(typecast/datatype->reader-type (casting/safe-flatten datatype))
         (getDatatype [rdr#] ~datatype)
         (lsize [rdr#] n-elems#)
         (read [rdr# idx#]
           (casting/datatype->unchecked-cast-fn
            ~host-dtype
            ~datatype
            (binary-read ~host-dtype ~binary-reader (* idx# stride#))))))))


(defn- as-binary-writer
  ^BinaryWriter [item] item)


(defmacro binary-write
  [datatype writer value offset]
  (case datatype
    :boolean `(.writeBoolean ~writer ~value ~offset)
    :int8 `(.writeByte ~writer ~value ~offset)
    :int16 `(.writeShort ~writer ~value ~offset)
    :int32 `(.writeInt ~writer ~value ~offset)
    :int64 `(.writeLong ~writer ~value ~offset)
    :float32 `(.writeFloat ~writer ~value ~offset)
    :float64 `(.writeDouble ~writer ~value ~offset)))


(defmacro make-primitive-column-writer
  [datatype binary-writer n-elems stride]
  (let [host-dtype (casting/datatype->host-type datatype)]
    `(let [~'writer (as-binary-writer ~binary-writer)
           n-elems# (long ~n-elems)
           stride# (long ~stride)]
       (reify ~(typecast/datatype->writer-type (casting/safe-flatten datatype))
         (getDatatype [rdr#] ~datatype)
         (lsize [rdr#] n-elems#)
         (write [rdr# idx# val#]
           (binary-write ~host-dtype ~binary-writer
                         (casting/datatype->unchecked-cast-fn
                          ~datatype
                          ~host-dtype
                          val#)
                         (* idx# stride#)))))))


(deftype StructColumnBuffer [datatype
                             ^long n-elems
                             ^long stride
                             ^long elem-size
                             buffer
                             options]
  dtype-proto/PEndianness
  (endianness [ary] (dtype-proto/default-endianness
                     (:endianness options)))
  dtype-proto/PDatatype
  (get-datatype [item] datatype)
  dtype-proto/PCountable
  (ecount [item] n-elems)
  dtype-proto/PClone
  (clone [item]
    (if (struct-datatype? datatype)
      (let [new-structs (new-array-of-structs datatype n-elems options)]
        (dtype-base/copy! item new-structs))
      (let [new-buffer (dtype-base/make-container :typed-buffer :datatype n-elems)]
        (dtype-base/copy! item new-buffer))))
  dtype-proto/PBuffer
  (sub-buffer [item offset len]
    (let [offset (long offset)
          len (long len)]
      (if (and (== 0 offset)
               (== n-elems len))
        item
        (StructColumnBuffer. datatype len stride elem-size
                             (dtype-proto/sub-buffer buffer
                                                     (* offset stride)
                                                     (+ elem-size
                                                        (* stride (dec len))))
                             options))))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (->
     (if (struct-datatype? datatype)
       (reify ObjectReader
         (getDatatype [rdr] datatype)
         (lsize [rdr] n-elems)
         (read [rdr idx]
           (inplace-new-struct  datatype
                                (dtype-proto/sub-buffer
                                 buffer (* idx stride) elem-size)
                                options)))
       (let [^BinaryReader binary-reader (dtype-proto/->binary-reader buffer options)]
         (case (casting/datatype->host-type datatype)
           :boolean (make-primitive-column-reader :boolean binary-reader n-elems stride)
           :int8 (make-primitive-column-reader :int8 binary-reader n-elems stride)
           :uint8 (make-primitive-column-reader :uint8 binary-reader n-elems stride)
           :int16 (make-primitive-column-reader :int16 binary-reader n-elems stride)
           :uint16 (make-primitive-column-reader :uint16 binary-reader n-elems stride)
           :int32 (make-primitive-column-reader :int32 binary-reader n-elems stride)
           :uint32 (make-primitive-column-reader :uint32 binary-reader n-elems stride)
           :int64 (make-primitive-column-reader :int64 binary-reader n-elems stride)
           :uint64 (make-primitive-column-reader :uint64 binary-reader n-elems stride)
           :float32 (make-primitive-column-reader :float32 binary-reader n-elems stride)
           :float64 (make-primitive-column-reader :float64 binary-reader n-elems stride))))
     (dtype-proto/->reader options)))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item options]
    (-> (if (struct-datatype? datatype)
          (let [struct-def (get-struct-def datatype)]
            (reify ObjectWriter
              (getDatatype [rdr] datatype)
              (lsize [rdr] n-elems)
              (write [rdr idx value]
                (assign-struct! value struct-def
                                (dtype-proto/sub-buffer
                                 buffer (* idx stride) elem-size)))))
          (let [^BinaryWriter binary-writer (dtype-proto/->binary-writer buffer options)]
            (case (casting/datatype->host-type datatype)
              :boolean (make-primitive-column-writer :boolean binary-writer n-elems stride)
              :int8 (make-primitive-column-writer :int8 binary-writer n-elems stride)
              :uint8 (make-primitive-column-writer :uint8 binary-writer n-elems stride)
              :int16 (make-primitive-column-writer :int16 binary-writer n-elems stride)
              :uint16 (make-primitive-column-writer :uint16 binary-writer n-elems stride)
              :int32 (make-primitive-column-writer :int32 binary-writer n-elems stride)
              :uint32 (make-primitive-column-writer :uint32 binary-writer n-elems stride)
              :int64 (make-primitive-column-writer :int64 binary-writer n-elems stride)
              :uint64 (make-primitive-column-writer :uint64 binary-writer n-elems stride)
              :float32 (make-primitive-column-writer :float32 binary-writer n-elems stride)
              :float64 (make-primitive-column-writer :float64 binary-writer n-elems stride))))
        (dtype-proto/->writer options))))


(defn- datatype-elem-size
  ^long [datatype]
  (long (if (struct-datatype? datatype)
          (:datatype-size (get-struct-def datatype))
          (casting/numeric-byte-width datatype))))


(defn explode-datatype
  [datatype buffer original-offset name-stem n-structs elem-size struct-opts]
  (let [struct-def (get-struct-def datatype)]
    (->> (:data-layout struct-def)
         (mapcat
          (fn [{:keys [name datatype offset n-elems]}]
            (let [new-name (keyword (format "%s-%s"
                                            (clojure.core/name name-stem)
                                            (clojure.core/name name)))
                  offset (+ (long offset) (long original-offset))]
              (if (struct-datatype? datatype)
                (explode-datatype datatype buffer offset new-name
                                  n-structs elem-size struct-opts)
                [{:name new-name
                  :data (StructColumnBuffer. datatype n-structs elem-size
                                             (datatype-elem-size datatype)
                                             (dtype-base/sub-buffer buffer offset)
                                             struct-opts)}])))))))


(defn array-of-structs->columns
  "Given an array of structs create an sequence of 'columns' {:name :data}
  where the names patch the property names and the column values are a reader
  of field variables."
  ([^ArrayOfStructs structs {:keys [scalar-columns?]
                             :or {scalar-columns? false}}]
   (let [struct-def (.struct-def structs)
         n-structs (.n-elems structs)
         elem-size (long (:datatype-size struct-def))
         data-buffer (.buffer structs)
         struct-opts (.options structs)]
     (->> (:data-layout struct-def)
          (mapcat
           (fn [{:keys [name datatype offset n-elems]}]
             (let [offset (long offset)
                   n-elems (long n-elems)]
               (if (and (== (long n-elems) 1))
                 (if (or (not scalar-columns?)
                         (not (struct-datatype? datatype)))
                   [{:name name
                     :data (StructColumnBuffer. datatype n-structs elem-size
                                                (datatype-elem-size datatype)
                                                (dtype-base/sub-buffer data-buffer
                                                                       offset)
                                                struct-opts)}]
                   (explode-datatype datatype data-buffer offset name
                                     n-structs elem-size struct-opts))
                 (let [e-size (datatype-elem-size datatype)]
                   (->> (range n-elems)
                        (mapcat (fn [elem-idx]
                                  (let [name (keyword (format "%s-%d"
                                                             (clojure.core/name name)
                                                             elem-idx))]
                                    (if (or (not scalar-columns?)
                                            (not (struct-datatype? datatype)))
                                      [{:name name
                                        :data (StructColumnBuffer.
                                               datatype n-structs elem-size
                                               e-size
                                               (dtype-base/sub-buffer
                                                data-buffer
                                                (+ offset (* e-size (long elem-idx))))
                                               struct-opts)}]
                                      (explode-datatype datatype data-buffer offset name
                                                        n-structs elem-size struct-opts))))))))))))))
  ([^ArrayOfStructs structs]
   (array-of-structs->columns structs {})))


(comment
  (require '[tech.v2.datatype :as dtype])
  (define-datatype! :vec3 [{:name :x :datatype :float32}
                           {:name :y :datatype :float32}
                           {:name :z :datatype :float32}])

  (define-datatype! :segment [{:name :begin :datatype :vec3}
                              {:name :end :datatype :vec3}])
  (require '[tech.v2.datatype.datetime :as datetime])
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
