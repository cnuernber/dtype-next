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
            [tech.v3.datatype.ffi.size-t :as size-t]
            [clj-commons.primitive-math :as pmath])
  (:import [tech.v3.datatype BinaryBuffer ObjectBuffer BooleanBuffer
            LongBuffer DoubleBuffer]
           [ham_fisted Casts ITypedReduce ChunkedList]
           [java.util.concurrent ConcurrentHashMap]
           [java.util RandomAccess List Map LinkedHashSet Collection
            LinkedHashMap]
           [clojure.lang MapEntry IObj IFn ILookup
            IFn$OOLO IFn$OOLOO IFn$OOLL IFn$OOLD
            IFn$OOLLO IFn$OOLDO]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defonce ^:private struct-datatypes (ConcurrentHashMap.))


(def ^:private ptr-types #{:pointer :size-t :offset-t})

(defn- ptr-type? [dt] (ptr-types dt))

(defn datatype->host-type
  "Structs allow size-t, offset-t, and pointer datatypes so this is an ovveride
  you have to use when working with them."
  [dt]
  (if (ptr-type? dt)
    (size-t/numeric-size-t-type dt)
    (casting/datatype->host-type dt)))

(defn datatype-size
  "Return the size, in bytes, of a datatype."
  ^long [datatype]
  (if-let [struct-dtype (.get ^ConcurrentHashMap struct-datatypes datatype)]
    (long (:datatype-size struct-dtype))
    (-> (datatype->host-type datatype)
        (casting/numeric-byte-width))))

(defn datatype-width
  "Return the width or the of a datatype.  The width dictates what address
  the datatype can start at when embedded in another datatype."
  ^long [datatype]
  (if-let [struct-dtype (.get ^ConcurrentHashMap struct-datatypes datatype)]
    (long (:datatype-width struct-dtype))
    (-> (datatype->host-type datatype)
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


(defrecord LayoutEntry [name datatype ^long offset ^long n-elems struct?])


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
                         [(conj datatype-seq (map->LayoutEntry
                                              (assoc entry
                                                     :offset current-offset
                                                     :n-elems n-elems
                                                     :struct? (struct-datatype? datatype))))
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


(defrecord ^:private Accessor [reader writer])

(declare struct->buffer)
(declare inplace-new-struct)
(declare inplace-new-array-of-structs)


(defn- host-flatten
  [dt]
  (if (ptr-type? dt)
    (size-t/numeric-size-t-type dt)
    (casting/host-flatten dt)))



(defn- create-accessors
  [struct-def]
  (let [accessors (LinkedHashMap.)
        layout (get struct-def :data-layout)]
    (reduce (fn [acc layout-entry]
              (let [dtype (get layout-entry :datatype)
                    offset (long (get layout-entry :offset))
                    ^Accessor scalar-acc
                    (if (struct-datatype? dtype)
                      (let [sdef (get-struct-def dtype)
                            offset (long (get layout-entry :offset))
                            dsize (long (get sdef :datatype-size))]
                        (Accessor. (fn [buffer bin-buffer ^long idx]
                                     (->> (dtype-proto/sub-buffer buffer (+ offset (* dsize idx)) dsize)
                                          (inplace-new-struct dtype)))
                                   (fn [buffer bin-buffer ^long idx val]
                                     (dtype-cmc/copy! (struct->buffer val)
                                                      (dtype-proto/sub-buffer buffer (+ offset (* dsize idx)) dsize))
                                     nil)))
                      (let [host-dtype (host-flatten dtype)
                            unsigned? (casting/unsigned-integer-type? dtype)]
                        (if unsigned?
                          (case host-dtype
                            :int8 (Accessor. (fn ^long [buffer ^BinaryBuffer reader ^long idx]
                                               (unchecked-long (Byte/toUnsignedInt (.readBinByte reader (+ offset idx)))))
                                             (fn [buffer ^BinaryBuffer writer ^long idx ^long val]
                                               (.writeBinByte writer (+ idx offset) (unchecked-byte (Casts/longCast val)))))
                            :int16 (Accessor. (fn ^long [buffer ^BinaryBuffer reader ^long idx]
                                                (unchecked-long (Short/toUnsignedInt (.readBinShort reader (+ offset (* idx 2))))))
                                              (fn [buffer ^BinaryBuffer writer ^long idx ^long val]
                                                (.writeBinShort writer (+ offset (* idx 2)) (unchecked-short (Casts/longCast val)))))
                            :int32 (Accessor. (fn ^long [buffer ^BinaryBuffer reader ^long idx]
                                                (unchecked-long (Integer/toUnsignedLong (.readBinInt reader (+ offset (* idx 4))))))
                                              (fn [buffer ^BinaryBuffer writer ^long idx ^long val]
                                                (.writeBinInt writer (+ offset (* idx 4)) (unchecked-int (Casts/longCast val)))))
                            :int64 (Accessor. (fn ^long [buffer ^BinaryBuffer reader ^long idx]
                                                (.readBinLong reader (+ offset (* idx 8))))
                                              (fn [buffer ^BinaryBuffer writer ^long idx ^long val]
                                                (.writeBinLong writer (+ offset (* idx 8)) (Casts/longCast val)))))
                          (case host-dtype
                            :int8 (Accessor. (fn ^long [buffer ^BinaryBuffer reader ^long idx]
                                               (unchecked-long (.readBinByte reader (+ offset idx))))
                                             (fn [buffer ^BinaryBuffer writer ^long idx ^long ^long val]
                                               (.writeBinByte writer (+ offset idx) (byte (Casts/longCast val)))))
                            :int16 (Accessor. (fn ^long [buffer ^BinaryBuffer reader ^long idx]
                                                (unchecked-long (.readBinShort reader (+ offset (* 2 idx)))))
                                              (fn [buffer ^BinaryBuffer writer ^long idx ^long val]
                                                (.writeBinShort writer (+ offset (* 2 idx)) (short (Casts/longCast val)))))
                            :int32 (Accessor. (fn ^long [buffer ^BinaryBuffer reader ^long idx]
                                                (unchecked-long (.readBinInt reader (+ offset (* 4 idx)))))
                                              (fn [buffer ^BinaryBuffer writer ^long idx ^long val]
                                                (.writeBinInt writer (+ offset (* 4 idx)) (int (Casts/longCast val)))))
                            :int64 (Accessor. (fn ^long [buffer ^BinaryBuffer reader ^long idx]
                                                (.readBinLong reader (+ offset (* 8 idx))))
                                              (fn [buffer ^BinaryBuffer writer ^long idx ^long val]
                                                (.writeBinLong writer (+ offset (* 8 idx)) (Casts/longCast val))))
                            :float32 (Accessor. (fn ^double [buffer ^BinaryBuffer reader ^long idx]
                                                  (double (.readBinFloat reader (+ offset (* 4 idx)))))
                                                (fn [buffer ^BinaryBuffer writer ^long idx ^double val]
                                                  (.writeBinFloat writer (+ offset (* 4 idx)) (float (Casts/doubleCast val)))))
                            :float64 (Accessor. (fn ^double [buffer ^BinaryBuffer reader ^long idx]
                                                  (.readBinDouble reader (+ offset (* 8 idx))))
                                                (fn [buffer ^BinaryBuffer writer ^long idx ^double val]
                                                  (.writeBinDouble writer (+ offset (* 8 idx)) (Casts/doubleCast val))))))))
                    n-elems (long (get layout-entry :n-elems))]
                (.put accessors
                      (get layout-entry :name)
                      (if (== 1 n-elems)
                        (let [read-fn (.reader scalar-acc)
                              write-fn (.writer scalar-acc)]
                          (Accessor. #(read-fn %1 %2 0)
                                     #(write-fn %1 %2 0 %3)))
                        (case (casting/simple-operation-space dtype)
                          :int64
                          (let [read-fn ^IFn$OOLL (.reader scalar-acc)
                                write-fn ^IFn$OOLLO (.writer scalar-acc)
                                _ (assert (and (instance? IFn$OOLL read-fn)
                                               (instance? IFn$OOLLO write-fn))
                                          (str "Datatype " dtype " created invalid acccessr"))]
                            (Accessor. #(reify LongBuffer
                                          (elemwiseDatatype [_] dtype)
                                          (lsize [_] n-elems)
                                          (readLong [_ idx]
                                            (.invokePrim read-fn %1 %2 (ChunkedList/indexCheck 0 n-elems idx)))
                                          (writeLong [_ idx v]
                                            (.invokePrim write-fn %1 %2 (ChunkedList/indexCheck 0 n-elems idx) v)))
                                       #(throw (Exception. "Bulk set of array properties not supported yet - use read to get writable list"))))
                          :float64
                          (let [read-fn ^IFn$OOLD (.reader scalar-acc)
                                write-fn ^IFn$OOLDO (.writer scalar-acc)
                                _ (assert (and (instance? IFn$OOLD read-fn)
                                               (instance? IFn$OOLDO write-fn))
                                          (str "Datatype " dtype " created invalid acccessr"))]
                            (Accessor. #(reify DoubleBuffer
                                          (elemwiseDatatype [_] dtype)
                                          (lsize [_] n-elems)
                                          (readDouble [_ idx]
                                            (.invokePrim read-fn %1 %2 (ChunkedList/indexCheck 0 n-elems idx)))
                                          (writeDouble [_ idx v]
                                            (.invokePrim write-fn %1 %2 (ChunkedList/indexCheck 0 n-elems idx) v)))
                                       #(throw (Exception. "Bulk set of array properties not supported yet - use read to get writable list"))))
                          ;;Then return a new array of structs
                          (if (struct-datatype? dtype)
                            (let [sdef (get-struct-def dtype)
                                  dsize (long (get sdef :datatype-size))]
                              (Accessor. (fn [buffer bin-buffer]
                                           (inplace-new-array-of-structs dtype (dtype-proto/sub-buffer buffer offset (* n-elems dsize))))
                                         #(throw (Exception. "Bulk set of array properties not supported yet - use read to get writable list"))))
                            (let [read-fn ^IFn$OOLO (.reader scalar-acc)
                                  write-fn ^IFn$OOLOO (.writer scalar-acc)
                                  _ (assert (and (instance? IFn$OOLO read-fn)
                                                 (instance? IFn$OOLOO write-fn))
                                            (str "Datatype " dtype " created invalid acccessr"))]
                              (Accessor. #(reify ObjectBuffer
                                            (elemwiseDatatype [_] dtype)
                                            (lsize [_] n-elems)
                                            (readObject [_ idx]
                                              (.invokePrim read-fn %1 %2 (ChunkedList/indexCheck 0 n-elems idx)))
                                            (writeObject [_ idx v]
                                              (.invokePrim write-fn %1 %2 (ChunkedList/indexCheck 0 n-elems idx) v)))
                                         #(throw (Exception. "Bulk set of array properties not supported yet - use read to get writable list"))))))))))
            nil
            layout)
    (assoc struct-def :accessors accessors)))

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
    (.put ^ConcurrentHashMap struct-datatypes datatype-name (create-accessors new-datatype))
    new-datatype))


(declare inplace-new-struct)


(defmacro ^:private ensure-binary-buffer!
  []
  `(do
     (when-not ~'cached-buffer
       (set! ~'cached-buffer (dtype-base/->binary-buffer ~'buffer)))
     ~'cached-buffer))


(declare struct->clj)

(defn- structv->clj
  [v]
  (cond
    (instance? java.util.Map v)
    (struct->clj v)
    (instance? java.util.List v)
    (mapv structv->clj v)
    :else
    v))

(defn struct->clj
  "Copy a struct into native clojure datastructures"
  [s]
  (-> (reduce #(assoc! %1 (key %2) (-> (val %2) structv->clj))
              (transient {})
              s)
      (persistent!)))


(deftype ^{:doc "Struct instance.  Derives most-notably from `java.util.Map` and
 `clojure.lang.ILookup`.  Metadata (meta, with-meta, vary-meta) is supported.
  Imporant member variables are:

 * `.struct-def - The struct definition of this instance.
 * `.buffer - The underlying backing store of this instance."}
    Struct [struct-def
            buffer
            ^{:unsynchronized-mutable true
              :tag BinaryBuffer} cached-buffer
            metadata
            accessors]
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
      (Struct. struct-def buffer cached-buffer m accessors))

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
      (when-let [^Accessor accessor (get accessors k)]
        ((.reader accessor) buffer (ensure-binary-buffer!))))
    (getOrDefault [m k d]
      (or (.get m k) d))
    (put [m k v]
      (let [writer (ensure-binary-buffer!)]
        (when-not (.allowsBinaryWrite writer)
          (throw (Exception. "Item is immutable")))
        (if-let [^Accessor accessor (get accessors k)]
          ((.writer accessor) buffer writer v)
          (throw (Exception. (format "Datatype %s does not contain field %s"
                                     (dtype-proto/datatype m) k))))))
    ITypedReduce
    (reduce [this rfn acc]
      (let [bin-buf (ensure-binary-buffer!)]
        (reduce
         (fn [acc e]
           (rfn acc (MapEntry/create
                     (key e)
                     ((.reader ^Accessor (val e)) buffer bin-buf))))
         acc
         accessors)))
    Object
    (toString [this] (str (struct->clj this))))


(defn struct->buffer
  [^Struct s]
  (.buffer s))



(defn inplace-new-struct
  "Create a new struct in-place in the backing store.  The backing store must
  either be convertible to a native buffer or a byte-array.

  Returns a new Struct datatype."
  (^Struct [datatype backing-store _options]
   (let [struct-def (get-struct-def datatype)]
     (Struct. struct-def backing-store nil {} (get struct-def :accessors))))
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
     (Struct. struct-def backing-data nil options (:accessors struct-def))))
  (^Struct [datatype]
   (new-struct datatype {})))


(defn map->struct!
  [data rv]
  (reduce (fn [acc e]
            (.put ^Map rv (key e) (val e)))
          false
          data)
  rv)


(defn map->struct
  ([dtype data]
   (map->struct dtype data :gc))
  ([dtype data track-type]
   (map->struct! data (new-struct dtype {:container-type :native-heap
                                         :resource-type track-type}))))


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


(defn inplace-new-array-of-structs
  "Create an array of structs from an existing buffer.  Buffer must be an exact multiple
  of the size of the desired struct type."
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
  "Create a new array of structs from new memory.

  Options:

  * `:container-type` - passed directly into make-container, defaults to `:jvm-heap`.
  For native heap there is a further option of `:resource-type` which can be on of the
  tech.v3.resource track types of nil, :stack, :gc, or :auto.  Nil means this memory
  will either live for the lifetime of the process or need to be freed manually.
  For more options see [[tech.v3.datatype.native-buffer/malloc]]."
  ([datatype n-elems options]
   (let [struct-def (get-struct-def datatype)
         n-elems (long n-elems)
         elem-size (long (:datatype-size struct-def))
         buf-size (* n-elems elem-size)
         buffer (dtype-cmc/make-container
                 (get options :container-type :jvm-heap)
                 :int8
                 options
                 buf-size)]
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
    (case (casting/simple-operation-space (datatype->host-type datatype))
      :boolean (reify
                 BooleanBuffer
                 (lsize [this] n-structs)
                 (readObject [this idx]
                   (if (== 0 (.readBinByte buffer (+ offset (* idx stride))))
                     false
                     true))
                 (writeObject [this idx val]
                   (.writeBinByte buffer (+ offset (* idx stride))
                                  (unchecked-byte (if (Casts/booleanCast val) 1 0))))
                 dtype-proto/PEndianness
                 (endianness [_m] (dtype-proto/endianness buffer)))
      ;;Reading data involves unchecked casts.  Writing involves checked-casts
      :int64
      (let [datatype (if (ptr-type? datatype)
                       (size-t/numeric-size-t-type datatype)
                       datatype)
            [read-fn write-fn]
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
  (def test-vec3 (new-struct :vec3-uint8))

    (do
    (require '[criterium.core :as crit])


    (define-datatype! :vec3 [{:name :x :datatype :float32}
                             {:name :y :datatype :float32}
                             {:name :z :datatype :float32}])
    (def test-vec3 (new-struct :vec3))

    (println "accessor")
    (crit/quick-bench (test-vec3 :x))
    ;;47ns initial, after accessor upgrade 14ns

    (println "reduction")
    (crit/quick-bench (reduce (fn [acc v] (+ acc (val v)))
                              0.0
                              test-vec3))
    ;;466ns initial, after accessor upgrade 60ns
    )

    (define-datatype! :ptr-types [{:name :a :datatype :pointer}
                                  {:name :b :datatype :size-t}
                                  {:name :c :datatype :offset-t}
                                  {:name :d :datatype :int64}])
  )
