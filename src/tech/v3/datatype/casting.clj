(ns tech.v3.datatype.casting
  (:refer-clojure :exclude [cast])
  (:require [clojure.set :as c-set]
            [ham-fisted.lazy-noncaching :as lznc]
            [clj-commons.primitive-math :as pmath]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :as errors])
  (:import [java.util Map Set HashSet Collection]
           [java.util.concurrent ConcurrentHashMap]
           [clojure.lang RT Keyword]
           [tech.v3.datatype HasheqWrap]
           [ham_fisted Casts]
           [java.math BigDecimal]
           [java.util UUID]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defonce ^{:tag Set} valid-datatype-set (ConcurrentHashMap/newKeySet))
(defn- add-valid-datatype [dtype] (.add valid-datatype-set dtype))

(defonce aliased-datatypes (ConcurrentHashMap.))


(defn alias-datatype!
  "Alias a new datatype to a base datatype.  Only useful for primitive datatypes"
  [new-dtype old-dtype]
  (.put ^Map aliased-datatypes new-dtype old-dtype)
  (add-valid-datatype new-dtype))


(defn un-alias-datatype
  [dtype]
  (.getOrDefault ^Map aliased-datatypes dtype dtype))


;;Only object datatypes are represented in these maps
(defonce ^ConcurrentHashMap datatype->class-map (ConcurrentHashMap.))
(defonce ^ConcurrentHashMap class->datatype-map (ConcurrentHashMap.))



(defn add-object-datatype!
  ;;Add an object datatype.
  ([datatype klass implement-protocols?]
   (.put datatype->class-map datatype klass)
   (.put class->datatype-map klass datatype)
   (add-valid-datatype datatype)
   (when implement-protocols?
     (clojure.core/extend klass
       dtype-proto/PElemwiseDatatype
       {:elemwise-datatype (constantly datatype)}
       dtype-proto/PDatatype
       {:datatype (constantly datatype)}))
   :ok)
  ([datatype klass]
   (add-object-datatype! datatype klass true)))


(defn object-class->datatype
  [cls]
  (get class->datatype-map cls :object))


(defn datatype->object-class
  [dtype]
  (if (instance? Class dtype)
    dtype
    (get datatype->class-map dtype Object)))


(def signed-unsigned
  {:int8 :uint8
   :int16 :uint16
   :int32 :uint32
   :int64 :uint64})

(defmacro cdef
  "Eval the code at compile time and just output constant - saves initialization time."
  [name code]
  (let [v (eval code)]
    `(def ~name ~v)))

(cdef unsigned-signed (c-set/map-invert signed-unsigned))

(cdef float-types #{:float32 :float64})

(cdef int-types (set (concat (flatten (seq signed-unsigned)))))

(cdef signed-int-types (set (keys signed-unsigned)))

(cdef unsigned-int-types (set (vals signed-unsigned)))

(cdef host-numeric-types (set (concat signed-int-types float-types)))

(cdef numeric-types (set (concat host-numeric-types unsigned-int-types)))


(defn base-host-datatype?
  [dtype]
  (case dtype
    :int32 true
    :int16 true
    :float32 true
    :float64 true
    :int64 true
    :int8 true
    :boolean true
    :object true
    false))


(defn valid-datatype?
  [datatype]
  (case datatype
    :int64 true
    :float64 true
    :int32 true
    :int8 true
    :int16 true
    :uint8 true
    :uint16 true
    :uint32 true
    :uint64 true
    :char true
    :float32 true
    :boolean true
    :object true
    :string true
    :keyword true
    :uuid true
    (.contains valid-datatype-set datatype)))


(defn ensure-valid-datatype
  [datatype]
  (when-not (valid-datatype? datatype)
    (throw (Exception. (format "Invalid datatype: %s" datatype)))))


(cdef primitive-types (set (concat numeric-types [:boolean])))


(defn- set->hash-set
  ^HashSet [data]
  (let [retval (HashSet. )]
    (.addAll retval ^Collection data)
    retval))


(cdef base-host-datatypes (set (concat host-numeric-types
                                      [:object :boolean])))


(cdef base-datatypes (set (concat host-numeric-types
                                 unsigned-int-types
                                 [:boolean :char :object])))

(cdef ^{:tag HashSet} base-host-datatypes-hashset
      (set->hash-set base-host-datatypes))


(defn int-width
  ^long [dtype]
  (long (get {:boolean 8 ;;unfortunately, this is what C decided
              :int8 8
              :uint8 8
              :int16 16
              :char 16
              :uint16 16
              :int32 32
              :uint32 32
              :int64 64
              :uint64 64}
             dtype
             0)))


(defn float-width
  ^long [dtype]
  (long (get {:float32 32
              :float64 64}
             dtype
             0)))


(defn numeric-byte-width
  ^long [dtype]
  (let [dtype (un-alias-datatype dtype)]
    (case dtype
      (:int8 :uint8 :boolean) 1
      (:int16 :uint16 :char) 2
      (:int32 :uint32 :float32) 4
      (:int64 :uint64 :float64) 8
      (throw (ex-info (format "datatype is not numeric: %s" dtype)
                      {:datatype dtype})))))


(defn numeric-type?
  [dtype]
  (let [dtype (un-alias-datatype dtype)]
    (boolean
     (or (int-types dtype)
         (float-types dtype)))))

(defn- unaliased-float-type?
  [dtype]
  (if (or (identical? dtype :float64)
          (identical? dtype :float32))
    true
    false))

(defn float-type?
  [dtype]
  (unaliased-float-type? (un-alias-datatype dtype)))


(defn- unaliased-integer-type?
  [dtype]
  (case dtype
    :int64 true
    :int32 true
    :uint8 true
    :int16 true
    :uint64 true
    :uint16 true
    :int8 true
    :uint32 true
    false))


(defn integer-type?
  [dtype]
  (unaliased-integer-type? (un-alias-datatype dtype)))


(defn signed-integer-type?
  [dtype]
  (let [dtype (un-alias-datatype dtype)]
    (boolean (contains? signed-unsigned dtype))))


(defn unsigned-integer-type?
  [dtype]
  (let [dtype (un-alias-datatype dtype)]
    (boolean (contains? unsigned-signed dtype))))


(defn integer-datatype->float-datatype
  [dtype]
  (if (#{:int8 :uint8 :int16 :uint16} dtype)
    :float32
    :float64))


(defn is-host-numeric-datatype?
  [dtype]
  (let [dtype (un-alias-datatype dtype)]
    (boolean (host-numeric-types dtype))))


(defn is-host-datatype?
  [dtype]
  (let [dtype (un-alias-datatype dtype)]
    (boolean (base-datatypes dtype))))


(defn datatype->host-datatype
  [dtype]
  (let [dtype (un-alias-datatype dtype)]
    (get unsigned-signed dtype dtype)))


(defmacro bool->number
  [item]
  `(if ~item 1 0))


(defn ->number
  [item]
  (cond
    (number? item) item
    (boolean? item) (bool->number item)
    (char? item) (unchecked-int item)
    :else ;;punt!!
    (Casts/doubleCast item)))


(defn as-long
  ^long [item] (long item))


(defmacro datatype->number
  [src-dtype item]
  (cond
    (not (numeric-type? src-dtype))
    `(->number ~item)
    ;;characters are not numbers...
    (= :char src-dtype)
    `(as-long ~item)
    :else
    `~item))


(defmacro datatype->boolean
  [src-dtype item]
  `(Casts/booleanCast ~item))

;; Save these because we are switching to unchecked soon

;;Numeric casts
(defmacro datatype->unchecked-cast-fn
  [src-dtype dtype val]
  (if (= src-dtype dtype)
    val
    (case dtype
      :int8 `(pmath/byte (datatype->number ~src-dtype ~val))
      :int16 `(pmath/short (datatype->number ~src-dtype ~val))
      :char `(RT/uncheckedCharCast (datatype->number ~src-dtype ~val))
      :int32 `(pmath/int (datatype->number ~src-dtype ~val))
      :int64 `(pmath/long (datatype->number ~src-dtype ~val))

      :uint8 `(pmath/byte->ubyte (datatype->number ~src-dtype ~val))
      :uint16 `(pmath/short->ushort (datatype->number ~src-dtype ~val))
      :uint32 `(pmath/int->uint (datatype->number ~src-dtype ~val))
      :uint64 `(pmath/long (datatype->number ~src-dtype ~val))

      :float32 `(pmath/float (datatype->number ~src-dtype ~val))
      :float64 `(pmath/double (datatype->number ~src-dtype ~val))
      :boolean `(datatype->boolean ~src-dtype ~val)
      :object `~val)))


(defmacro check
  [compile-time-max compile-time-min runtime-val datatype]
  `(if (or (pmath/> ~runtime-val
                ~compile-time-max)
           (pmath/< ~runtime-val
                    ~compile-time-min))
     (throw (ex-info (format "Value out of range for %s: %s"
                             (name ~datatype) ~runtime-val)
                     {:min ~compile-time-min
                      :max ~compile-time-max
                      :value ~runtime-val}))
     ~runtime-val))


(defmacro datatype->cast-fn
  [src-dtype dst-dtype val]
  (if (= src-dtype dst-dtype)
    val
    (case dst-dtype
      :uint8 `(datatype->unchecked-cast-fn ~src-dtype ~dst-dtype
                                           (check (short 0xff) (short 0)
                                                  (short (datatype->number ~src-dtype
                                                                           ~val))
                                                  ~dst-dtype))
      :uint16 `(datatype->unchecked-cast-fn ~src-dtype ~dst-dtype
                                            (check (int 0xffff) (int 0)
                                                   (int (datatype->number ~src-dtype
                                                                          ~val))
                                                   ~dst-dtype))
      :uint32 `(datatype->unchecked-cast-fn ~src-dtype ~dst-dtype
                                            (check (long 0xffffffff) (int 0)
                                                   (long (datatype->number ~src-dtype
                                                                           ~val))
                                                   ~dst-dtype))
      :uint64 `(datatype->unchecked-cast-fn ~src-dtype ~dst-dtype
                                            (check (long Long/MAX_VALUE) (long 0)
                                                   (long (datatype->number ~src-dtype
                                                                           ~val))
                                                   ~dst-dtype))
      :int8 `(RT/byteCast (datatype->number ~src-dtype ~val))
      :int16 `(RT/shortCast (datatype->number ~src-dtype ~val))
      :char `(RT/charCast (datatype->number ~src-dtype ~val))
      :int32 `(RT/intCast (datatype->number ~src-dtype ~val))
      :int64 `(RT/longCast (datatype->number ~src-dtype ~val))
      :float32 `(RT/floatCast (datatype->number ~src-dtype ~val))
      :float64 `(RT/doubleCast (datatype->number ~src-dtype ~val))
      :boolean `(datatype->boolean ~src-dtype ~val)
      :keyword `(keyword ~val)
      :symbol `(symbol ~val)
      :object `~val)))


(defmacro ^:private checked-cast-table
  []
  `(into {} ~(mapv (fn [dtype]
                     [dtype `#(datatype->cast-fn :unknown ~dtype %)])
                   base-datatypes)))

(defmacro ^:private unchecked-cast-table
  []
  `(into {} ~(mapv (fn [dtype]
                     [dtype `#(datatype->unchecked-cast-fn :unknown ~dtype %)])
                   base-datatypes)))

(defonce ^:dynamic *cast-table* (atom (checked-cast-table)))
(defonce ^:dynamic *unchecked-cast-table* (atom (unchecked-cast-table)))


(defn add-cast-fn!
  [datatype cast-fn]
  (swap! *cast-table* assoc datatype cast-fn))


(defn add-unchecked-cast-fn!
  [datatype cast-fn]
  (swap! *unchecked-cast-table* assoc datatype cast-fn))



(defn all-datatypes
  []
  (keys @*cast-table*))

(cdef all-host-datatypes
      (set (concat host-numeric-types
                   [:boolean :object])))


(alias-datatype! :double :float64)
(alias-datatype! :float :float32)
(alias-datatype! :long :int64)
(alias-datatype! :int :int32)
(alias-datatype! :short :int16)
(alias-datatype! :byte :int8)
(alias-datatype! :bool :boolean)


(defn datatype->host-type
  "Get the signed analog of an unsigned type or return datatype unchanged."
  [datatype]
  (let [datatype (un-alias-datatype datatype)]
    (get unsigned-signed datatype datatype)))


(defn datatype->safe-host-type
  "Get a jvm datatype wide enough to store all values of this datatype"
  [dtype]
  (let [base-dtype (un-alias-datatype dtype)]
    (case base-dtype
        :uint8 :int16
        :uint16 :int32
        :uint32 :int64
        :uint64 :int64
        base-dtype)))


(defmacro datatype->host-cast-fn
  [src-dtype dst-dtype val]
  (let [host-type (datatype->host-type dst-dtype)]
    `(datatype->unchecked-cast-fn
      :ignored ~host-type
      (datatype->cast-fn ~src-dtype ~dst-dtype ~val))))


(defmacro datatype->unchecked-host-cast-fn
  [src-dtype dst-dtype val]
  (let [host-type (datatype->host-type dst-dtype)]
    `(datatype->unchecked-cast-fn
      :ignored ~host-type
      (datatype->unchecked-cast-fn ~src-dtype ~dst-dtype ~val))))



(defn flatten-datatype
  "Move a datatype into the canonical set"
  [dtype]
  (let [dtype (un-alias-datatype dtype)]
    (if (base-datatypes dtype)
      dtype
      :object)))

(defn- case-flatten
  [dtype]
  (case dtype
    :int32 dtype
    :int16 dtype
    :float32 dtype
    :float64 dtype
    :int64 dtype
    :int8 dtype
    :boolean dtype
    :char dtype
    :object))


(defn safe-flatten
  [dtype]
  (if (base-host-datatype? dtype)
    dtype
    (-> dtype
        datatype->safe-host-type
        case-flatten)))


(defn host-flatten
  [dtype]
  (if (base-host-datatype? dtype)
    dtype
    (-> dtype
        un-alias-datatype
        datatype->host-datatype
        case-flatten)))


(defn- perform-cast
  [cast-table value datatype]
  (let [datatype (un-alias-datatype datatype)]
    (if-let [cast-fn (get cast-table datatype)]
      (cast-fn value)
      (if (= (flatten-datatype datatype) :object)
        value
        (throw (ex-info "No cast available" {:datatype datatype}))))))


(defn cast
  "Perform a checked cast of a value to specific datatype."
  [value datatype]
  (perform-cast @*cast-table* value datatype))


(defn cast-fn
  [datatype cast-table]
  (let [datatype (un-alias-datatype datatype)]
    (if-let [cast-fn (get cast-table datatype)]
      cast-fn
      (if (identical? (flatten-datatype datatype) :object)
        identity
        (throw (ex-info "No cast available" {:datatype datatype}))))))


(defn unchecked-cast
  "Perform an unchecked cast of a value to specific datatype."
  [value datatype]
  (perform-cast @*unchecked-cast-table* value datatype))


(defmacro datatype->sparse-value
  [datatype]
  (cond
    (= datatype :object)
    `nil
    (= datatype :boolean)
    `false
    :else
    `(datatype->unchecked-cast-fn :unknown ~datatype 0)))


;;Everything goes to these and these go to everything.
(def base-marshal-types
  #{:int8 :int16 :int32 :int64 :float32 :float64 :boolean :object})


(defmacro make-base-datatype-table
  [inner-macro]
  `(->> [~@(for [dtype base-marshal-types]
             [dtype `(~inner-macro ~dtype)])]
        (into {})))


(defmacro make-base-no-boolean-datatype-table
  [inner-macro]
  `(->> [~@(for [dtype (->> base-marshal-types
                            (remove #(= :boolean %)))]
             [dtype `(~inner-macro ~dtype)])]
        (into {})))


(def marshal-source-table
  {:int8 base-marshal-types
   :int16 base-marshal-types
   :int32 base-marshal-types
   :int64 base-marshal-types
   :float32 base-marshal-types
   :float64 base-marshal-types
   :boolean base-marshal-types
   :object base-marshal-types})



(defn- set-conj
  [container item]
  (if container
    (conj container item)
    #{item}))


(cdef type-tree
      (reduce (fn [m [child-type parent-type]]
                (update m child-type set-conj parent-type))
              {}
              [[:boolean :int8]
               [:int8 :int16]
               [:uint8 :int16]
               [:int16 :int32]
               [:uint16 :int32]
               [:int32 :int64]
               [:uint32 :int64]
               [:int64 :float64]
               [:uint64 :float64]
               [:float32 :float64]
               [:int16 :float32]
               [:uint16 :float32]]))


(def type-path->root
  (memoize
   (fn
     ([datatype type-vec type-tree]
      (let [un-aliased (un-alias-datatype datatype)
            type-vec (conj type-vec datatype)]
        (if (= un-aliased datatype)
          (reduce #(type-path->root %2 %1 type-tree)
                  type-vec
                  (get type-tree datatype))
          (type-path->root un-aliased type-vec type-tree))))
     ([datatype]
      (->>
       (conj
        (type-path->root datatype [] type-tree)
        :object)
       (reverse)
       (distinct)
       (reverse))))))


(def type-path->root-set
  (memoize
   (fn [datatype]
     (set (type-path->root
           datatype)))))


(defn widest-datatype
  ([lhs-dtype rhs-dtype]
   (if (= lhs-dtype rhs-dtype)
     lhs-dtype
     (first (filter (type-path->root-set lhs-dtype)
                    (type-path->root rhs-dtype)))))
  ([lhs-dtype] lhs-dtype))


(defn simple-operation-space
  "Flatten datatypes down into long, double, or object."
  ([lhs-dtype rhs-dtype]
   (if (or (nil? lhs-dtype)
           (nil? rhs-dtype))
     :object
     (let [lhs-dtype (un-alias-datatype lhs-dtype)
           rhs-dtype (un-alias-datatype rhs-dtype)]
       (cond
         (and (unaliased-integer-type? lhs-dtype)
              (unaliased-integer-type? rhs-dtype))
         :int64
         (and (or (unaliased-integer-type? lhs-dtype) (unaliased-float-type? lhs-dtype))
              (or (unaliased-integer-type? rhs-dtype) (unaliased-float-type? rhs-dtype)))
         :float64
         :else
         :object))))
  ([lhs-dtype]
   (simple-operation-space lhs-dtype lhs-dtype)))


;;Default object datatypes
(add-object-datatype! :string String)
(add-cast-fn! :string str)
(add-unchecked-cast-fn! :string str)
(defn keyword-cast
  [item]
  (when item
    (if-let [retval (keyword item)]
      retval
      (errors/throwf "Unable to cast to keyword: %s" item))))

(add-object-datatype! :keyword Keyword)
(add-cast-fn! :keyword keyword-cast)
(add-unchecked-cast-fn! :keyword keyword-cast)

(add-object-datatype! :decimal BigDecimal)
(defn decimal-cast
  [item]
  (when item
    (cond
      (instance? BigDecimal item)
      item
      (string? item)
      (BigDecimal. ^String item)
      :else
      (errors/throwf "Unable to cast item to decimal: %s" item))))

(add-cast-fn! :decimal decimal-cast)
(add-unchecked-cast-fn! :decimal decimal-cast)

(add-object-datatype! :uuid UUID)
(defn uuid-cast
  [item]
  (when item
    (cond
      (instance? UUID item)
      item
      (string? item)
      (UUID/fromString item)
      :else
      (errors/throwf "Unable to cast item to uuid: %s" item))))

(add-cast-fn! :uuid uuid-cast)
(add-unchecked-cast-fn! :uuid uuid-cast)
