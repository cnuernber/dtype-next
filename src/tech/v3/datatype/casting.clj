(ns tech.v3.datatype.casting
  (:refer-clojure :exclude [cast])
  (:require [clojure.set :as c-set]
            [primitive-math :as pmath]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :as errors])
  (:import [java.util Map Set HashSet]
           [java.util.concurrent ConcurrentHashMap]
           [clojure.lang RT Keyword]
           [java.util UUID]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(declare rebuild-valid-datatypes!)

(defonce aliased-datatypes (ConcurrentHashMap.))


(defn alias-datatype!
  "Alias a new datatype to a base datatype.  Only useful for primitive datatypes"
  [new-dtype old-dtype]
  (.put ^Map aliased-datatypes new-dtype old-dtype)
  (rebuild-valid-datatypes!))


(defn un-alias-datatype
  [dtype]
  (.getOrDefault ^Map aliased-datatypes dtype dtype))


;;Only object datatypes are represented in these maps
(defonce ^ConcurrentHashMap datatype->class-map (ConcurrentHashMap.))
(defonce ^ConcurrentHashMap class->datatype-map (ConcurrentHashMap.))



(defn add-object-datatype!
  ;;Add an object datatype.
  ([datatype klass & [implement-protocols?]]
   (.put datatype->class-map datatype klass)
   (.put class->datatype-map klass datatype)
   (rebuild-valid-datatypes!)
   (when implement-protocols?
     (clojure.core/extend klass
       dtype-proto/PElemwiseDatatype
       {:elemwise-datatype (constantly datatype)}
       dtype-proto/PDatatype
       {:datatype (constantly datatype)}))
   :ok))


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

(def unsigned-signed (c-set/map-invert signed-unsigned))

(def float-types #{:float32 :float64})

(def int-types (set (concat (flatten (seq signed-unsigned)) [:char])))

(def signed-int-types (set (keys signed-unsigned)))

(def unsigned-int-types (set (vals signed-unsigned)))

(def host-numeric-types (set (concat signed-int-types float-types)))

(def numeric-types (set (concat host-numeric-types unsigned-int-types [:char])))


(defonce valid-datatype-set (atom nil))

(defn ->hash-set
  [data]
  (doto (HashSet.)
    (.addAll data)))

(defn rebuild-valid-datatypes!
  []
  (reset! valid-datatype-set
          (->> (concat (keys datatype->class-map)
                       (keys aliased-datatypes)
                       numeric-types
                       [:object :boolean])
               (->hash-set))))


(defn valid-datatype?
  [datatype]
  (.contains ^Set @valid-datatype-set datatype))


(defn ensure-valid-datatype
  [datatype]
  (when-not (valid-datatype? datatype)
    (throw (Exception. (format "Invalid datatype: %s" datatype)))))


(def primitive-types (set (concat numeric-types [:boolean])))


(def base-host-datatypes (set (concat host-numeric-types
                                      [:object :boolean])))


(def base-datatypes (set (concat host-numeric-types
                                 unsigned-int-types
                                 [:boolean :char :object])))


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
    (long (cond
            (int-types dtype)
            (quot (int-width dtype) 8)
            (float-types dtype)
            (quot (float-width dtype) 8)
            :else
            (throw (ex-info (format "datatype is not numeric: %s" dtype)
                            {:datatype dtype}))))))


(defn numeric-type?
  [dtype]
  (let [dtype (un-alias-datatype dtype)]
    (boolean
     (or (int-types dtype)
         (float-types dtype)))))

(defn float-type?
  [dtype]
  (let [dtype (un-alias-datatype dtype)]
    (boolean
     (float-types dtype))))


(defn integer-type?
  [dtype]
  (let [dtype (un-alias-datatype dtype)]
    (boolean (int-types dtype))))


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
    (double item)))


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
  (cond
    (numeric-type? src-dtype)
    `(boolean (not= 0.0 (unchecked-double ~item)))
    (= :boolean src-dtype)
    `(boolean ~item)
    :else
    `(boolean
      (let [item# ~item]
        (if (number? item#)
          (not= 0.0 (unchecked-double item#))
          item#)))))

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


(defonce ^:dynamic *cast-table* (atom {}))
(defonce ^:dynamic *unchecked-cast-table* (atom {}))


(defn add-cast-fn!
  [datatype cast-fn]
  (swap! *cast-table* assoc datatype cast-fn))


(defn add-unchecked-cast-fn!
  [datatype cast-fn]
  (swap! *unchecked-cast-table* assoc datatype cast-fn))


(defmacro add-all-cast-fns
  []
  `(do
     ~@(for [dtype base-datatypes]
         [`(add-cast-fn! ~dtype #(datatype->cast-fn :unknown ~dtype %))
          `(add-unchecked-cast-fn! ~dtype #(datatype->unchecked-cast-fn
                                            :unknown ~dtype %))])))

(def casts (add-all-cast-fns))


(defn all-datatypes
  []
  (keys @*cast-table*))

(def all-host-datatypes
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


(defn safe-flatten
  [dtype]
  (-> dtype
      datatype->safe-host-type
      flatten-datatype))


(defn host-flatten
  [dtype]
  (-> dtype
      un-alias-datatype
      datatype->host-datatype
      flatten-datatype))


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



(def type-tree (atom {}))


(defn- set-conj
  [container item]
  (if container
    (conj container item)
    #{item}))


(defn add-type-pair
  [child-type parent-type]
  (swap! type-tree update child-type set-conj parent-type))


(def default-type-pairs
  [[:int8 :int16]
   [:uint8 :int16]
   [:int16 :int32]
   [:uint16 :int32]
   [:int32 :int64]
   [:uint32 :int64]
   [:int64 :float64]
   [:uint64 :float64]
   [:float32 :float64]
   [:int16 :float32]
   [:uint16 :float32]])


(doseq [[child parent] default-type-pairs]
  (add-type-pair child parent))


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
        (type-path->root datatype [] @type-tree)
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
   (cond
     (and (= :boolean lhs-dtype)
          (= :boolean rhs-dtype))
     :boolean
     (and (integer-type? lhs-dtype)
          (integer-type? rhs-dtype))
     :int64
     (and (numeric-type? lhs-dtype)
          (numeric-type? rhs-dtype))
     :float64
     :else
     :object))
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
