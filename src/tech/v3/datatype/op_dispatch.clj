(ns tech.v3.datatype.op-dispatch
  (:require [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.argtypes :refer [arg-type]]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.language :refer [cond constantly]]
            [ham-fisted.protocols :as hamf-proto]
            [ham-fisted.defprotocol :refer [extend extend-protocol extend-type]]
            [ham-fisted.print :refer [implement-tostring-print]]
            [ham-fisted.iterator :as hamf-iter])
  (:import [tech.v3.datatype UnaryOperator BinaryOperator Buffer LongReader DoubleReader ObjectReader
            UnaryOperators$LongUnaryOperator UnaryOperators$DoubleUnaryOperator
            UnaryOperators$ObjLongUnaryOperator UnaryOperators$ObjDoubleUnaryOperator]
           [ham_fisted Casts ITypedReduce Transformables$IterableSeq]
           [java.util Iterator])
  (:refer-clojure :exclude [cond constantly extend extend-protocol extend-type]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn- ->olo
  ^clojure.lang.IFn$OLO [ff]
  (if (instance? clojure.lang.IFn$OLO ff)
    ff
    (fn [acc ^long v] (ff acc v))))

(defn- ->odo
  ^clojure.lang.IFn$ODO [ff]
  (if (instance? clojure.lang.IFn$ODO ff)
    ff
    (fn [acc ^double v] (ff acc v))))

(defn- ->ooo
  ^clojure.lang.IFn [ff] ff)

(defn- unary-op-type
  [in-space out-space]
  (cond
    (and (casting/unaliased-numeric-type? in-space)
         (identical? in-space out-space))
    (if (identical? in-space :int64)
      :long-long
      :double-double)
    (identical? out-space :int64)
    :object-long
    (identical? out-space :float64)
    :object-double
    :else
    :object-object))

(defn- nest-unary-ops
  [^UnaryOperator l l-op-space l-res-space
   ^UnaryOperator r r-op-space r-res-space]
  (let [in-space (-> (casting/simple-operation-space l-res-space r-op-space)
                     (casting/simple-operation-space l-op-space))
        out-space r-res-space
        op-type (unary-op-type in-space out-space)]
    (cond
      (identical? op-type :long-long)
      {:operator (reify UnaryOperators$LongUnaryOperator
                   (unaryLong [this v] (->> (.unaryLong l v) (.unaryLong r)))
                   dt-proto/PInputDatatype (input-datatype [this] :int64)
                   hamf-proto/ReturnedDatatype (returned-datatype [this] :int64))
       :input-space :int64
       :output-space :int64}
      (identical? op-type :double-double)
      {:operator (reify UnaryOperators$DoubleUnaryOperator
                   (unaryDouble [this v] (->> (.unaryDouble l v) (.unaryDouble r)))
                   dt-proto/PInputDatatype (input-datatype [this] :float64)
                   hamf-proto/ReturnedDatatype (returned-datatype [this] :float64))
       :input-space :float64
       :output-space :float64}
      (identical? op-type :object-long)
      {:operator (reify UnaryOperators$ObjLongUnaryOperator
                   (unaryObjLong [this v] (->> (.unaryObject l v) (.unaryObjLong r)))
                   dt-proto/PInputDatatype (input-datatype [this] :object)
                   hamf-proto/ReturnedDatatype (returned-datatype [this] :int64))
       :input-space :object
       :output-space :int64}
      (identical? op-type :object-double)
      {:operator (reify UnaryOperators$ObjDoubleUnaryOperator
                   (unaryObjDouble [this v] (->> (.unaryObject l v) (.unaryObjDouble r)))
                   dt-proto/PInputDatatype (input-datatype [this] :object)
                   hamf-proto/ReturnedDatatype (returned-datatype [this] :float64))
       :input-space :object
       :output-space :float64}
      :else
      {:operator (reify UnaryOperator
                   (unaryObject [this v] (->> (.unaryObject l v) (.unaryObject r)))
                   dt-proto/PInputDatatype (input-datatype [this] :object)
                   hamf-proto/ReturnedDatatype (returned-datatype [this] :object))
       :input-space :object
       :output-space out-space})))

(defmacro ^:private unary-op-reader
  [in-dtype out-dtype reported-dtype]
  (let [[basecls read-op rfn-cast rfn-invoke]
        (cond
          (identical? out-dtype :int64) ['LongReader 'readLong '->olo '.invokePrim]
          (identical? out-dtype :float64) ['DoubleReader 'readDouble '->odo '.invokePrim]
          :else ['ObjectReader 'readObject '->ooo '.invoke])
        [dot-read-op unary-op rf-v-tag]
        (cond
          (identical? in-dtype :int64) ['.readLong '.unaryLong 'long]
          (identical? in-dtype :float64) ['.readDouble '.unaryDouble 'double]
          :else ['.readObject (cond (identical? out-dtype :int64) '.unaryObjLong
                                    (identical? out-dtype :float64) '.unaryObjDouble
                                    :else '.unaryObject)
                 nil])]
    `(reify
       dt-proto/POperationalElemwiseDatatype
       (operational-elemwise-datatype [~'this] ~reported-dtype)
       ~basecls
       (elemwiseDatatype [~'rdr] ~reported-dtype)
       (lsize [~'rdr] (.lsize ~'arg-rdr))
       (~read-op [~'rdr ~'idx] (~unary-op ~'un-op (~dot-read-op ~'arg-rdr ~'idx)))
       (reduce [~'rdr ~'rfn ~'acc]
         (let [~'rfn (~rfn-cast ~'rfn)]
           (reduce (fn [~'acc ~(with-meta 'v {:tag rf-v-tag})]
                     (~rfn-invoke ~'rfn ~'acc (~unary-op ~'un-op ~'v)))
                   ~'acc
                   ~'arg-rdr)))
       (subBuffer [~'rdr ~'sidx ~'eidx]
         (dt-proto/apply-unary-op (.subBuffer ~'arg-rdr ~'sidx ~'eidx) ~'un-op ~in-dtype ~reported-dtype))
       dt-proto/PApplyUnary
       (apply-unary-op [~'lhs ~'un-op2 ~'in-dtype2 ~'out-dtype2]
         (let [{:keys [~'operator ~'input-space ~'output-space]}
               (nest-unary-ops ~'un-op ~in-dtype ~out-dtype
                               ~'un-op2 ~'in-dtype2 ~'out-dtype2)]
           (do-apply-unary-op ~'arg-rdr ~'operator ~'input-space ~'output-space))))))

(defn- do-apply-unary-op
  ([^Buffer arg-rdr ^UnaryOperator un-op in-dtype out-dtype reported-dtype]
   (let [op-type (unary-op-type in-dtype out-dtype)]
     (cond
       (identical? op-type :long-long)
       (unary-op-reader :int64 :int64 reported-dtype)
       (identical? op-type :object-long)
       (unary-op-reader :object :int64 reported-dtype)
       (identical? op-type :double-double)
       (unary-op-reader :float64 :float64 reported-dtype)
       (identical? op-type :object-double)
       (unary-op-reader :object :float64 reported-dtype)
       :else ;;object-object
       (unary-op-reader :object :object reported-dtype ))))
  ([arg-rdr un-op in-dtype out-dtype]
   (do-apply-unary-op arg-rdr un-op in-dtype (casting/simple-operation-space out-dtype) out-dtype)))

(extend-protocol dt-proto/PApplyUnary
  Buffer
  (apply-unary-op [this op in-dtype out-dtype]
    (do-apply-unary-op this op in-dtype out-dtype)))

(declare typed-map-1)

(deftype UnaryMap [res op in-space out-space reader-dtype arg m]
  Iterable (iterator [this] (.iterator ^Iterable res))
  dt-proto/PElemwiseDatatype (elemwise-datatype [e] reader-dtype)
  dt-proto/POperationalElemwiseDatatype (operational-elemwise-datatype [e] reader-dtype)
  dt-proto/PApplyUnary
  (apply-unary-op [this un-op2 in-space2 out-space2]
    (let [{:keys [operation input-space output-space]}
          (nest-unary-ops op in-space out-space
                          un-op2 in-space2 out-space2)]
      (typed-map-1 operation input-space output-space output-space arg)))
  ITypedReduce
  (reduce [this rfn acc]
    (.reduce ^ITypedReduce res rfn acc))
  Object
  (toString [this] (.toString ^Object res))
  (hashCode [this] (.hashCode ^Object res))
  (equals [this o] (.equals ^Object res o))
  Transformables$IterableSeq
  (hasheq [this] (.hasheq ^clojure.lang.IHashEq res))  
  (equiv [this o] (clojure.lang.Util/pcequiv this res))
  (meta [this] m)
  (withMeta [this mm] (UnaryMap. this op in-space out-space reader-dtype arg mm)))

(implement-tostring-print UnaryMap)

(defn- typed-map-1
  [^UnaryOperator op in-space out-space reader-dtype arg]
  (let [optype (unary-op-type in-space out-space)
        map-fn (cond
                 (identical? optype :long-long)
                 (fn ^long [^long v] (.unaryLong op v))
                 (identical? optype :object-long)
                 (fn ^long [v] (.unaryObjLong op v))
                 (identical? optype :double-double)
                 (fn ^double [^double v] (.unaryDouble op v))
                 (identical? optype :object-double)
                 (fn ^double [v] (.unaryObjDouble op v))
                 :else
                 (fn [v] (.unaryObject op v)))
        res (lznc/map map-fn arg)]
    (UnaryMap. res op in-space out-space reader-dtype arg nil)))

(defn- unary-dispatch
  [unary-op arg arg-type in-space out-space reader-dtype]
  (if (identical? arg-type :iterable)
    (typed-map-1 unary-op in-space out-space reader-dtype arg)
    (let [arg-rdr (dt-proto/elemwise-reader-cast arg in-space)
          rv (dt-proto/apply-unary-op arg-rdr unary-op in-space reader-dtype)]
      (if (identical? arg-type :tensor)
        (let [arg-shp (dt-proto/shape arg)]
          (if (> (count arg-shp) 1)
            (dt-proto/reshape rv arg-shp))
          rv)
        rv))))

(defn dispatch-unary-op
  ([^UnaryOperator op x]
   (let [xt (arg-type x)]
     (if (identical? xt :scalar)
       (.unaryObject op x)
       (let [os (casting/simple-operation-space (dt-proto/operational-elemwise-datatype x))]
         (unary-dispatch op x xt os os os)))))
  ([^UnaryOperator op input-space output-space x]
   (let [xt (arg-type x)]
     (if (identical? xt :scalar)
       (.unaryObject op x)
       (let [input-space (casting/simple-operation-space (dt-proto/operational-elemwise-datatype x) input-space)
             reported-space output-space
             output-space (casting/simple-operation-space output-space)]
         (unary-dispatch op x xt input-space output-space reported-space))))))

(defn- unary-op-l
  [^BinaryOperator binary-op scalar op-space]
  (cond
    (identical? op-space :int64)
    (let [scalar (Casts/longCast scalar)]
      (reify UnaryOperator
        (unaryLong [this b] (.binaryLong binary-op scalar b))))
    (identical? op-space :float64)
    (let [scalar (Casts/doubleCast scalar)]
      (reify UnaryOperator
        (unaryDouble [this b] (.binaryDouble binary-op scalar b))))
    :else
    (reify UnaryOperator
      (unaryObject [this b] (.binaryObject binary-op scalar b)))))

(defn- unary-op-r
  [^BinaryOperator binary-op scalar op-space]
  (cond
    (identical? op-space :int64)
    (let [scalar (Casts/longCast scalar)]
      (reify UnaryOperators$LongUnaryOperator
        (unaryLong [this a] (.binaryLong binary-op a scalar))))
    (identical? op-space :float64)
    (let [scalar (Casts/doubleCast scalar)]
      (reify UnaryOperators$DoubleUnaryOperator
        (unaryDouble [this a] (.binaryDouble binary-op a scalar))))
    :else
    (reify UnaryOperator
      (unaryObject [this a] (.binaryObject binary-op a scalar)))))

(defmacro ^:private binary-op-reader
  ([dtype basecls bin-op read-op]
   (let [dot-read-op (symbol (str "." read-op))]
     `(reify ~basecls
        (elemwiseDatatype [~'this] ~dtype)
        (lsize [~'this] ~'ne)
        (~read-op [~'this ~'idx] (~bin-op ~'op (~dot-read-op ~'l ~'idx) (~dot-read-op ~'r ~'idx)))
        (subBuffer [~'this ~'sidx ~'eidx]
          (if (and (== ~'sidx 0) (== ~'eidx (.lsize ~'this)))
            ~'this
            (apply-binary-op ~'op ~'in-space ~'out-space ~'reader-dtype
                             (.subBuffer ~'l ~'sidx ~'eidx)
                             (.subBuffer ~'r ~'sidx ~'eidx))))))))

(defn- apply-binary-op
  [^BinaryOperator op in-space out-space reader-dtype l r]
  (let [^Buffer l (dt-proto/elemwise-reader-cast l in-space)
        ^Buffer r (dt-proto/elemwise-reader-cast r in-space)
        ne (min (.lsize l) (.lsize r))]
    (cond
      (identical? out-space :int64)
      (binary-op-reader reader-dtype LongReader .binaryLong readLong)
      (identical? out-space :float64)
      (binary-op-reader reader-dtype DoubleReader .binaryDouble readDouble)
      :else
      (binary-op-reader reader-dtype ObjectReader .binaryObject readObject))))

(deftype TypedMap [res op-dt m]
  Iterable (iterator [this] (.iterator ^Iterable res))
  dt-proto/PElemwiseDatatype (elemwise-datatype [e] op-dt)
  dt-proto/POperationalElemwiseDatatype (operational-elemwise-datatype [e] op-dt)
  ITypedReduce
  (reduce [this rfn acc]
    (.reduce ^clojure.lang.IReduceInit res rfn acc))
  Object
  (toString [this] (.toString ^Object res))
  (hashCode [this] (.hashCode ^Object res))
  (equals [this o] (.equals ^Object res o))
  Transformables$IterableSeq
  (hasheq [this] (.hasheq ^clojure.lang.IHashEq res))  
  (equiv [this o] (clojure.lang.Util/pcequiv this res))
  (meta [this] m)
  (withMeta [this mm] (TypedMap. res op-dt m)))

(implement-tostring-print TypedMap)

(defn typed-map-2
  [^BinaryOperator op op-dt reported-dt a b]
  (let [map-fn (cond
                 (identical? op-dt :int64)
                 (fn ^long [^long l ^long r] (.binaryLong op l r))
                 (identical? op-dt :float64)
                 (fn ^double [^double l ^double r] (.binaryDouble op l r))
                 :else
                 #(.binaryObject op %1 %2))
        res (lznc/map map-fn a b)]
    (TypedMap. res reported-dt nil)))

(defn- do-dispatch-binary-op
  [op in-space out-space reported-space a at b bt]
  (cond
    (or (identical? at :scalar)
        (identical? bt :scalar))
    (if (identical? at :scalar)
      (unary-dispatch (unary-op-l op a in-space) b bt in-space out-space reported-space)
      (unary-dispatch (unary-op-r op b in-space) a at in-space out-space reported-space))
    (or (identical? at :iterator)
        (identical? bt :iterator))
    (typed-map-2 op out-space reported-space a b)
    :else
    (apply-binary-op op in-space out-space reported-space a b)))

(defn dispatch-binary-op
  ([op a b]
   (let [at (arg-type a)
         bt (arg-type b)]
     (if (and (identical? at bt)
              (identical? at :scalar))
       (.binaryObject ^BinaryOperator op a b)
       (let [a-dt (dt-proto/operational-elemwise-datatype a)
             b-dt (dt-proto/operational-elemwise-datatype b)
             op-dt (casting/simple-operation-space a-dt b-dt)]
         (do-dispatch-binary-op op op-dt op-dt op-dt a at b bt)))))
  ([op in-space out-space a b]
   (let [at (arg-type a)
         bt (arg-type b)]
     (if (and (identical? at bt)
              (identical? at :scalar))
       (.binaryObject ^BinaryOperator op a b)
       (let [a-dt (dt-proto/operational-elemwise-datatype a)
             b-dt (dt-proto/operational-elemwise-datatype b)
             in-space (-> (casting/simple-operation-space a-dt b-dt)
                          (casting/simple-operation-space in-space))
             reported-space out-space
             out-space (casting/simple-operation-space out-space)]
         (do-dispatch-binary-op op in-space out-space reported-space a at b bt))))))

(defn apply-op
  ([op arg]
   (let [op (unary-op/->unary-operator op)
         in-space (dt-proto/input-datatype op)
         out-space (hamf-proto/returned-datatype op)]
     (if (and in-space out-space)
       ;;Op defines fixed output space it produces
       (dispatch-unary-op op in-space out-space arg)
       ;;Op climbs number tower
       (dispatch-unary-op op arg))))
  ([op arg1 arg2]
   (let [op (binary-op/->binary-operator op)
         in-space (dt-proto/input-datatype op)
         out-space (hamf-proto/returned-datatype op)]
     (if (and in-space out-space)
       (dispatch-binary-op op in-space out-space arg1 arg2)
       (dispatch-binary-op op arg1 arg2)))))

(defn typed-map-n
  [map-fn output-dtype args arg-types]
  (let [res (->> args
                 (lznc/map (fn [arg arg-type]
                             (if (identical? arg-type :scalar)
                               (hamf-iter/const-iterable arg)
                               arg))
                           args arg-types)
                 (apply lznc/map map-fn))]
    (TypedMap. res output-dtype nil)))

(comment
  (def in (dispatch-unary-op unary-op/log1p :float64 :float64 (vec (range 10 100 10))))
  (def rr (dispatch-unary-op unary-op/round :int64 :int64 in))
  )
