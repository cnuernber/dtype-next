(ns tech.v3.tensor.dimensions.gtol-insn
  (:require [tech.v3.datatype.base :as dtype]
            [insn.core :as insn]
            [camel-snake-kebab.core :as csk])
  (:import [java.util.function Function]
           [java.util List Map HashMap ArrayList]
           [java.lang.reflect Constructor]
           [tech.v3.datatype Buffer LongReader]))


(defn- ast-symbol-access
  [ary-name dim-idx]
  {:ary-name ary-name
   :dim-idx dim-idx})


(defn- ast-instance-const-fn-access
  [ary-name dim-idx fn-name]
  {:ary-name ary-name
   :dim-idx dim-idx
   :fn-name fn-name})


(defn- make-symbol
  [symbol-stem dim-idx]
  (ast-symbol-access symbol-stem dim-idx))


(defn elemwise-ast
  [dim-idx direct? offsets? broadcast?
   trivial-stride?
   most-rapidly-changing-index?
   least-rapidly-changing-index?]
  (let [shape (make-symbol :shape dim-idx)
        stride (make-symbol :stride dim-idx)
        offset (make-symbol :offset dim-idx)
        shape-ecount-stride (make-symbol :shape-ecount-stride dim-idx)
        idx (if most-rapidly-changing-index?
              `~'idx
              `(~'quot ~'idx ~shape-ecount-stride))
        offset-idx (if offsets?
                     `(~'+ ~idx ~offset)
                     `~idx)
        shape-ecount (if direct?
                       `~shape
                       (ast-instance-const-fn-access :shape dim-idx :lsize))
        idx-bcast (if (or offsets? broadcast? (not least-rapidly-changing-index?))
                    `(~'rem ~offset-idx ~shape-ecount)
                    `~offset-idx)
        elem-idx (if direct?
                   `~idx-bcast
                   `(.read ~shape ~idx-bcast))]
    (if trivial-stride?
      `~elem-idx
      `(~'* ~elem-idx ~stride))))


(defn signature->ast
  [signature]
  (let [n-dims (long (:n-dims signature))
        direct-vec (:direct-vec signature)
        offsets? (:offsets? signature)
        trivial-last-stride? (:trivial-last-stride? signature)
        broadcast? (:broadcast? signature)]
    {:signature signature
     :ast
     (if (= n-dims 1)
       (elemwise-ast 0 (direct-vec 0) offsets? broadcast?
                     trivial-last-stride? true true)
       (let [n-dims-dec (dec n-dims)]
         (->> (range n-dims)
              (map (fn [dim-idx]
                     (let [dim-idx (long dim-idx)
                           least-rapidly-changing-index? (== dim-idx 0)
                           ;;This optimization removes the 'rem' on the most
                           ;;significant dimension.  Valid if we aren't
                           ;;broadcasting
                           most-rapidly-changing-index? (and (not broadcast?)
                                                             (== dim-idx n-dims-dec))
                           trivial-stride? (and most-rapidly-changing-index?
                                                trivial-last-stride?)]
                       (elemwise-ast dim-idx (direct-vec dim-idx) offsets? broadcast?
                                     trivial-stride? most-rapidly-changing-index?
                                     least-rapidly-changing-index?))))
              (apply list '+))))}))


(def constructor-args
  (let [name-map {:stride :strides
                  :shape-ecount :shape-ecounts
                  :shape-ecount-stride :shape-ecount-strides
                  :offset :offsets}]
    (->>
     [:shape :stride :offset :shape-ecount :shape-ecount-stride]
     (map-indexed (fn [idx argname]
                    ;;inc because arg0 is 'this' object
                    [argname {:arg-idx (inc (long idx))
                              :ary-name (get name-map argname argname)
                              :name (csk/->camelCase (name argname))}]))
     (into {}))))


(defn- rectify-shape-entry
  [shape-entry]
  (if (number? shape-entry)
    (long shape-entry)
    (dtype/->reader shape-entry)))


(defn reduced-dims->constructor-args
  [{:keys [shape strides offsets shape-ecounts shape-ecount-strides]}]
  (let [argmap {:shape (object-array (map rectify-shape-entry shape))
                :strides (long-array strides)
                :offsets (long-array offsets)
                :shape-ecounts  (long-array shape-ecounts)
                :shape-ecount-strides (long-array shape-ecount-strides)}]
    (->> (vals constructor-args)
         (map (fn [{:keys [ary-name]}]
                (if-let [carg (ary-name argmap)]
                  carg
                  (when-not (= ary-name :offsets)
                    (throw (Exception. (format "Failed to find constructor argument %s"
                                               ary-name)))))))
         (object-array))))


(defn bool->str
  ^String [bval]
  (if bval "T" "F"))


(defn direct-vec->str
  ^String [^List dvec]
  (let [builder (StringBuilder.)
        iter (.iterator dvec)]
    (loop [continue? (.hasNext iter)]
      (when continue?
        (let [next-val (.next iter)]
          (.append builder (bool->str next-val))
          (recur (.hasNext iter)))))
    (.toString builder)))


(defn ast-sig->class-name
  [{:keys [signature]}]
  (format "GToL%d%sOff%sBcast%sTrivLastS%s"
          (:n-dims signature)
          (direct-vec->str (:direct-vec signature))
          (bool->str (:offsets? signature))
          (bool->str (:broadcast? signature))
          (bool->str (:trivial-last-stride? signature))))


(defmulti apply-ast-fn!
  (fn [ast ^Map _fields ^List _instructions]
    (first ast)))


(defn ensure-field!
  [{:keys [ary-name dim-idx] :as field} ^Map fields]
  (-> (.computeIfAbsent
       fields field
       (reify Function
         (apply [this field]
           (assoc field
                  :field-idx (.size fields)
                  :name (if (:fn-name field)
                          (format "%s%d-%s"
                                  (csk/->camelCase (name ary-name))
                                  dim-idx
                                  (csk/->camelCase (name (:fn-name field))))
                          (format "%s%d"
                                  (csk/->camelCase (name ary-name))
                                  dim-idx))))))
      :name))


(defn push-arg!
  [ast fields ^List instructions]
  (cond
    (= 'idx ast)
    (.add instructions [:lload 1])
    (map? ast)
    (do
      (.add instructions [:aload 0])
      (.add instructions [:getfield :this (ensure-field! ast fields) :long]))
    (seq ast)
    (apply-ast-fn! ast fields instructions)
    :else
    (throw (Exception. (format "Unrecognized ast element: %s" ast)))))


(defn reduce-math-op!
  [math-op ast ^Map fields ^List instructions]
  (reduce (fn [prev-arg arg]
            (push-arg! arg fields instructions)
            (when-not (nil? prev-arg)
              (.add instructions [math-op]))
            arg)
          nil
          (rest ast)))


(defmethod apply-ast-fn! '+
  [ast fields instructions]
  (reduce-math-op! :ladd ast fields instructions))


(defmethod apply-ast-fn! '*
  [ast fields instructions]
  (reduce-math-op! :lmul ast fields instructions))


(defmethod apply-ast-fn! 'quot
  [ast fields instructions]
  (reduce-math-op! :ldiv ast fields instructions))


(defmethod apply-ast-fn! 'rem
  [ast fields instructions]
  (reduce-math-op! :lrem ast fields instructions))


(defmethod apply-ast-fn! '.read
  [ast ^Map fields ^List instructions]
  (when-not (= 3 (count ast))
    (throw (Exception. (format "Invalid .read ast: %s" ast))))
  (let [[_opname this-obj idx] ast]
    (.add instructions [:aload 0])
    (.add instructions [:getfield :this (ensure-field! this-obj fields) Buffer])
    (push-arg! idx fields instructions)
    (.add instructions [:invokeinterface Buffer "readLong"])))


(defmethod apply-ast-fn! '.lsize
  [ast ^Map fields ^List instructions]
  (when-not (= 2 (count ast))
    (throw (Exception. (format "Invalid .read ast: %s" ast))))
  (let [[_opname this-obj] ast]
    (.add instructions [:aload 0])
    (.add instructions [:getfield :this (ensure-field! this-obj fields) Buffer])
    (.add instructions [:invokeinterface Buffer "lsize"])))


(defn eval-read-ast!
  "Eval the read to isns instructions"
  [ast ^Map fields ^List instructions]
  ;;The ast could be 'idx
  (if (= ast 'idx)
    (push-arg! ast fields instructions)
    (apply-ast-fn! ast fields instructions))
  (.add instructions [:lreturn]))


(defn emit-fields
  [shape-scalar-vec ^Map fields]
  (concat
   (->> (vals fields)
        (sort-by :name)
        (mapv (fn [{:keys [name _field-idx ary-name dim-idx fn-name]}]
                (if (and (= ary-name :shape)
                         (not (shape-scalar-vec dim-idx)))
                  (if fn-name
                    {:flags #{:public :final}
                     :name name
                     :type :long}
                    {:flags #{:public :final}
                     :name name
                     :type Buffer})
                  {:flags #{:public :final}
                   :name name
                   :type :long}))))
   [{:flags #{:public :final}
     :name "nElems"
     :type :long}]))


(defn carg-idx
  ^long [argname]
  (if-let [idx-val (get-in constructor-args [argname :arg-idx])]
    (long idx-val)
    (throw (Exception. (format "Unable to find %s in %s"
                               argname
                               (keys constructor-args))))))


(defn load-constructor-arg
  [shape-scalar-vec {:keys [ary-name _field-idx name dim-idx fn-name]}]
  (let [carg-idx (carg-idx ary-name)]
    (if (= ary-name :shape)
      (if (not (shape-scalar-vec dim-idx))
        (if fn-name
          [[:aload 0]
           [:aload carg-idx]
           [:ldc (int dim-idx)]
           [:aaload]
           [:checkcast Buffer]
           [:invokeinterface Buffer (clojure.core/name fn-name)]
           [:putfield :this name :long]]
          [[:aload 0]
           [:aload carg-idx]
           [:ldc (int dim-idx)]
           [:aaload]
           [:checkcast Buffer]
           [:putfield :this name Buffer]])
        [[:aload 0]
         [:aload carg-idx]
         [:ldc (int dim-idx)]
         [:aaload]
         [:checkcast Long]
         [:invokevirtual Long "longValue"]
         [:putfield :this name :long]])
      [[:aload 0]
       [:aload carg-idx]
       [:ldc (int dim-idx)]
       [:laload]
       [:putfield :this name :long]])))


(defn emit-constructor
  [shape-type-vec ^Map fields]
  (concat [[:aload 0]
           [:invokespecial :super :init [:void]]]
          (->> (vals fields)
               (sort-by :name)
               (mapcat (partial load-constructor-arg shape-type-vec)))
          [[:aload 0]
           [:aload 4]
           [:ldc (int 0)]
           [:laload]
           [:aload 5]
           [:ldc (int 0)]
           [:laload]
           [:lmul]
           [:putfield :this "nElems" :long]
           [:return]]))


(defn gen-ast-class-def
  [{:keys [signature ast] :as ast-data}]
  (let [cname (ast-sig->class-name ast-data)
        pkg-symbol (symbol (format "tech.v3.datatype.%s" cname))
        ;;map of name->field-data
        fields (HashMap.)
        read-instructions (ArrayList.)
        ;;Which of the shape items are scalars
        ;;they are either scalars or readers.
        shape-scalar-vec (:direct-vec signature)]
    (eval-read-ast! ast fields read-instructions)
    {:name pkg-symbol
     :interfaces [LongReader]
     :fields (vec (emit-fields shape-scalar-vec fields))
     :methods [{:flags #{:public}
                :name :init
                :desc [(Class/forName "[Ljava.lang.Object;")
                       (Class/forName "[J")
                       (Class/forName "[J")
                       (Class/forName "[J")
                       (Class/forName "[J")
                       :void]
                :emit (vec (emit-constructor shape-scalar-vec fields))}
               {:flags #{:public}
                :name "lsize"
                :desc [:long]
                :emit [[:aload 0]
                       [:getfield :this "nElems" :long]
                       [:lreturn]]}
               {:flags #{:public}
                :name "readLong"
                :desc [:long :long]
                :emit (vec read-instructions)}]}))


(defn generate-constructor
  "Given a signature, return a fucntion that, given the reduced dimensions returns a
  implementation of a long reader that maps a global dimension to a local dimension."
  [signature]
  (let [ast-data (signature->ast signature)
        class-def (gen-ast-class-def ast-data)]
    (try
      ;;nested so we capture the class definition
      (let [^Class class-obj (insn/define class-def)
            ^Constructor first-constructor
            (first (.getDeclaredConstructors
                    class-obj))]
        #(try
           (let [constructor-args (reduced-dims->constructor-args %)]
             (.newInstance first-constructor constructor-args))
           (catch Throwable e
             (throw
              (ex-info (format "Error instantiating ast object: %s\n%s"
                               e
                               (with-out-str
                                 (println (:ast ast-data))))
                       {:error e
                        :class-def class-def
                        :signature signature})))))
      (catch Throwable e
        (throw (ex-info (format "Error generating ast object: %s\n%s"
                                e
                                (with-out-str
                                  (println (:ast ast-data))))
                        {:error e
                         :class-def class-def
                         :signature signature}))))))
