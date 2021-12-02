(ns tech.v3.datatype.export-symbols
  (:require [clojure.java.io :as io])
  (:import [java.io Writer]))


(defmacro export-symbols
  [src-ns & symbol-list]
  `(do
     (require '~src-ns)
     ~@(->> symbol-list
            (mapv
             (fn [sym-name]
               `(let [varval# (requiring-resolve (symbol ~(name src-ns)
                                                         ~(name sym-name)))
                      var-meta# (meta varval#)]
                  (when-not varval#
                    (throw (ex-info
                            (format "Failed to find symbol '%s' in namespace '%s'"
                                    ~(name sym-name) ~(name src-ns))
                            {:symbol '~sym-name
                             :src-ns '~src-ns})))
                  (when (:macro var-meta#)
                    (throw
                     (ex-info
                      (format "Cannot export macros as this breaks aot: %s"
                              '~sym-name)
                      {:symbol '~sym-name})))
                  (def ~(symbol (name sym-name)) @varval#)
                  (alter-meta! #'~(symbol (name sym-name))
                               merge
                               (-> (select-keys var-meta#
                                                [:file :line :column
                                                 :doc
                                                 :column :tag
                                                 :arglists])
                                   (assoc :export-info {:src-ns '~src-ns
                                                        :src-sym '~sym-name})))))))))

(defn- write!
  ^Writer [^Writer writer data & datas]
  (.append writer (str data))
  (doseq [data datas]
    (when data
      (.append writer (str data))))
  writer)


(defn- writeln!
  ^Writer [^Writer writer strdata & strdatas]
  (.append writer (str strdata))
  (doseq [data strdatas]
    (when data
      (.append writer (str data))))
  (.append writer "\n")
  writer)

(defn- indent!
  ^Writer [^Writer writer ^long n-spaces]
  (dotimes [sp n-spaces]
    (.append writer \space))
  writer)



(defn write-api!
  [src-ns-symbol dst-ns-symbol dst-clj-file]
  (let [ns-doc (:doc (meta (the-ns src-ns-symbol)))
        pubs (ns-publics src-ns-symbol)
        metas (->> (map meta (vals pubs))
                   (sort-by (comp name :name)))
        require-ns (concat [src-ns-symbol]
                           (->> metas
                                (map (comp :src-ns :export-info))
                                (filter identity)
                                (distinct)
                                (sort-by name)))]
    (with-open [writer (io/writer dst-clj-file)]
      ;;ns declaration
      (-> writer
          (writeln! "(ns " (name dst-ns-symbol))
          (indent! 2)
          (write! "\"")
          (write! ns-doc)
          (writeln! "\"")
          (indent! 2)
          (write! "(:require "))
      (doseq [req require-ns]
        (when-not (= req src-ns-symbol)
          (indent! writer 12))
        (writeln! writer "[" req "]"))
      (indent! writer 12)
      (writeln! writer "))\n")
      (doseq [data metas]
        (cond
          (:arglists data)
          (do
            (let [macro? (:macro data)]
              (if macro?
                (writeln! writer "(defmacro " (:name data))
                (writeln! writer "(defn " (:name data)))
              (when-not (empty? (:doc data))
                (indent! writer 2)
                (write! writer "\"")
                (write! writer (:doc data))
                (writeln! writer "\""))
              (doseq [arglist (:arglists data)]
                (indent! writer 2)
                (write! writer "(")
                (when-let [meta (meta arglist)]
                  (write! writer "^" (str meta) " "))
                (writeln! writer (str arglist))
                (let [varargs (seq (rest (drop-while #(not= '& %) arglist)))
                      varargs (if varargs ['args] nil)
                      arglist (->> (concat (take-while #(not= '& %) arglist)
                                           varargs)
                                   (map (fn [arglist-arg]
                                          (cond
                                            (symbol? arglist-arg)
                                            arglist-arg
                                            (map? arglist-arg)
                                            (if (arglist-arg :as)
                                              (arglist-arg :as)
                                              arglist-arg)))))]
                  (indent! writer 2)
                  (when macro?
                    (write! writer "`"))
                  (let [[src-ns-symbol fn-name]
                        (if-let [export-info (:export-info data)]
                          [(:src-ns export-info) (:src-sym export-info)]
                          [src-ns-symbol (:name data)])]
                    (if varargs
                      (write! writer "(apply " src-ns-symbol "/" fn-name " ")
                      (write! writer "(" src-ns-symbol "/" fn-name " ")))
                  (->> (if macro?
                         (map (fn [sym-name]
                                (symbol (str "~" (name sym-name))))
                              arglist)
                         arglist)
                       (interpose " ")
                       (apply write! writer)))
                (writeln! writer "))")))
            (writeln! writer ")\n\n"))
          :else
          (println "Unrecognized data" data))))))


(comment
  (write-api! 'tech.v3.datatype-api
              'tech.v3.datatype
              "src/tech/v3/datatype.clj")
  )
