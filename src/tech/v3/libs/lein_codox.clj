(ns tech.v3.libs.lein-codox
  (:require [codox.main :as codox]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))


(defn find-vals
  [proj-file key-seq]
  (let [pvec (vec proj-file)
        key-set (set key-seq)
        n-elems (count pvec)]
    (loop [idx 0
           retval {}]
      (if (< idx n-elems)
        (let [[idx retval]
              (if (key-set (pvec idx))
                [(inc idx) (assoc retval
                                  (pvec idx)
                                  (pvec (inc idx)))]
                [idx retval])]
          (recur (inc idx) retval))
        retval))))


(defn get-codox-options
  [args]
  (let [base-argmap (first args)]
    (merge base-argmap
           (cond
             (.exists (io/file "deps.edn"))
             (let [deps-edn (edn/read-string (slurp "deps.edn"))
                   retval (->> (:arg-paths base-argmap)
                               (map #(get-in deps-edn %))
                               (apply merge))]
               (println "RETVAL is" retval)
               retval)
             (.exists (io/file "project.clj"))
             (let [proj-file (edn/read-string (slurp "project.clj"))
                   proj-name (name (second proj-file))
                   val-map (find-vals proj-file [:description :profiles])
                   codox-section (get-in val-map [:profiles :codox :codox])]
               (merge
                {:name proj-name
                 :description (:description val-map "")
                 :version (nth proj-file 2)}
                codox-section))))))


(defn -main
  [& args]
  (codox/generate-docs (get-codox-options args)))
