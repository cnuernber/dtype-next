(ns tech.v2.tensor.color-gradients.build-gradients
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.edn :as edn]
            [tech.libs.buffered-image :as bufimg]
            [tech.v2.tensor :as dtt]
            [tech.v2.datatype :as dtype])
  (:import [java.net URL]))



(def gradient-root-url (:base-url (edn/read-string
                                   (slurp "build-data/mathematica-gradients.edn"))))

(def gradient-map (edn/read-string (slurp "build-data/parsed-gradients.edn")))


(defn image->gradient-line
  [img-partial-path]
  (let [img (bufimg/load (URL. (str gradient-root-url img-partial-path)))
        _ (bufimg/save! img "PNG" "temp.png")
        sh-val (sh/sh "mogrify" "-trim" "temp.png")
        _ (when-not (= 0 (long (:exit sh-val)))
            (throw (Exception. (str (:err sh-val)))))
        img (bufimg/load "temp.png")
        ;;Ensure we know the format of the image.
        dst-img (bufimg/new-image (.getWidth img) (.getHeight img) :byte-bgr)
        _ (bufimg/draw-image! img dst-img)]
    (-> dst-img
        (bufimg/as-ubyte-tensor)
        (dtt/select 10 :all :all))))


(defn build-gradient-datastructure
  []
  (let [base-structure
        (->> gradient-map
             (map-indexed vector)
             (map (fn [[idx [img-name img-path]]]
                    (let [gradient-line (image->gradient-line img-path)]
                      [img-name {:tensor-index idx
                                 :gradient-shape (dtype/shape gradient-line)
                                 :gradient gradient-line}])))
             (into {}))
        full-gradient-tensor-shape [(count base-structure)
                                    (->> (vals base-structure)
                                         (map (comp first :gradient-shape))
                                         (apply max))
                                    3]
        final-img (bufimg/new-image (second full-gradient-tensor-shape)
                                    (first full-gradient-tensor-shape)
                                    :byte-bgr)
        final-tensor (dtt/ensure-tensor final-img)
        _ (doseq [{:keys [tensor-index gradient-shape gradient]}
                  (vals base-structure)]
            (dtype/copy! gradient (dtt/select final-tensor
                                              tensor-index
                                              (range 0 (first gradient-shape))
                                              :all)))
        final-gradient-structure (->> base-structure
                                      (map (fn [[k v]]
                                             [k (dissoc v :gradient)]))
                                      (into {}))]
    (spit "resources/gradients.edn" (pr-str final-gradient-structure))
    (bufimg/save! final-img "PNG" "resources/gradients.png")
    :ok))
