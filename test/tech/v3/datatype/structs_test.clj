(ns tech.v3.datatype.structs-test
  (:require [clojure.spec.alpha :as s]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.struct :as dt-struct]
            ;; [clojure.spec.gen.alpha :as gen]
            [tech.v3.datatype.casting :as dt-casting]
            [clojure.test.check :as tc]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer [deftest is]]))


(s/def :field/name simple-keyword?)
(s/def :field/datatype
  tech.v3.datatype.casting/numeric-types)
(s/def ::struct-field
  (s/keys :req-un [:field/name
                   :field/datatype]))

(s/def :struct/fields (s/+ ::struct-field))
(s/def :struct/name simple-keyword?)
(s/def ::struct-definition
  (s/keys :req-un [:struct/fields
                   :struct/name]))

(s/def ::num-structs (s/and pos-int?
                            #(< % 1e6)))

(def struct-datatypes @#'dt-struct/struct-datatypes)

(def new-define-type-property
  (prop/for-all
   [struct-def (s/gen ::struct-definition)]
   (dt-struct/define-datatype!
     (:name struct-def)
     (:fields struct-def))
   (.clear struct-datatypes)
   true))




(def new-array-of-structs-property
  (prop/for-all
   [[num-elements struct-def] (s/gen
                               (s/tuple
                                ::num-structs
                                ::struct-definition))]
   (dt-struct/define-datatype!
     (:name struct-def)
     (:fields struct-def))
   (dt-struct/new-array-of-structs (:name struct-def) num-elements)
   (.clear struct-datatypes)
   true))

#_(s/def ::struct-array
  (s/with-gen any?
    (fn []
      (gen/fmap )))
  )

(def fill-array-of-structs-property
  (prop/for-all
   [[num-elements struct-def] (s/gen
                               (s/tuple
                                ::num-structs
                                ::struct-definition))]
   (dt-struct/define-datatype!
     (:name struct-def)
     (:fields struct-def))
   (let [arr (dt-struct/new-array-of-structs (:name struct-def) num-elements)]
     (doseq [struct arr
             field (:fields struct-def)]
       (.put struct (:name field) 0)))
   (.clear struct-datatypes)
   true))

(def fill-array-of-structs-nth-property
  (prop/for-all
   [[num-elements struct-def] (s/gen
                               (s/tuple
                                ::num-structs
                                ::struct-definition))]
   (dt-struct/define-datatype!
     (:name struct-def)
     (:fields struct-def))
   (let [arr (dt-struct/new-array-of-structs (:name struct-def) num-elements)]
     (dotimes [i num-elements]
       (let [struct (nth arr i)]
         (doseq [field (:fields struct-def)]
           (.put struct (:name field) 0)))))
   (.clear struct-datatypes)
   true))


(deftest spot-test-set-nth
  (let [dt (dt-struct/define-datatype!
             :my-struct
             [{:name :my-field, :datatype :int32}])
        my-arr (dt-struct/new-array-of-structs :my-struct 2)
        ^java.util.Map my-struct (nth my-arr 1)]
    (.put my-struct :my-field 42)
    (is (= (.get my-struct :my-field) 42))))



(comment
  (tc/quick-check 20 new-define-type-property)
  (tc/quick-check 20 new-array-of-structs-property)
  (tc/quick-check 20 fill-array-of-structs-property)
  (tc/quick-check 20 fill-array-of-structs-nth-property)
  )
