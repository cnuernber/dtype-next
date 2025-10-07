(ns tech.v3.datatype.hamf-proto
  "Uses hamf's defprotocol system for particularly performance sensitive operations."
  (:require [ham-fisted.defprotocol :refer [defprotocol extend extend-type extend-protocol]]
            [tech.v3.datatype.protocols :as dtype-proto])
  (:import [tech.v3.datatype ElemwiseDatatype ECount Buffer BinaryBuffer ObjectReader]
           [clojure.lang Counted Keyword]
           [ham_fisted IMutList]
           [java.util Collection Map])
  (:refer-clojure :exclude [defprotocol extend extend-type extend-protocol]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defprotocol PElemwiseDatatype
  (elemwise-datatype [item]))

(extend-protocol PElemwiseDatatype
  ElemwiseDatatype
  (elemwise-datatype [item] (.elemwiseDatatype item)))

(clojure.core/extend-type Object
  dtype-proto/POperationalElemwiseDatatype
  (operational-elemwise-datatype [item]
    (elemwise-datatype item)))

(defprotocol PECount
  (^long ecount [item]))

(defprotocol PDatatype
  (datatype [item]))

(extend nil
  PECount {:ecount 0}
  PElemwiseDatatype {:elemwise-datatype :object}
  PDatatype {:datatype :object})

(extend Object
  PECount {:ecount (fn ^long [v] (long (dtype-proto/ecount v)))}
  PElemwiseDatatype {:elemwise-datatype #(dtype-proto/elemwise-datatype %)}
  PDatatype {:datatype #(dtype-proto/datatype %)})

(extend String
  PDatatype {:datatype :string}
  PElemwiseDatatype {:elemwise-datatype :char})
(extend Keyword PDatatype {:datatype :keyword})

(extend-protocol PECount
  Counted (ecount [c] (.count c))
  ECount (ecount [c] (.lsize c))
  Collection (ecount [c] (.size c))
  Map (ecount [c] (.size c))
  String (ecount [c] (.length c)))
