(ns tech.v3.datatype.unary-pred
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.double-ops :as double-ops])
  (:import [tech.v3.datatype UnaryPredicate
            UnaryPredicates$BooleanUnaryPredicate
            UnaryPredicates$DoubleUnaryPredicate
            UnaryPredicates$ObjectUnaryPredicate]))
