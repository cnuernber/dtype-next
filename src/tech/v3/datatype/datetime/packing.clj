(ns tech.v3.datatype.datetime.packing
  (:require [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.datetime.base :as dt-base]
            [tech.v3.datatype.protocols :as dtype-proto]
            [primitive-math :as pmath]
            [tech.v3.datatype.base :as dtype-base])
  (:import  [tech.v3.datatype PackedLocalDate]
            [java.time Instant LocalDate Duration]))



(packing/add-packed-datatype! Instant :instant :packed-instant :int64
                              #(if %
                                (dt-base/instant->microseconds-since-epoch %)
                                Long/MIN_VALUE)
                              (fn [^long data]
                                (if-not (== data Long/MIN_VALUE)
                                  (dt-base/microseconds-since-epoch->instant data)
                                  nil)))


(packing/add-packed-datatype! LocalDate :local-date :packed-local-date :int32
                              #(if %
                                 (PackedLocalDate/pack %)
                                 0)
                              (fn [^long value]
                                (if-not (== 0 value)
                                  (PackedLocalDate/asLocalDate (pmath/int value))
                                  nil)))


(packing/add-packed-datatype! Duration :duration :packed-duration :int64
                              #(if %
                                 (dt-base/duration->nanoseconds %)
                                 Long/MIN_VALUE)
                              (fn [^long data]
                                (if-not (== data Long/MIN_VALUE)
                                  (dt-base/nanoseconds->duration data)
                                  nil)))
