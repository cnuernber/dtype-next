(ns tech.v3.datatype.datetime.packing
  (:require [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.datetime.base :as dt-base])
  (:import [java.time Instant LocalDate Duration]))



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
                                 (dt-base/local-date->days-since-epoch %)
                                 Integer/MIN_VALUE)
                              (fn [^long value]
                                (if-not (== Integer/MIN_VALUE value)
                                  (dt-base/days-since-epoch->local-date value)
                                  nil)))


(packing/add-packed-datatype! Duration :duration :packed-duration :int64
                              #(if %
                                 (dt-base/duration->nanoseconds %)
                                 Long/MIN_VALUE)
                              (fn [^long data]
                                (if-not (== data Long/MIN_VALUE)
                                  (dt-base/nanoseconds->duration data)
                                  nil)))


(def datatypes #{:packed-instant :packed-local-date :packed-duration})
