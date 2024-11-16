(ns tech.v3.datatype.datetime.packing
  (:require [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.datetime.base :as dt-base]
            [tech.v3.datatype.datetime.constants :as constants])
  (:import [java.time Instant LocalDate Duration LocalTime]))

(set! *warn-on-reflection* true)

(packing/add-packed-datatype! Instant :instant :packed-instant :int64
                              #(if %
                                (dt-base/instant->microseconds-since-epoch %)
                                Long/MIN_VALUE)
                              (fn [^long data]
                                (when-not (== data Long/MIN_VALUE)
                                  (dt-base/microseconds-since-epoch->instant data))))

(packing/add-packed-datatype! Instant :instant :packed-milli-instant :int64
                              #(if %
                                (dt-base/instant->milliseconds-since-epoch %)
                                Long/MIN_VALUE)
                              (fn [^long data]
                                (when-not (== data Long/MIN_VALUE)
                                  (dt-base/milliseconds-since-epoch->instant data))))


(packing/add-packed-datatype! LocalDate :local-date :packed-local-date :int32
                              #(if %
                                 (dt-base/local-date->days-since-epoch %)
                                 Integer/MIN_VALUE)
                              (fn [^long value]
                                (when-not (== Integer/MIN_VALUE value)
                                  (dt-base/days-since-epoch->local-date value))))


(packing/add-packed-datatype! Duration :duration :packed-duration :int64
                              #(if %
                                 (dt-base/duration->nanoseconds %)
                                 Long/MIN_VALUE)
                              (fn [^long data]
                                (when-not (== data Long/MIN_VALUE)
                                  (dt-base/nanoseconds->duration data))))


(packing/add-packed-datatype! LocalTime :local-time :packed-local-time :int64
                              #(if %
                                 (-> (.toNanoOfDay ^LocalTime %)
                                     (quot constants/nanoseconds-in-microsecond))
                                 Long/MIN_VALUE)
                              (fn [^long data]
                                (when-not (== data Long/MIN_VALUE)
                                  (LocalTime/ofNanoOfDay
                                   (* data constants/nanoseconds-in-microsecond)))))


(def datatypes #{:packed-instant :packed-milli-instant :packed-local-date :packed-duration :packed-local-time})
