(ns tech.v3.datatype.datetime.packing
  (:require [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base])
  (:import  [tech.v3.datatype PackedLocalDate]
            [java.time Instant]))


(packing/add-packed-datatype! Instant :instant :packed-instant :int64
                              dtype-dt/instant->microseconds-since-epoch
                              dtype-dt/microseconds-since-epoch->instant)
