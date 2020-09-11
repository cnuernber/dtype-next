(ns tech.v3.datatype.datetime
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime.packing :as datetime-packing]
            [tech.v3.datatype.datetime.base :as datetime-base]
            [clojure.set :as set])
  (:import [java.time LocalDate LocalDateTime
            ZonedDateTime Instant ZoneId Duration ZoneOffset
            OffsetDateTime LocalTime]
           [java.time.temporal ChronoUnit Temporal ChronoField
            WeekFields TemporalAmount]))



(def datetime-datatypes (set/union datetime-packing/datatypes
                                   datetime-base/datatypes))


(defn datetime-datatype?
  [datatype]
  (boolean (datetime-datatypes datatype)))


(def millisecond-datatypes #{:duration :packed-duration})

(defn duration-datatype?
  [dtype]
  (boolean (millisecond-datatypes dtype)))
