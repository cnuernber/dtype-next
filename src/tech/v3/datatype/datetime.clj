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


(defn ->milliseconds
  "Convert a datatype to either epoch-seconds with an implied zero no time offset"
  [data ]
  (let [datatype (dtype-base/get-datatype data)]
    (when-not (dtype-dt/datetime-datatype? datatype)
      (throw (Exception. (format "Invalid datatype for datetime operation: %s"
                                 datatype))))
    (if (dtype-dt/millis-datatypes datatype)
      (get-milliseconds data)
      (get-epoch-milliseconds data))))


(defn milliseconds->datetime
  "Vectorized Conversion of milliseconds to a given datetime datatype using defaults.
  Specialized conversions for particular datatypes also available as overrides or
  tech.v2.datatype.datetime/milliseconds-since-epoch->X where X can be:
  local-date local-date-time zoned-date-time."
  [datatype milli-data]
  (when-not (dtype-dt/datetime-datatype? datatype)
    (throw (Exception. (format "Datatype is not a datetime datatype: %s" datatype))))

  (let [unpacked-dt (dtype-dt/unpack-datatype datatype)
        packed? (dtype-dt/packed-datatype? datatype)
        conv-fn #(dtype-dt/from-milliseconds % unpacked-dt)
        retval
        (case (arg->arg-type milli-data)
          :scalar (conv-fn milli-data)
          :iterable (unary-op/unary-iterable-map {:datatype unpacked-dt}
                                                 conv-fn milli-data)
          :reader (unary-op/unary-reader-map {:datatype unpacked-dt}
                                             conv-fn milli-data))]
    (if packed?
      (dtype-dt/pack retval)
      retval)))


(defn millisecond-descriptive-stats
  "Get the descriptive stats.  Stats are calulated in milliseconds and
  then min, mean, max are returned as objects of the unpacked datetime
  datatype.  Any other stats values are returned in milliseconds unless
  the input is a duration or packed duration type in which case standard
  deviation is also a duration datatype."
  ([data stats-seq]
   (let [stats-set (set stats-seq)
         datatype (dtype-base/get-datatype data)
         _ (when-not (dtype-dt/datetime-datatype? datatype)
             (throw (Exception. (format "%s is not a datetime datatype"
                                        datatype))))
         numeric-data (->milliseconds data)
         unpacked-datatype (dtype-dt/unpack-datatype datatype)
         value-stats (if (dtype-dt/duration-datatype? datatype)
                       #{:min :max :mean :median :standard-deviation
                         :quartile-1 :quartile-3}
                       #{:min :max :mean :median :quartile-1 :quartile-3})
         stats-data (dfn/descriptive-stats numeric-data stats-set)]
     (->> stats-data
          (map (fn [[k v]]
                 [k
                  (if (value-stats k)
                    (dtype-dt/from-milliseconds v unpacked-datatype)
                    v)]))
          (into {}))))
  ([data]
   (if (dtype-dt/duration-datatype? (dtype-base/get-datatype data))
     (millisecond-descriptive-stats data #{:min :mean :max :standard-deviation
                                           :quartile-1 :quartile-3})
     (millisecond-descriptive-stats data #{:min :mean :max
                                           :quartile-1 :quartile-3}))))
