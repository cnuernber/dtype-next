(ns tech.v3.datatype.datetime.operations
  (:require [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.datetime.packing :as dt-packing]
            [tech.v3.datatype.datetime.base :as dt-base]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.errors :as errors]
            [clojure.set :as set])
  (:import [tech.v3.datatype LongReader ObjectReader Buffer]
           [java.time.temporal ChronoUnit Temporal ChronoField
            WeekFields TemporalAmount TemporalField
            TemporalAccessor]
           [java.util Locale]))


(def datetime-datatypes (set/union dt-packing/datatypes
                                   dt-base/datatypes))


(defn datetime-datatype?
  [datatype]
  (boolean (datetime-datatypes datatype)))


(def millisecond-datatypes #{:duration :packed-duration})

(defn duration-datatype?
  [dtype]
  (boolean (millisecond-datatypes dtype)))


(defn millisecond-reader
  ^LongReader [convert-fn datatype data]
  (let [data (dtype-base/->reader data)]
    (reify LongReader
      (elemwiseDatatype [rdr] datatype)
      (lsize [rdr] (.lsize data))
      (readLong [rdr idx]
        (unchecked-long (convert-fn (.readObject data idx)))))))


(defn vectorized-dispatch-millisecond-reader
  [convert-fn datatype data]
  (dispatch/vectorized-dispatch-1
   convert-fn nil
   (fn [_dtype data]
     (millisecond-reader convert-fn datatype data))
   data))


(defn datetime->milliseconds
  "Vectorized conversion of a datetime datatype to milliseconds with a given timezone (or utc timezone)
  and an implied 0 time offset."
  ([timezone data]
   (let [data (packing/unpack data)
         datatype (dtype-base/elemwise-datatype data)
         _ (when-not (datetime-datatype? datatype)
             (throw (Exception. (format "Invalid datatype for datetime operation: %s"
                                        datatype))))]
     (case datatype
       :instant
       (vectorized-dispatch-millisecond-reader
        dt-base/instant->microseconds-since-epoch
        :epoch-milliseconds data)

       :zoned-date-time
       (vectorized-dispatch-millisecond-reader
        dt-base/zoned-date-time->milliseconds-since-epoch
        :epoch-milliseconds data)

       :local-date-time
       (vectorized-dispatch-millisecond-reader
        #(dt-base/local-date-time->milliseconds-since-epoch % timezone)
        :epoch-milliseconds data)

       :local-date
       (vectorized-dispatch-millisecond-reader
        #(dt-base/local-date->milliseconds-since-epoch % 0 timezone)
        :epoch-milliseconds data)

       :duration
       (vectorized-dispatch-millisecond-reader
        dt-base/duration->milliseconds
        :milliseconds data))))
  ([data]
   (datetime->milliseconds (dt-base/utc-zone-id) data)))


(defn object-reader
  ^ObjectReader [convert-fn datatype data]
  (let [data (dtype-base/->reader data)]
    (reify ObjectReader
      (elemwiseDatatype [rdr] datatype)
      (lsize [rdr] (.lsize data))
      (readObject [rdr idx]
        (convert-fn (.readLong data idx))))))


(defn vectorized-dispatch-object-reader
  [convert-fn datatype data]
  (dispatch/vectorized-dispatch-1
   convert-fn nil
   (fn [_dtype data]
     (object-reader convert-fn datatype data))
   data))


(defn milliseconds->datetime
  "Vectorized Conversion of milliseconds to a given datetime datatype using defaults.
  Specialized conversions for particular datatypes also available as overrides or
  tech.v2.datatype.datetime/milliseconds-since-epoch->X where X can be:
  local-date local-date-time zoned-date-time."
  ([datatype timezone milli-data]
   (when-not (datetime-datatype? datatype)
     (throw (Exception. (format "Datatype is not a datetime datatype: %s" datatype))))
   (let [packed? (packing/packed-datatype? datatype)
         datatype (packing/unpack-datatype datatype)
         retval
         (case datatype
           :instant (vectorized-dispatch-object-reader
                     dt-base/microseconds-since-epoch->instant
                     :instant milli-data)
           :zoned-date-time (vectorized-dispatch-object-reader
                             dt-base/milliseconds-since-epoch->zoned-date-time
                             :zoned-date-time milli-data)
           :local-date-time (vectorized-dispatch-object-reader
                             #(dt-base/milliseconds-since-epoch->local-date-time % timezone)
                             :local-date-time milli-data)
           :local-date (vectorized-dispatch-object-reader
                        #(dt-base/milliseconds-since-epoch->local-date % timezone)
                        :local-date-time milli-data)
           :duration (vectorized-dispatch-object-reader
                      dt-base/milliseconds->duration
                      :duration milli-data))]
     (if packed?
       (packing/pack retval)
       retval)))
  ([datatype milli-data]
   (milliseconds->datetime datatype (dt-base/utc-zone-id) milli-data)))


(defn millisecond-descriptive-statistics
  "Get the descriptive stats.  Stats are calulated in milliseconds and
  then min, mean, max are returned as objects of the unpacked datetime
  datatype.  Any other stats values are returned in milliseconds unless
  the input is a duration or packed duration type in which case standard
  deviation is also a duration datatype."
  ([stats-seq options data]
   (let [stats-set (set stats-seq)
         data (packing/unpack data)
         datatype (dtype-base/elemwise-datatype data)
         _ (when-not (datetime-datatype? datatype)
             (throw (Exception. (format "%s is not a datetime datatype"
                                        datatype))))
         numeric-data (datetime->milliseconds data)
         value-stats (if (duration-datatype? datatype)
                       #{:min :max :mean :median :standard-deviation
                         :quartile-1 :quartile-3}
                       #{:min :max :mean :median :quartile-1 :quartile-3})
         stats-data (dfn/descriptive-statistics stats-set (assoc options :nan-strategy :keep)
                                                numeric-data)]
     (->> stats-data
          (map (fn [[k v]]
                 [k
                  (if (value-stats k)
                    (milliseconds->datetime datatype v)
                    v)]))
          (into {}))))
  ([data]
   (if (duration-datatype? (dtype-base/elemwise-datatype data))
     (millisecond-descriptive-statistics data #{:min :mean :max :standard-deviation
                                                :quartile-1 :quartile-3})
     (millisecond-descriptive-statistics data #{:min :mean :max
                                                :quartile-1 :quartile-3}))))


(def ^{:doc "Map of keyword to temporal field"}
  keyword->temporal-field
  {:years ChronoField/YEAR
   :months ChronoField/MONTH_OF_YEAR
   :days ChronoField/DAY_OF_MONTH
   :day-of-year ChronoField/DAY_OF_YEAR
   :day-of-week ChronoField/DAY_OF_WEEK
   ;;Locale sensitive
   :week-of-year (.. (WeekFields/of (Locale/getDefault)) weekOfWeekBasedYear)
   :iso-week-of-year (.. (WeekFields/ISO) weekOfWeekBasedYear)
   :iso-day-of-week (.. (WeekFields/ISO) dayOfWeek)
   :epoch-days ChronoField/EPOCH_DAY
   :hours ChronoField/HOUR_OF_DAY
   :minutes ChronoField/MINUTE_OF_HOUR
   :seconds ChronoField/SECOND_OF_MINUTE
   ;;This isn't as useful because logical things (like instances) don't support
   ;;this field but do support epoch milliseconds.
   ;; :epoch-seconds ChronoField/INSTANT_SECONDS
   :milliseconds ChronoField/MILLI_OF_SECOND})


(defn long-temporal-field
  "Given a temporal field (or a keyword representing desired field)
  return an :int64 thing that represents that value of this temporal.
  Temporal fields are:
   #{:iso-day-of-week :iso-week-of-year :day-of-week :months :days :seconds
  :epoch-days :day-of-year :hours :years :milliseconds :minutes :week-of-year}.

  Not all temporal objects support all temporal fields.
  Also, if you are looking to convert a thing to seconds-since-epochs please
  see datetime->milliseconds."
  [tf data]
  (let [^TemporalField tf
        (cond
          (keyword? tf)
          (if-let [retval (get keyword->temporal-field tf)]
            retval
            (errors/throwf "Unrecognized temporal field %s" tf))
          (instance? TemporalField tf)
          tf
          :else
          (errors/throwf "Unable to convert tf (%s) to a temporal field"
                         (type tf)))
        data (packing/unpack data)
        dtype (dtype-base/elemwise-datatype data)
        _ (errors/when-not-errorf
           (or (= :object dtype)
               (datetime-datatype? dtype))
           "Data datatype (%s) is not a date time datatype."
           (dtype-base/elemwise-datatype data))
        convert-fn (fn [^TemporalAccessor arg]
                     (.getLong arg tf))]
    (dispatch/vectorized-dispatch-1
     convert-fn
     (fn [_ iter]
       (dispatch/typed-map-1 convert-fn :int64 iter))
     (fn [_ data]
       (let [^Buffer data (dtype-base/->reader data)]
         (reify LongReader
           (lsize [rdr] (dtype-base/ecount data))
           (readLong [rdr idx]
             (if-let [data (.readObject data idx)]
               (unchecked-long (convert-fn data))
               Long/MIN_VALUE)))))
     data)))

(def keyword->chrono-unit
  {:years ChronoUnit/YEARS
   :months ChronoUnit/MONTHS
   :weeks ChronoUnit/WEEKS
   :days ChronoUnit/DAYS
   :hours ChronoUnit/HOURS
   :minutes ChronoUnit/MINUTES
   :seconds ChronoUnit/SECONDS
   :milliseconds ChronoUnit/MILLIS})


(defn- ensure-temporal-amount
  ^TemporalAmount [tf]
  (cond
    (keyword? tf)
    (if-let [retval (get keyword->chrono-unit tf)]
      retval
      (errors/throwf "Unrecognized temporal unit %s" tf))
    (instance? TemporalAmount tf)
    tf
    :else
    (errors/throwf "Unable to convert tf (%s) to a temporal amount"
                   (type tf))))



(defn- temporal-dispatch
  [convert-fn lhs rhs]
  (let [lhs (packing/unpack lhs)
        dtype (dtype-base/elemwise-datatype lhs)
        _ (errors/when-not-errorf
           (or (= :object dtype)
               (datetime-datatype? dtype))
           "Data datatype (%s) is not a date time datatype."
           (dtype-base/elemwise-datatype lhs))]
    (dispatch/vectorized-dispatch-2
     convert-fn
     #(dispatch/typed-map-2 convert-fn dtype %2 %3)
     (fn [_ lhs rhs]
       (let [^Buffer lhs (argops/ensure-reader lhs)
             ^Buffer rhs (argops/ensure-reader rhs)]
         (reify ObjectReader
           (elemwiseDatatype [rdr] dtype)
           (lsize [rdr] (dtype-base/ecount lhs))
           (readObject [rdr idx]
             (if-let [data (.readObject lhs idx)]
               (convert-fn data (.readLong rhs idx))
               nil)))))
     nil
     lhs
     rhs)))


(defn plus-temporal-amount
  "Add a given temporal amount (in integers) to a temporal dataset.
  Valid temporal amounts are:
  #{:months :days :seconds :hours :years :milliseconds :minutes :weeks}"
  [datetime-data long-data tf]
  (let [tf (ensure-temporal-amount tf)
        convert-fn (fn [^Temporal arg ^long amt]
                     (.plus arg amt tf))]
    (temporal-dispatch convert-fn datetime-data long-data)))


(defn minus-temporal-amount
  "Subtract a given temporal amount (in integers) to a temporal dataset.
  Valid temporal amounts are:
  #{:months :days :seconds :hours :years :milliseconds :minutes :weeks}"
  [datetime-data long-data tf]
  (let [tf (ensure-temporal-amount tf)
        convert-fn (fn [^Temporal arg ^long amt]
                     (.minus arg amt tf))]
    (temporal-dispatch convert-fn datetime-data long-data)))
