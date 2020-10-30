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


(defn- millisecond-reader
  ^LongReader [convert-fn datatype data]
  (let [data (dtype-base/->reader data)]
    (reify LongReader
      (elemwiseDatatype [rdr] datatype)
      (lsize [rdr] (.lsize data))
      (readLong [rdr idx]
        (unchecked-long (convert-fn (.readObject data idx)))))))


(defn- vectorized-dispatch-millisecond-reader
  [convert-fn datatype data]
  (dispatch/vectorized-dispatch-1
   convert-fn nil
   (fn [_dtype data]
     (millisecond-reader convert-fn datatype data))
   data))


(defn- object-reader
  ^ObjectReader [convert-fn datatype data]
  (let [data (dtype-base/->reader data)]
    (reify ObjectReader
      (elemwiseDatatype [rdr] datatype)
      (lsize [rdr] (.lsize data))
      (readObject [rdr idx]
        (convert-fn (.readLong data idx))))))


(defn- vectorized-dispatch-object-reader
  [convert-fn datatype data]
  (dispatch/vectorized-dispatch-1
   convert-fn nil
   (fn [_dtype data]
     (object-reader convert-fn datatype data))
   data))


(def ^:private epoch-conversion-table
  {:epoch-milliseconds {:instant {:->epoch dt-base/instant->milliseconds-since-epoch
                                  :epoch-> dt-base/milliseconds-since-epoch->instant}
                        :zoned-date-time {:->epoch dt-base/zoned-date-time->milliseconds-since-epoch
                                          :epoch-> dt-base/milliseconds-since-epoch->zoned-date-time}
                        :local-date-time {:->epoch dt-base/local-date-time->milliseconds-since-epoch
                                          :epoch-> dt-base/milliseconds-since-epoch->local-date-time}
                        :local-date {:->epoch dt-base/local-date->milliseconds-since-epoch
                                     :epoch-> dt-base/milliseconds-since-epoch->local-date}}
   :epoch-microseconds {:instant {:->epoch dt-base/instant->microseconds-since-epoch
                                  :epoch-> dt-base/microseconds-since-epoch->instant}}
   :epoch-seconds {:instant {:->epoch dt-base/instant->seconds-since-epoch
                             :epoch-> dt-base/seconds-since-epoch->instant}}
   :epoch-days {:instant {:->epoch dt-base/instant->days-since-epoch
                          :epoch-> dt-base/days-since-epoch->instant}
                :local-date {:->epoch dt-base/local-date->days-since-epoch
                             :epoch-> dt-base/days-since-epoch->local-date}}
   :instant {:zoned-date-time {:->epoch dt-base/zoned-date-time->instant
                               :epoch-> dt-base/instant->zoned-date-time}
             :local-date-time {:->epoch dt-base/local-date-time->instant
                               :epoch-> dt-base/instant->local-date-time}
             :local-date {:->epoch dt-base/local-date->instant
                          :epoch-> dt-base/instant->local-date}}})


(defn datetime->epoch
  "Convert datetime data to one of the epoch timestamps.  If timezone is passed in it is used
  else UTC is used.


  * `timezone` a java.time.ZoneId or nil.
  * `epoch-datatype` - one of `#{:epoch-microseconds :epoch-milliseconds :epoch-seconds :epoch-days}`
  * `data` - datetime scalar or vector data."
  ([timezone epoch-datatype data]
   (let [dtype (packing/unpack-datatype (dtype-base/elemwise-datatype data))
         ->epoch-fn (or (get-in epoch-conversion-table [epoch-datatype dtype :->epoch])
                        (when-let [->instant (get-in epoch-conversion-table [:instant dtype :->epoch])]
                          (comp (get-in epoch-conversion-table [epoch-datatype :instant :->epoch])
                                ->instant)))
         _ (errors/when-not-errorf
            ->epoch-fn
            "Failed to find conversion of datatype %s to epoch-datatype %s"
            dtype epoch-datatype)
         ->epoch-fn (if timezone
                      #(->epoch-fn % timezone)
                      ->epoch-fn)]
     (vectorized-dispatch-millisecond-reader ->epoch-fn epoch-datatype data)))
  ([epoch-datatype data]
   (datetime->epoch nil epoch-datatype data))
  ([data]
   (datetime->epoch nil :epoch-milliseconds data)))


(def ^:private epoch-datatype-set (set (keys epoch-conversion-table)))


(defn epoch->datetime
  "Convert data in long or integer epoch data to a datetime datatype.

  * `timezone` - `java.time.ZoneId` or nil.
  * `epoch-datatype` - one of `#{:epoch-microseconds :epoch-milliseconds :epoch-seconds :epoch-days}`.
    In the case where it is not provided the elemwise-datatype of data must one of that set.
  * `datetime-datatype` - The target datatype.
  * `data` - data in integer or long format."
  ([timezone epoch-datatype datetime-datatype data]
   (errors/when-not-errorf
    (epoch-datatype-set epoch-datatype)
    "Epoch datatype (%s) is not an epoch datatype - %s"
    epoch-datatype epoch-datatype-set)
   (let [dtype (packing/unpack-datatype datetime-datatype)
         packed? (not= dtype datetime-datatype)
         epoch->fn (or (get-in epoch-conversion-table [epoch-datatype dtype :epoch->])
                       (when-let [instant-> (get-in epoch-conversion-table [:instant dtype :epoch->])]
                         (let [instant-> (if timezone
                                           #(instant-> % timezone)
                                           instant->)]
                           (comp instant->
                                 (get-in epoch-conversion-table [epoch-datatype :instant :epoch->])))))
         _ (errors/when-not-errorf
            epoch->fn
            "Failed to find conversion of epoch-datatype %s to datatype %s"
            datetime-datatype epoch-datatype)
         result (vectorized-dispatch-object-reader epoch->fn dtype data)]
     (if packed?
       (packing/pack result)
       result)))
  ([timezone datetime-datatype data]
   (epoch->datetime timezone (dtype-base/elemwise-datatype data) datetime-datatype data))
  ([datetime-datatype data]
   (epoch->datetime nil (dtype-base/elemwise-datatype data) datetime-datatype data)))


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
        dt-base/instant->milliseconds-since-epoch
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
                     dt-base/milliseconds-since-epoch->instant
                     :instant milli-data)
           :zoned-date-time (vectorized-dispatch-object-reader
                             dt-base/milliseconds-since-epoch->zoned-date-time
                             :zoned-date-time milli-data)
           :local-date-time (vectorized-dispatch-object-reader
                             #(dt-base/milliseconds-since-epoch->local-date-time % timezone)
                             :local-date-time milli-data)
           :local-date (vectorized-dispatch-object-reader
                        #(dt-base/milliseconds-since-epoch->local-date % timezone)
                        :local-date milli-data)
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


(defn between
  "Find the time unit between two datetime objects.  Must have the same datatype.  Units
  may be a chronounit or one of `#{:milliseconds :seconds :minutes :hours :days :weeks
  :months :years}` Returns longs or long readers.  Note that support for the various
  units across java.time the datatypes partial."
  [lhs rhs units]
  (let [lhs-dtype (packing/unpack-datatype (dtype-base/elemwise-datatype lhs))
        rhs-dtype (packing/unpack-datatype (dtype-base/elemwise-datatype rhs))
        _ (errors/when-not-errorf
           (and (or (= :object lhs-dtype) (datetime-datatype? lhs-dtype))
                (or (= :object rhs-dtype) (datetime-datatype? rhs-dtype)))
           "Datatype (%s) are not datetime datatypes"
           lhs-dtype)
        ^ChronoUnit chrono-unit (if (instance? ChronoUnit units)
                                  units
                                  (get keyword->chrono-unit units))
        _ (errors/when-not-errorf
           chrono-unit
           "Unrecognized chrono unit %s" units)
        convert-fn (fn ^long [^Temporal lhs ^Temporal rhs]
                    (.between chrono-unit lhs rhs))]
    (dispatch/vectorized-dispatch-2
     convert-fn
     #(dispatch/typed-map-2 convert-fn :int64 %2 %3)
     (fn [_ lhs rhs]
       (let [^Buffer lhs (argops/ensure-reader lhs)
             ^Buffer rhs (argops/ensure-reader rhs)]
         (reify LongReader
           (lsize [rdr] (dtype-base/ecount lhs))
           (readObject [rdr idx]
             (convert-fn (.readObject lhs idx)
                         (.readObject rhs idx))))))
     nil
     lhs
     rhs)))
