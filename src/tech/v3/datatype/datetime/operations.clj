(ns tech.v3.datatype.datetime.operations
  (:require [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy-make-container :as copy-cmc]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.datetime.packing :as dt-packing]
            [tech.v3.datatype.datetime.base :as dt-base]
            [tech.v3.datatype.rolling :as dt-rolling]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.binary-op :as bin-op]
            [tech.v3.datatype.op-dispatch :as op-dispatch]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.errors :as errors]
            [ham-fisted.api :as hamf]
            [clojure.set :as set])
  (:import [tech.v3.datatype DoubleReader LongReader ObjectReader Buffer
            BinaryOperator
            BinaryOperators$LongBinaryOperator]
           [java.time.temporal ChronoUnit Temporal ChronoField
            WeekFields TemporalAmount TemporalField
            TemporalAccessor]
           [java.util Locale Iterator Set]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(def datetime-datatypes (set/union dt-packing/datatypes
                                   dt-base/datatypes))

(def ^Set datetime-datatypes-java (hamf/java-hashset datetime-datatypes))


(defn datetime-datatype?
  [datatype]
  (.contains datetime-datatypes-java datatype))


(def millisecond-datatypes #{:duration :packed-duration})

(defn duration-datatype?
  [dtype]
  (boolean (millisecond-datatypes dtype)))



(defn- millisecond-reader
  ;; Because we are converting from object data to a numeric representation we have to account
  ;; for the fact that objects may be nil (in the case of missing data) which leads us to
  ;; have to use doubles even though milliseconds are long quantities throughout the system.
  ^DoubleReader [convert-fn datatype data]
  (let [data (dtype-base/->reader data)]
    (reify DoubleReader
      (elemwiseDatatype [rdr] datatype)
      (lsize [rdr] (.lsize data))
      (readDouble [rdr idx]
        (if-let [obj-data (.readObject data idx)]
          (double (convert-fn obj-data))
          Double/NaN)))))


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
         stats-data (dfn/descriptive-statistics numeric-data stats-set options)]
     (->> stats-data
          (map (fn [[k v]]
                 [k
                  (if (value-stats k)
                    (milliseconds->datetime datatype v)
                    v)]))
          (into {}))))
  ([data]
   (if (duration-datatype? (dtype-base/elemwise-datatype data))
     (millisecond-descriptive-statistics #{:min :mean :max :standard-deviation
                                           :quartile-1 :quartile-3}
                                         {} data)
     (millisecond-descriptive-statistics #{:min :mean :max
                                           :quartile-1 :quartile-3}
                                         {} data))))


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
   :milliseconds ChronoUnit/MILLIS
   :microseconds ChronoUnit/MICROS})


(defn- get-chrono-unit
  [kwd]
  (if-let [retval (keyword->chrono-unit kwd)]
    retval
    (errors/throwf "Unrecognized chrono unit kwd %s" kwd)))


(defn- ensure-temporal-amount
  ^TemporalAmount [tf]
  (cond
    (keyword? tf)
    (if-let [retval (get-chrono-unit tf)]
      retval
      (errors/throwf "Unrecognized temporal unit %s" tf))
    (instance? TemporalAmount tf)
    tf
    :else
    (errors/throwf "Unable to convert tf (%s) to a temporal amount"
                   (type tf))))

(defn- temporal-dispatch
  [op-space convert-fn lhs rhs]
  (op-dispatch/dispatch-binary-op (bin-op/->binary-operator convert-fn) op-space op-space lhs rhs))

(defn plus-temporal-operator
  "Returns an binary that expects the temporal quantity as
  the lhs and the amount as the rhs.

  Returns the space of the operation and boolean operator that can perform the
  operation."
  [dtype tf]
  (let [dtype (packing/unpack-datatype dtype)]
    (case (dt-base/classify-datatype dtype)
      :temporal (let [tf (ensure-temporal-amount tf)]
                  (reify BinaryOperator
                    (binaryObject [this lhs rhs]
                      (when lhs
                        (.plus ^Temporal lhs (unchecked-long rhs) tf)))))
      :epoch (let [epoch-conv (long (dt-base/epoch->microseconds dtype))
                   tf-conv (long (dt-base/relative->microseconds tf))]
               (reify BinaryOperators$LongBinaryOperator
                 (binaryLong [this lhs rhs]
                   ;;perform operation in microsecond space and round back.
                   (quot (+ (* lhs epoch-conv)
                            (* rhs tf-conv))
                         epoch-conv))))
      :relative (let [lhs-conv (long (dt-base/epoch->microseconds dtype))
                      rhs-conv (long (dt-base/relative->microseconds tf))]
                  (reify BinaryOperators$LongBinaryOperator
                    (binaryLong [this lhs rhs]
                      ;;perform operation in microsecond space and round back.
                      (quot (+ (* lhs lhs-conv)
                               (* rhs rhs-conv))
                            lhs-conv)))))))


(defn plus-temporal-amount
  "Add a given temporal amount (in integers) to a temporal dataset.
  Valid temporal amounts are:
  #{:months :days :seconds :hours :years :milliseconds :minutes :weeks}"
  ([datetime-data long-data tf]
   (let [res-dtype (packing/unpack-datatype
                    (dtype-base/elemwise-datatype datetime-data))
         binop (plus-temporal-operator
                res-dtype
                tf)]
     (temporal-dispatch res-dtype binop datetime-data long-data)))
  ([datetime-data long-data]
   (plus-temporal-amount datetime-data long-data (dtype-base/elemwise-datatype
                                                  long-data))))


(defn minus-temporal-operator
  "Returns an binary that expects the temporal quantity as
  the lhs and the amount as the rhs.

  Returns the space of the operation and boolean operator that can perform the
  operation."
  [dtype tf]
  (let [dtype (packing/unpack-datatype dtype)]
    (case (dt-base/classify-datatype dtype)
      :temporal (let [tf (ensure-temporal-amount tf)]
                  (reify BinaryOperator
                    (binaryObject [this lhs rhs]
                      (when lhs
                        (.minus ^Temporal lhs (unchecked-long rhs) tf)))))
      :epoch (let [epoch-conv (long (dt-base/epoch->microseconds dtype))
                   tf-conv (long (dt-base/relative->microseconds tf))]
               (reify BinaryOperators$LongBinaryOperator
                 (binaryLong [this lhs rhs]
                   ;;perform operation in microsecond space and round back.
                   (quot (- (* lhs epoch-conv)
                            (* rhs tf-conv))
                         epoch-conv))))
      :relative (let [lhs-conv (long (dt-base/epoch->microseconds dtype))
                      rhs-conv (long (dt-base/relative->microseconds tf))]
                  (reify BinaryOperators$LongBinaryOperator
                    (binaryLong [this lhs rhs]
                      ;;perform operation in microsecond space and round back.
                      (quot (- (* lhs lhs-conv)
                               (* rhs rhs-conv))
                            lhs-conv)))))))


(defn minus-temporal-amount
  "Subtract a given temporal amount (in integers) to a temporal dataset.
  Valid temporal amounts are:
  #{:months :days :seconds :hours :years :milliseconds :minutes :weeks}"
  ([datetime-data long-data tf]
   (let [res-dtype (packing/unpack-datatype
                    (dtype-base/elemwise-datatype datetime-data))
         binop (minus-temporal-operator
                res-dtype
                tf)]
     (temporal-dispatch res-dtype binop datetime-data long-data)))
  ([datetime-data long-data]
   (minus-temporal-amount datetime-data long-data (dtype-base/elemwise-datatype
                                                   long-data))))


(defn between-op
  "between always results in a long or long object.

  * src-dt-type - Source datatype.
  * units - Units to do comparison in.
  * reverse? Reverse makes the comparison work like a normal subtraction
    e.g. later dates on the left with earlier dates on the right return
    positive integers.  This is opposite the java.time api in general."
  ([src-dt-type units reverse?]
   (let [dtype (packing/unpack-datatype src-dt-type)]
     (case (dt-base/classify-datatype dtype)
       :temporal (let [^ChronoUnit chrono-unit (if (instance? ChronoUnit units)
                                                 units
                                                 (get-chrono-unit units))]
                   (if reverse?
                     (reify BinaryOperator
                       (binaryObject [this rhs lhs]
                         (.between chrono-unit ^Temporal lhs ^Temporal rhs)))
                     (reify BinaryOperator
                       (binaryObject [this lhs rhs]
                         (.between chrono-unit ^Temporal lhs ^Temporal rhs)))))
       :epoch (let [epoch-conv (long (dt-base/epoch->microseconds src-dt-type))
                    unit-conv (long (dt-base/relative->microseconds units))]
                (if reverse?
                  (reify BinaryOperators$LongBinaryOperator
                    (binaryLong [this rhs lhs]
                      (let [rel (* (- rhs lhs) epoch-conv)]
                        (quot rel unit-conv))))
                  (reify BinaryOperators$LongBinaryOperator
                    (binaryLong [this lhs rhs]
                      (let [rel (* (- rhs lhs) epoch-conv)]
                        (quot rel unit-conv))))))
       :relative (let [src-conv (long (dt-base/relative->microseconds src-dt-type))
                       unit-conv (long (dt-base/relative->microseconds units))]
                   (if reverse?
                     (reify BinaryOperators$LongBinaryOperator
                       (binaryLong [this rhs lhs]
                         (let [rel (* (- rhs lhs) src-conv)]
                           (quot rel unit-conv))))
                     (reify BinaryOperators$LongBinaryOperator
                       (binaryLong [this lhs rhs]
                         (let [rel (* (- rhs lhs) src-conv)]
                           (quot rel unit-conv)))))))))
  ([src-dt-type units]
   (between-op src-dt-type units false)))


(defn between
  "Find the time unit between two datetime objects.  Must have the same datatype.
  Units may be a chronounit or one of `#{:milliseconds :seconds :minutes :hours :days
  :weeks :months :years}` Returns longs or long readers.  Note that support for the
  various units across java.time the datatypes is partial."
  [lhs rhs units]
  (let [lhs-dtype (packing/unpack-datatype (dtype-base/elemwise-datatype lhs))
        rhs-dtype (packing/unpack-datatype (dtype-base/elemwise-datatype rhs))
        _ (errors/when-not-errorf
           (identical? lhs-dtype rhs-dtype)
           "lhs,rhs must have identical datatypes.")
        ^BinaryOperator convert-op (between-op lhs-dtype units)]
    (dispatch/vectorized-dispatch-2
     convert-op
     #(dispatch/typed-map-2 convert-op :int64 %2 %3)
     (fn [_ lhs rhs]
       (let [^Buffer lhs (copy-cmc/ensure-reader lhs)
             ^Buffer rhs (copy-cmc/ensure-reader rhs)]
         (if (= :object (casting/flatten-datatype lhs-dtype))
           (reify LongReader
             (elemwiseDatatype [rdr] units)
             (lsize [rdr] (dtype-base/ecount lhs))
             (readLong [rdr idx]
               (long
                (.binaryObject convert-op
                               (.readObject lhs idx)
                               (.readObject rhs idx)))))
           (reify LongReader
             (elemwiseDatatype [rdr] units)
             (lsize [rdr] (dtype-base/ecount lhs))
             (readLong [rdr idx]
               (.binaryLong convert-op
                            (.readLong lhs idx)
                            (.readLong rhs idx)))))))
     nil
     lhs
     rhs)))


(deftype ObjectRollingIterator [^{:unsynchronized-mutable true
                                  :tag long} start-idx
                                ^{:unsynchronized-mutable true
                                  :tag long} end-idx
                                ^long window-length
                                ^long n-elems
                                ^long n-windows
                                ^BinaryOperator tweener
                                ^Buffer data]
  Iterator
  (hasNext [_this] (not= start-idx n-windows))
  (next [_this]
    (let [start-val (data start-idx)
          next-end-idx
          (long
           (loop [eidx end-idx]
             (if (or (>= eidx n-elems)
                     (>= (long (tweener start-val (data eidx))) window-length))
               eidx
               (recur (unchecked-inc eidx)))))
          retval (range start-idx next-end-idx)]
      (set! start-idx (unchecked-inc start-idx))
      (set! end-idx next-end-idx)
      retval)))


(defn variable-rolling-window-ranges
  "Given a reader of monotonically increasing source datetime data, return an
  iterable of ranges that describe the windows in index space.  There will be one
  window per source input and windows are applied in int64 microsecond space.  Be
  aware that windows near the end cannot possibly satisfy the windowing requirement.

  See options for tech.v3.datatype.rolling/variable-rolling-window-ranges for further
  options, of which stepsize may be of interest."
  ([src-data window-length units options]
   (let [^BinaryOperator tweener (between-op (dtype-base/elemwise-datatype src-data)
                                             units
                                             true)]
     (dt-rolling/variable-rolling-window-ranges
      src-data window-length (merge {:comp-fn tweener} options))))
  ([src-data window-length units]
   (variable-rolling-window-ranges src-data window-length units nil)))
