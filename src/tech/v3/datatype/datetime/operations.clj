(ns tech.v3.datatype.datetime.operations
  (:require [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.datetime.base :as dt-base]
            [tech.v3.datatype.functional :as dfn])
  (:import [tech.v3.datatype LongReader ObjectReader]))


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
   data convert-fn nil
   (fn [data _dtype]
     (millisecond-reader convert-fn datatype data))))


(defn ->milliseconds
  "Convert a datatype to either epoch-seconds with a timezone and implied zero time offset"
  ([timezone data]
   (let [data (packing/unpack data)
         datatype (dtype-base/elemwise-datatype data)
         _ (when-not (dtype-dt/datetime-datatype? datatype)
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
   (->milliseconds (dt-base/utc-zone-id) data)))


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
   data convert-fn nil
   (fn [data _dtype]
     (object-reader convert-fn datatype data))))


(defn milliseconds->datetime
  "Vectorized Conversion of milliseconds to a given datetime datatype using defaults.
  Specialized conversions for particular datatypes also available as overrides or
  tech.v2.datatype.datetime/milliseconds-since-epoch->X where X can be:
  local-date local-date-time zoned-date-time."
  ([datatype timezone milli-data]
   (when-not (dtype-dt/datetime-datatype? datatype)
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
         _ (when-not (dtype-dt/datetime-datatype? datatype)
             (throw (Exception. (format "%s is not a datetime datatype"
                                        datatype))))
         numeric-data (->milliseconds data)
         value-stats (if (dtype-dt/duration-datatype? datatype)
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
   (if (dtype-dt/duration-datatype? (dtype-base/elemwise-datatype data))
     (millisecond-descriptive-statistics data #{:min :mean :max :standard-deviation
                                                :quartile-1 :quartile-3})
     (millisecond-descriptive-statistics data #{:min :mean :max
                                                :quartile-1 :quartile-3}))))
