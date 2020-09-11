(ns tech.v3.datatype.datetime.base
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime.constants :refer [nanoseconds-in-millisecond]])
  (:import [java.time LocalDate LocalDateTime
            ZonedDateTime Instant ZoneId Duration ZoneOffset
            OffsetDateTime LocalTime]
           [java.time.temporal ChronoUnit Temporal ChronoField
            WeekFields TemporalAmount]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare local-date-time
         local-date-time->instant
         local-date-time->local-date
         local-time->local-date-time)



(defn system-zone-id
  ^ZoneId []
  (ZoneId/systemDefault))


(defn utc-zone-id
  ^ZoneId []
  (ZoneId/of "UTC"))


(defn system-zone-offset
  ^ZoneOffset []
  (.. (OffsetDateTime/now) getOffset))


(defn utc-zone-offset
  ^ZoneOffset []
  ZoneOffset/UTC)


(defn milliseconds-since-epoch->instant
  ^Instant [^long arg]
  (Instant/ofEpochMilli arg))


(defn seconds-since-epoch->instant
  ^Instant [seconds]
  (milliseconds-since-epoch->instant (* (long seconds) 1000)))


(defn instant
  (^Instant []
   (Instant/now))
  (^Instant [arg]
   (if (instance? Instant arg)
     arg
     (milliseconds-since-epoch->instant arg))))


(defn instant->milliseconds-since-epoch
  ^long [^Instant instant]
  (.toEpochMilli instant))


(defn instant->seconds-since-epoch
  ^long [^Instant instant]
  (quot (.toEpochMilli instant)
        1000))


(defn instant->microseconds-since-epoch
  ^long [^Instant instant]
  (long (+ (* (.getEpochSecond instant)
              1000000)
           (quot (.getNano ^Instant instant)
                 1000))))


(defn microseconds-since-epoch->instant
  ^Instant [^long microseconds-since-epoch]
  (Instant/ofEpochSecond (quot microseconds-since-epoch 1000000)
                         (* (rem microseconds-since-epoch 1000000)
                            1000)))


(defn local-date-time->local-time
  ^LocalTime [^LocalDateTime ldt]
  (.toLocalTime ldt))

(defn local-time
  (^LocalTime []
   (LocalTime/now))
  (^LocalTime [arg]
   (if (instance? LocalTime arg)
     arg
     (-> (local-date-time arg)
         (local-date-time->local-time)))))


(defn milliseconds-since-epoch->local-time
  ^LocalTime [millis]
  (local-time millis))


(defn local-time->instant
  (^Instant [^LocalTime lt]
   (-> (local-time->local-date-time lt)
       (local-date-time->instant)))
  (^Instant [^LocalTime lt ^LocalDate ld]
   (-> (local-time->local-date-time lt ld)
       (local-date-time->instant))))


(defn local-time->seconds
  ^long [^LocalTime lt]
  (long (.toSecondOfDay lt)))


(defn local-time->milliseconds
  ^long [^LocalTime lt]
  (quot (.toNanoOfDay lt)
        nanoseconds-in-millisecond))


(defn seconds->local-time
  ^LocalTime [^long seconds]
  (LocalTime/ofSecondOfDay seconds))


(defn milliseconds->local-time
  ^LocalTime [^long milliseconds]
  (LocalTime/ofNanoOfDay (* milliseconds
                            (nanoseconds-in-millisecond))))


(defn instant->local-date-time
  (^LocalDateTime [^Instant inst ^ZoneId zone-id]
   (LocalDateTime/ofInstant inst zone-id))
  (^LocalDateTime [^Instant inst]
   (LocalDateTime/ofInstant inst (utc-zone-id))))


(defn zoned-date-time->instant
  ^Instant [^ZonedDateTime zid]
  (.toInstant zid))


(defn zoned-date-time->milliseconds-since-epoch
  ^long [^ZonedDateTime zid]
  (-> (zoned-date-time->instant zid)
      (instant->milliseconds-since-epoch)))


(defn instant->zoned-date-time
  (^ZonedDateTime [^Instant inst ^ZoneId zid]
   (.atZone inst zid))
  (^ZonedDateTime [^Instant inst]
   (instant->zoned-date-time inst (utc-zone-id))))


(defn milliseconds-since-epoch->zoned-date-time
  (^ZonedDateTime [^long zid timezone]
   (-> (milliseconds-since-epoch->instant zid)
       (instant->zoned-date-time timezone)))
  (^ZonedDateTime [^long zid]
   (milliseconds-since-epoch->zoned-date-time zid (utc-zone-id))))


(defn local-date-time->instant
  (^Instant [^LocalDateTime ldt zone-or-offset]
   (cond
     (instance? ZoneOffset zone-or-offset)
     (.toInstant ldt zone-or-offset)
     (instance? ZoneId zone-or-offset)
     (-> (.atZone ldt ^ZoneId zone-or-offset)
         (zoned-date-time->instant))
     :else
     (throw (Exception. (format "Unrecognized zone or offset type: %s"
                                (type zone-or-offset))))))
  (^Instant [^LocalDateTime ldt]
   (local-date-time->instant ldt (utc-zone-offset))))


(defn local-date-time->zoned-date-time
  (^Instant [^LocalDateTime ldt ^ZoneId zone-id]
   (.atZone ldt zone-id))
  (^Instant [^LocalDateTime ldt]
   (local-date-time->instant ldt (utc-zone-id))))


(defn local-date-time
  (^LocalDateTime []
   (LocalDateTime/now))
  (^LocalDateTime [arg]
   (if (instance? LocalDateTime arg)
     arg
     (-> (milliseconds-since-epoch->instant arg)
         (instant->local-date-time)))))


(defn milliseconds-since-epoch->local-date-time
  (^LocalDateTime [millis]
   (local-date-time millis))
  (^LocalDateTime [millis ^ZoneId zone-id]
   (-> (milliseconds-since-epoch->instant millis)
       (LocalDateTime/ofInstant ^ZoneId zone-id))))


(defn local-date-time->milliseconds-since-epoch
  (^long [^LocalDateTime ldt timezone]
   (-> (local-date-time->instant ldt timezone)
       (instant->milliseconds-since-epoch)))
  (^long [ldt]
   (local-date-time->milliseconds-since-epoch (utc-zone-id))))


(defn local-date
  (^LocalDate []
   (LocalDate/now)))


(defn local-date-time->local-date
  ^LocalDate [^LocalDateTime ldt]
  (.toLocalDate ldt))


(defn local-date->local-date-time
  (^LocalDateTime [^LocalDate ld lt]
   (let [time (if (number? lt)
                (local-time (long lt))
                lt)]
     (.atTime ld ^LocalTime time)))
  (^LocalDateTime [^LocalDate ld]
   (local-date->local-date-time ld (LocalTime/MIN))))


(defn local-date->instant
  (^Instant [^LocalDate ld ^LocalTime lt zoneid-or-offset]
   (-> (local-date->local-date-time ld lt)
       (local-date-time->instant zoneid-or-offset)))
  (^Instant [^LocalDate ld ^LocalTime lt]
   (local-date->instant ld lt (utc-zone-id)))
  (^Instant [^LocalDate ld]
   (local-date->instant ld 0 (utc-zone-id))))


(defn local-date->zoned-date-time
  (^Instant [^LocalDate ld lt zoneid]
   (-> (local-date->local-date-time ld lt)
       (local-date-time->zoned-date-time zoneid)))
  (^Instant [^LocalDate ld lt]
   (local-date->instant ld lt (utc-zone-id)))
  (^Instant [^LocalDate ld]
   (local-date->zoned-date-time ld 0 (utc-zone-id))))


(defn local-date->milliseconds-since-epoch
  (^long [^LocalDate ld ^LocalTime lt zoneid]
   (-> (local-date->local-date-time ld lt)
       (local-date-time->milliseconds-since-epoch zoneid)))
  (^long [^LocalDate ld]
   (local-date->milliseconds-since-epoch ld 0 (utc-zone-id))))


(defn local-time->local-date-time
  (^LocalDateTime [^LocalTime ldt ^LocalDate day]
   (local-date->local-date-time day ldt))
  (^LocalDateTime [^LocalTime ldt]
   (local-date->local-date-time (local-date) ldt)))


(defn milliseconds-since-epoch->local-date
  (^LocalDate [millis]
   (milliseconds-since-epoch->local-date millis (utc-zone-id)))
  (^LocalDate [millis zone-id]
   (-> (milliseconds-since-epoch->local-date-time millis zone-id)
       (.toLocalDate))))

(defn duration
  ([]
   (Duration/ofNanos 0))
  ([arg]
   (if (instance? Duration arg)
     arg
     (Duration/ofNanos (long arg)))))


(defn duration->nanoseconds
  ^long [^Duration duration]
  (.toNanos duration))


(defn nanoseconds->duration
  ^Duration [^long nanos]
  (Duration/ofNanos nanos))


(defn duration->milliseconds
  ^long [^Duration duration]
  (quot (.toNanos duration)
        nanoseconds-in-millisecond))


(defn milliseconds->duration
  ^Duration [^long millis]
  (Duration/ofNanos (* millis nanoseconds-in-millisecond)))


(casting/add-object-datatype! :instant Instant)
(casting/add-object-datatype! :zoned-date-time ZonedDateTime)
(casting/add-object-datatype! :local-date LocalDate)
(casting/add-object-datatype! :local-date-time LocalDateTime)
(casting/add-object-datatype! :duration Duration)
(casting/alias-datatype! :epoch-milliseconds :int64)
(casting/alias-datatype! :milliseconds :int64)
(casting/alias-datatype! :microseconds :int64)
(casting/alias-datatype! :nanoseconds :int64)
(casting/alias-datatype! :epoch-microseconds :int64)

(def datatypes #{:instant :zoned-date-time :local-date :local-date-time :duration})
