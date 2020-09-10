(ns tech.v3.datatype.datetime
  (:require [tech.v3.datatype.casting :as casting])
  (:import [java.time LocalDate LocalDateTime
            ZonedDateTime Instant ZoneId Duration ZoneOffset
            OffsetDateTime LocalTime]
           [java.time.temporal ChronoUnit Temporal ChronoField
            WeekFields TemporalAmount]
           [tech.v3.datatype PackedLocalDate]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare local-date-time
         local-date-time->instant
         local-date-time->local-date
         local-time->local-date-time)


(def ^{:tag 'long} seconds-in-day (* 24 3600))
(def ^{:tag 'long} seconds-in-hour 3600)
(def ^{:tag 'long} seconds-in-minute 60)
(def ^{:tag 'long} nanoseconds-in-millisecond 1000000)
(def ^{:tag 'long} nanoseconds-in-second 1000000000)
(def ^{:tag 'long} nanoseconds-in-minute 60000000000)
(def ^{:tag 'long} nanoseconds-in-hour 3600000000000)
(def ^{:tag 'long} nanoseconds-in-day 86400000000000)
(def ^{:tag 'long} nanoseconds-in-week 604800000000000)
(def ^{:tag 'long} milliseconds-in-week 604800000)
(def ^{:tag 'long} milliseconds-in-day 86400000)
(def ^{:tag 'long} milliseconds-in-hour 3600000)
(def ^{:tag 'long} milliseconds-in-minute 60000)
(def ^{:tag 'long} milliseconds-in-second 1000)


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
  ^long [^LocalDateTime ldt]
  (-> (local-date-time->instant ldt)
      (instant->milliseconds-since-epoch)))


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
  (^long [^LocalDate ld ^LocalTime lt]
   (-> (local-date->local-date-time ld lt)
       (local-date-time->milliseconds-since-epoch)))
  (^long [^LocalDate ld]
   (-> (local-date->local-date-time ld)
       (local-date-time->milliseconds-since-epoch))))


(defn local-time->local-date-time
  (^LocalDateTime [^LocalTime ldt ^LocalDate day]
   (local-date->local-date-time day ldt))
  (^LocalDateTime [^LocalTime ldt]
   (local-date->local-date-time (local-date) ldt)))

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
