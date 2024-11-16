(ns tech.v3.datatype.datetime-test
  (:require [clojure.test :refer [deftest is]]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.datetime.operations :as dtype-dt-ops]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.packing :as packing])
  (:import [java.time ZoneId]))


(deftest simple-packing
  (let [ld (dtype-dt/local-date)]
    (let [data-list (dtype/make-container :list :packed-local-date 0)]
      (.add data-list ld)
      (is (= [ld]
             (vec data-list)))
      (is (= [ld]
             (vec (dtype/->array data-list)))))
    (let [data-buf (dtype/make-container :jvm-heap :packed-local-date
                                         (repeat 5 nil))]
      (is (= (vec (repeat 5 nil))
             (vec data-buf)))
      (dtype/set-value! data-buf 1 ld)
      (is (= (vec [nil ld nil nil nil])
             (vec data-buf))))))


(deftest epoch-conversion-test
  (let [initial-data (dtype-dt/plus-temporal-amount
                      (dtype/make-container :instant (repeat 5 (dtype-dt/instant)))
                      (dfn/* dtype-dt/milliseconds-in-day (range 5))
                      :milliseconds)
        time-types [:epoch-milliseconds :epoch-microseconds :epoch-seconds :epoch-days]
        datetime-types [:instant :zoned-date-time :local-date :local-date-time]
        timezone (ZoneId/of "America/Denver")
        zoneid-types #{:zoned-date-time :local-date :local-date-time}
        epoch-data (dtype-dt-ops/datetime->epoch :epoch-microseconds initial-data)
        datetime-data (for [datetime-type datetime-types]
                        (let [utc-datetime-data (dtype-dt-ops/epoch->datetime datetime-type epoch-data)]
                          (merge
                           {:utc utc-datetime-data}
                           (when (zoneid-types datetime-type)
                             {:denver
                              (dtype-dt-ops/epoch->datetime timezone datetime-type epoch-data)}))))
        tz-map {:utc (dtype-dt/utc-zone-id)
                :denver timezone}
        epoch-data
        (->>
         (for [dt-data datetime-data
               time-dtype time-types]
           (->> dt-data
                (map (fn [[k v]]
                       (let [datetime-dtype (dtype/elemwise-datatype v)]
                         [datetime-dtype
                          {k
                           (let [retval (if (zoneid-types datetime-dtype)
                                         (dtype-dt-ops/datetime->epoch (tz-map k) time-dtype v)
                                         (dtype-dt-ops/datetime->epoch time-dtype v))]
                             ;;Force entire operation to complete
                             {(dtype/elemwise-datatype retval) (vec retval)})}])))))
         (apply concat)
         (group-by first)
         (map (fn [[k v]]
                [k (->> (map second v)
                        (group-by ffirst)
                        (map (fn [[kk vv]]
                               [kk (->> (map kk vv)
                                        (apply merge))]))
                        (into {}))]))
         (into {}))]
    (is (= 4 (count epoch-data)))))


(deftest plus-minus-temporal-amount
  (let [ld-vec (dtype/make-container :local-date
                                     (repeat 4 (dtype-dt/local-date)))
        later-vec (dtype-dt-ops/plus-temporal-amount ld-vec 3 :days)
        days-vec (dtype-dt-ops/between ld-vec later-vec :days)]
    (is (= [3 3 3 3] (vec days-vec)))
    (is (= :days (dtype/elemwise-datatype days-vec)))
    (let [ld-epoch (dtype-dt-ops/datetime->epoch ld-vec)
          later-epoch (dtype-dt-ops/plus-temporal-amount ld-epoch 3 :days)
          between-epoch (dtype-dt-ops/between ld-epoch later-epoch :days)]
      (is (= [3 3 3 3] (vec between-epoch)))
      (is (= (vec later-vec)
             (vec (dtype-dt-ops/epoch->datetime
                   :packed-local-date later-epoch))))))
  (let [insts (dtype/make-container :packed-instant
                                    (repeat 20 (dtype-dt/instant)))
        offset-insts (dtype-dt-ops/plus-temporal-amount
                      insts (dfn/* 20 (range 20)) :seconds)
        windows (-> (dtype-dt-ops/variable-rolling-window-ranges
                     offset-insts 50 :seconds)
                    vec)
        tweener (dtype-dt-ops/between-op :packed-instant :seconds)]
    (is (= 20 (count windows)))
    (is (every? (fn [window]
                  (< (long (tweener (offset-insts (first window))
                                    (offset-insts (last window))))
                     50))
                windows))))
