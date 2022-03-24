(ns tech.v3.parallel.queue-iter
  "Read an iterator from in a separate thread into a queue returning a new iterator.  This
  allows a traversal of potentially blocking information to happen in a separate thread
  with a fixed queue size."
  (:require [tech.v3.parallel.for :as pfor])
  (:import [java.util Iterator NoSuchElementException]
           [java.util.concurrent ArrayBlockingQueue]))


(set! *warn-on-reflection* true)


(deftype ^:private QueueException [e])
(deftype ^:private QueueIterator [^Thread thread
                                  ^Iterator srcIter
                                  ^ArrayBlockingQueue queue
                                  continue?*
                                  ^:unsynchronized-mutable value]
  Iterator
  (hasNext [this] (not (identical? value ::end)))
  (next [this]
    (cond
      (identical? value ::end)
      (throw (NoSuchElementException.))
      (instance? QueueException value)
      (throw (.e ^QueueException value))
      :else
      (let [next-val (.take queue)
            cur-val value]
        (set! value next-val)
        (when (identical? value ::end)
          (.join thread))
        cur-val)))
  java.lang.AutoCloseable
  (close [this]
    (vreset! continue?* false)
    (.clear queue)
    (.join thread)
    (when (instance? java.lang.AutoCloseable srcIter)
      (.close ^java.lang.AutoCloseable srcIter))))


(defn queue-iter
  "Given an object with a conversion to iterator, create a new thread that will read that
  iterator and place the results into a queue of a fixed size.  Returns new iterator.

  Options:

  * `:queue-depth` - Queue depth.  Defaults to 16.
  * `:queue-thread-name` - Name of new thread.  Defaults to namespace name."
  ^Iterator [iter & [options]]
  (let [queue-depth (long (get options :queue-depth 16))
        thread-name (str (get options :queue-thread-name "tech.v3.parallel.queue-iter"))
        queue (ArrayBlockingQueue. queue-depth)
        continue?* (volatile! true)
        src-iter iter
        iter (pfor/->iterator iter)
        thread
        (Thread.
         ^Runnable
         (fn []
           (try
             (loop [thread-continue? @continue?*
                    has-next? (.hasNext iter)]
               (if (and thread-continue? has-next?)
                 (do
                   (.put queue (.next iter))
                   (recur @continue?* (.hasNext iter)))
                 (.put queue ::end)))
             (catch Exception e
               (.put queue (QueueException. e)))))
         thread-name)]
    (.start thread)
    (QueueIterator. thread src-iter queue continue?* (.take queue))))


(comment
  (vec (iterator-seq (queue-iter (range 2000))))
  )
