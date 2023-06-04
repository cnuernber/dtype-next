(ns tech.v3.parallel.queue-iter
  "Read an iterator from in a separate thread into a queue returning a new iterator.  This
  allows a traversal of potentially blocking information to happen in a separate thread
  with a fixed queue size."
  (:require [tech.v3.parallel.for :as pfor]
            [clojure.tools.logging :as log])
  (:import [java.util Iterator NoSuchElementException]
           [java.util.concurrent ArrayBlockingQueue Executors ExecutorService ThreadFactory]
           [ham_fisted IFnDef]
           [java.lang AutoCloseable]))


(set! *warn-on-reflection* true)


(defonce ^:private default-thread-pool*
  (delay
    (Executors/newCachedThreadPool
     (reify ThreadFactory
       (newThread [this runnable]
         (let [t (Thread. runnable (str (ns-name *ns*)))]
           (.setDaemon t true)
           t))))))


(defn default-executor-service
  "Default executor service that is created via 'newCachedThreadPool with a custom thread
  factory that creates daemon threads.  This is an executor service that is suitable for
  blocking operations as it creates new threads as needed."
  ^ExecutorService[]
  @default-thread-pool*)


(deftype ^:private QueueException [e])
(deftype ^:private QueueIterator [^ArrayBlockingQueue queue
                                  ^:unsynchronized-mutable value
                                  close-fn*]
  Iterator
  (hasNext [this] (not (identical? value ::end)))
  (next [this]
    (cond
      (identical? value ::end)
      (throw (NoSuchElementException.))
      (instance? QueueException value)
      (do
        (.close this)
        (throw (.e ^QueueException value)))
      :else
      (let [next-val (.take queue)
            cur-val value]
        (set! value next-val)
        (when (identical? next-val ::end)
          (.close this))
        cur-val)))
  java.lang.AutoCloseable
  (close [this]
    @close-fn*))


(defn queue-iter
  "Given an object with a conversion to iterator, create a new thread that will read that
  iterator and place the results into a queue of a fixed size.  Returns new iterator.

  Options:

  * `:queue-depth` - Queue depth.  Defaults to 16.
  * `:log-level` - When set a message is logged when the iteration is finished.
  * `:executor-service` - Which executor service to use to run the thread.  Defaults to
     a default one created via [[default-executor-service]]."
  ^Iterator [iter & [options]]
  (let [queue-depth (long (get options :queue-depth 16))
        queue (ArrayBlockingQueue. queue-depth)
        continue?* (volatile! true)
        src-iter iter
        iter (pfor/->iterator iter)
        run-fn (fn []
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
        ^ExecutorService service (or (get options :executor-service)
                                     (default-executor-service))
        close-fn* (delay
                    (try
                      (vreset! continue?* false)
                      (.clear queue)
                      (when (instance? java.lang.AutoCloseable src-iter)
                        (.close ^java.lang.AutoCloseable src-iter))
                      (when-let [ll (get options :log-level)]
                        (log/log ll "queue-iter thread shutdown"))
                      (catch Exception e
                        (log/warnf e "Error closing down queue-iter thread"))))]
    (.submit service ^Callable run-fn)
    (QueueIterator. queue (.take queue) close-fn*)))


(deftype QueueFn [^{:unsynchronized-mutable true
                    :tag ArrayBlockingQueue} queue
                  close-fn*]
  IFnDef
  (invoke [this]
    (when queue
      (let [value (.take queue)]
        (cond
          (identical? value ::end)
          (do
            (.close this)
            nil)
          (instance? QueueException value)
          (do
            (.close this)
            (throw (.e ^QueueException value)))
          :else
          value))))
  AutoCloseable
  (close [this]
    (set! queue nil)
    @close-fn*))


(defn queue-fn
  "Given a clojure fn, create a new thread that will read that
  fn and place the results into a queue of a fixed size.  Returns new fn.
  Iteration stops when the src-fn returns nil.

  Options:

  * `:queue-depth` - Queue depth.  Defaults to 16.
  * `:log-level` - When set a message is logged when the iteration is finished.
  * `:executor-service` - Which executor service to use to run the thread.  Defaults to
     a default one created via [[default-executor-service]].
  * `:close-fn` - Function to call to close upstream iteration."
  [src-fn & [options]]
  (let [queue-depth (long (get options :queue-depth 16))
        queue (ArrayBlockingQueue. queue-depth)
        continue?* (volatile! true)
        close-fn (get options :close-fn)
        run-fn (fn []
                 (try
                   (loop [thread-continue? @continue?*
                          next-val (src-fn)]
                     (if (and thread-continue? next-val)
                       (do
                         (.put queue next-val)
                         (recur @continue?* (src-fn)))
                       (.put queue ::end)))
                   (catch Exception e
                     (.put queue (QueueException. e)))))
        ^ExecutorService service (or (get options :executor-service)
                                     (default-executor-service))
        close-fn* (delay
                    (try
                      (vreset! continue?* false)
                      (.clear queue)
                      (when close-fn
                        (close-fn))
                      (when-let [ll (get options :log-level)]
                        (log/log ll "queue-fn thread shutdown"))
                      (catch Exception e
                        (log/warnf e "Error closing down queue-fn thread"))))]
    (.submit service ^Callable run-fn)
    (QueueFn. queue close-fn*)))


(defn iter-fn
  "Create a non threadsafe clojure fn that will iterate the iterator.  Once iterator is exhausted further
  invocations of the function will return nil."
  [item]
  (let [iter (pfor/->iterator item)]
    (fn []
      (when (.hasNext iter)
        (.next iter)))))


(comment
  (def data (vec (iterator-seq (queue-iter (range 2000) {:log-level :info}))))
  )
