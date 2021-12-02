(ns tech.v3.datatype.object-pool
  "Simple threadsafe object pool."
  (:require [tech.v3.datatype.errors :as errors])
  (:import [java.util.concurrent ArrayBlockingQueue]))


(set! *warn-on-reflection* true)


(defprotocol PPool
  (take! [pool timeout]
    "Take an object from the pool.  Blocks until objects are available;
timeout is in milliseconds and if nil or <= 0 blocks forever.

Returns nil if timeout passes so nil cannot be stored in the pool.")
  (return! [pool item]
    "Return an item to the pool.  Timeout in"))


(extend-protocol PPool
  ArrayBlockingQueue
  (take! [pool timeout]
    (let [timeout (long (or timeout 0))
          retval
          (if (> timeout 0)
            (.poll pool timeout java.util.concurrent.TimeUnit/MILLISECONDS)
            (.poll pool))]
      (when retval
        @retval)))
  (return! [pool item]
    (.add pool (delay item))))


(defn make-object-pool
  "Create an object pool that has the capacity for n items.  Clients can take!
  to get an object or wait till one is available and must return! the item when
  finished."
  ([n-elems item-fn-item-seq]
   (let [retval (ArrayBlockingQueue. n-elems true)
         item-fn (if (fn? item-fn-item-seq)
                   item-fn-item-seq
                   (let [iterator (.iterator ^Iterable (seq item-fn-item-seq))]
                     (fn []
                       (locking iterator
                         (.next iterator)))))]
     (dotimes [_idx n-elems]
       (.add retval (delay
                      (if-let [retval (item-fn)]
                        retval
                        (errors/throwf "item-fn returned nil!")))))
     retval))
  ([item-seq]
   (make-object-pool (count item-seq) item-seq)))


(defmacro with-object-pool-object
  "Do something with an object pool object but be sure to return the object to
  the pool."
  [pool timeout varname & body]
  `(let [~varname (take! ~pool ~timeout)]
     (try
       ~@body
       (finally
         (return! ~pool ~varname)))))
