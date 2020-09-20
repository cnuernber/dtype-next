(ns tech.compute.verify.tensor
  (:require [tech.compute.context :as compute-ctx]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype :as dtype]
            [tech.v3.tensor :as dtt]
            [tech.compute.tensor :as ct]
            [tech.resource :as resource]
            [clojure.test :refer :all]))


(defmacro verify-context
  [driver datatype & body]
  `(resource/stack-resource-context
    (compute-ctx/with-context
      {:driver ~driver}
      ~@body)))


(defn clone
  [driver datatype]
  (verify-context
   driver datatype
   (let [tensor (ct/->tensor (partition 3 (range 9)))
         dev-tens (ct/clone-to-device tensor)
         host-tens (ct/clone-to-host dev-tens)]
     (is (dfn/equals tensor host-tens))
     (let [sub-tens (dtt/select tensor [0 1] [0 1])
           dev-tens (ct/clone-to-device sub-tens)
           host-tens (ct/clone-to-host dev-tens)]
       (is (dfn/equals sub-tens host-tens))))))


(defn assign!
  [driver datatype]
  (verify-context
   driver datatype
   (let [tensor (ct/->tensor (partition 3 (range 9)))
         dev-tens (ct/new-tensor [3 3])
         _ (ct/assign! dev-tens tensor)
         host-tens (ct/clone-to-host dev-tens)]
     (is (dfn/equals tensor host-tens)))))
