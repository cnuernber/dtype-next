(ns tech.compute.registry
  "Place to store global information about the drivers available to the compute
  subystem."
  (:require [tech.compute.driver :as drv]))


(def ^:dynamic *registered-drivers* (atom {}))
(def ^:dynamic *cpu-driver-name* (atom nil))


(defn- find-driver
  [driver-name]
  (get @*registered-drivers* driver-name))


(defn driver
  [driver-name]
  (if-let [retval (find-driver driver-name)]
    retval
    (throw (ex-info (format "Failed to find driver.  Perhaps a require is missing?" )
                    {:driver-name driver-name}))))


(defn register-driver
  [driver]
  (swap! *registered-drivers* assoc (drv/driver-name driver) driver)
  (drv/driver-name driver))


(defn driver-names
  []
  (->> (keys @*registered-drivers*)
       set))


;;The cpu driver has a special place in that it can attach to things that
;;aren't in the ecosystem.
(defn set-cpu-driver-name!
  [driver-name]
  (reset! *cpu-driver-name* driver-name))


(defn cpu-driver-name
  []
  @*cpu-driver-name*)


(defmacro current-ns->keyword
  "Use this to name your driver."
  []
  `(keyword (str *ns*)))
