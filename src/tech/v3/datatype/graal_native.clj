(ns tech.v3.datatype.graal-native)


(defn graal-native?
  []
  (= "true" (System/getProperty "tech.v3.datatype.graal-native")))


(defmacro when-defined-graal-native
  [& body]
  (when (graal-native?)
    `(do ~@body)))


(defmacro when-not-defined-graal-native
  [& body]
  (when-not (graal-native?)
    `(do ~@body)))


(defmacro if-defined-graal-native
  [truecode falsecode]
  (if (graal-native?)
    `~truecode
    `~falsecode))
