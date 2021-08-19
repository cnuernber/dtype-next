(ns tech.v3.datatype.graal-native)

(defmacro ^:private in-image-buildtime-code? []
  (try
    (import 'org.graalvm.nativeimage.ImageInfo)
    `(org.graalvm.nativeimage.ImageInfo/inImageBuildtimeCode)
    (catch ClassNotFoundException e
      false)))

(defn graal-native?
  []
  (if-let [graal-native-prop (System/getProperty "tech.v3.datatype.graal-native")]
    (= "true" graal-native-prop)
    (in-image-buildtime-code?)))


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
