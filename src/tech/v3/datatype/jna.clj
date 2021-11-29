(ns tech.v3.datatype.jna
  "Protocol bindings to the tech.jna system.  Most users will not need to
  require this namespace."
  (:require [tech.v3.jna :as jna])
  (:import [tech.v3.datatype.native_buffer NativeBuffer]
           [com.sun.jna Pointer]))



(extend-type NativeBuffer
  jna/PToPtr
  (is-jna-ptr-convertible? [item] true)
  (->ptr-backing-store [item]
    (Pointer. (.address item))))
