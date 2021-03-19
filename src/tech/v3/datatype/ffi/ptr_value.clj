(ns tech.v3.datatype.ffi.ptr-value
  (:require [tech.v3.datatype.ffi :as ffi]
            [tech.v3.datatype.errors :as errors]))



(defn- unchecked-ptr-value
  ^long [item]
  (if item
    (cond
      (instance? tech.v3.datatype.ffi.Pointer item)
      (.address ^tech.v3.datatype.ffi.Pointer item)
      (instance? tech.v3.datatype.native_buffer.NativeBuffer item)
      (.address ^tech.v3.datatype.native_buffer.NativeBuffer item)
      :else
      (do
        (errors/when-not-errorf
         (ffi/convertible-to-pointer? item)
         "Item %s is not convertible to a C pointer" item)
        (.address ^tech.v3.datatype.ffi.Pointer (ffi/->pointer item))))
    0))


(defn ptr-value
  "Item must not be nil.  A long address is returned."
  ^long [item]
  (let [retval (unchecked-ptr-value item)]
    (errors/when-not-error
     (not= 0 retval)
     "Pointer value is zero!")
    retval))


(defn ptr-value?
  "Item may be nil in which case 0 is returned."
  ^long [item]
  (if item
    (unchecked-ptr-value item)
    0))
