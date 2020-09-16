(ns tech.v3.datatype.errors
  (:import [org.apache.commons.math3.exception NotANumberException]))


(defmacro throwf
  [message & args]
  `(throw (Exception. (format ~message ~@args))))

(defmacro throw-nan
  []
  `(throw (NotANumberException.)))

(defmacro check-nan-error
  [arg]
  `(if (Double/isNaN ~arg)
     (throw-nan)
     arg))

(defmacro throw-index-out-of-boundsf
  [msg & args]
  `(throw (IndexOutOfBoundsException. (format ~msg ~@args))))

(defmacro check-idx
  [idx n-elems]
  `(when-not (< ~idx ~n-elems)
     (index-out-of-boundsf "idx (%s) >= n-elems (%s)" ~idx ~n-elems)))

(defmacro when-not-error
  [expr error-msg]
  `(when-not ~expr
     (throw (Exception. ~error-msg))))

(defmacro when-not-errorf
  [expr error-msg & args]
  `(when-not ~expr
     (throwf error-msg ~@args)))
