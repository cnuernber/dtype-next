(ns tech.v3.datatype.errors
  "Generic, basic error handling.  No dependencies aside from apache commons math
  for NAN exception."
  (:import [org.apache.commons.math3.exception NotANumberException]
           [java.util NoSuchElementException]))


(defmacro throwf
  "Throw an exception and format a message"
  [message & args]
  `(throw (Exception. (format ~message ~@args))))

(defmacro throw-nan
  "Throw a NAN exception."
  []
  `(throw (NotANumberException.)))

(defmacro check-nan-error
  "Check if arg is double NaN and throw an exception if so."
  [arg]
  `(if (Double/isNaN ~arg)
     (throw-nan)
     arg))

(defmacro throw-index-out-of-boundsf
  "Throw an index out of bounds exception with a nice message."
  [msg & args]
  `(throw (IndexOutOfBoundsException. (format ~msg ~@args))))

(defmacro check-idx
  "Check that an index is less than n-elems.  Throw an index-out-of-bounds
  exception if that isn't the case."
  [idx n-elems]
  `(do (when-not (< ~idx ~n-elems)
         (throw-index-out-of-boundsf "idx (%s) >= n-elems (%s)" ~idx ~n-elems))
       ~idx))

(defmacro when-not-error
  "Throw an error in the case where expr isn't true."
  [expr error-msg]
  {:style/indent 1}
  `(when-not ~expr
     (throw (Exception. ~error-msg))))

(defmacro when-not-errorf
  "Throw an exception with formatting in the case where expr isn't true."
  [expr error-msg & args]
  {:style/indent 1}
  `(when-not ~expr
     (throwf ~error-msg ~@args)))


(defmacro check-offset-length
  "Check that a combined offset and length fit within elem-ecount.
  Throw an exception with a nice message in the case where bounds were
  exceeded."
  [offset length elem-ecount]
  `(let [offset# (long ~offset)
         length# (long ~length)
         elem-ecount# (long ~elem-ecount)]
     (when-not-errorf (<= (+ offset# length#) elem-ecount#)
       "Offset %d + length (%d) out of range of item length %d"
       offset# length# elem-ecount#)))


(defmacro throw-unimplemented
  [& args]
  `(throw (UnsupportedOperationException. "Not implemented")))


(defmacro throw-iterator-past-end
  []
  `(throw (NoSuchElementException. "Iteration past end of sequence")))
