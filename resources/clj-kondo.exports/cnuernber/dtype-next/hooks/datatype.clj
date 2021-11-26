(ns hooks.datatype)

(defmacro make-reader
  ([datatype n-elems read-op]
   `(make-reader ~datatype ~datatype ~n-elems ~read-op))
  ([reader-datatype adv-datatype n-elems read-op]
   `(let [idx# ~n-elems]
      ~read-op)))
