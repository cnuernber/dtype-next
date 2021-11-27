(ns tech.v3.parallel.for)


(defmacro doiter
  [varname iterable & body]
  `(let [~varname ~iterable]
     ~@body))
