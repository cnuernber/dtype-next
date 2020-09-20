(ns tech.compute.cpu.utils)


(set! *warn-on-reflection* true)
(set! *unchecked-math* true)


(defn in-range?
  [^long lhs-off ^long lhs-len ^long rhs-off ^long rhs-len]
  (or (and (>= rhs-off lhs-off)
           (< rhs-off (+ lhs-off lhs-len)))
      (and (>= lhs-off rhs-off)
           (< lhs-off (+ rhs-off rhs-len)))))
