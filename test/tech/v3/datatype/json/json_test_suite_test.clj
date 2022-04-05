(ns clojure.data.json-test-suite-test
  (:require [tech.v3.datatype.char-input :as char-input]
            [clojure.test :refer :all]
            [clojure.string :as str]))

(deftest i-number-double-huge-neg-exp-test
  (is (= [0.0] (char-input/read-json "[123.456e-789]"))))

(deftest i-number-huge-exp-test
  (is (= [##Inf]
         (char-input/read-json "[0.4e00669999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999969999999006]"))))

(deftest i-number-neg-int-huge-exp-test
  (is (= [##-Inf] (char-input/read-json "[-1e+9999]"))))

(deftest i-number-pos-double-huge-exp-test
  (is (= [##Inf] (char-input/read-json "[1.5e+9999]"))))

(deftest i-number-real-neg-overflow-test
  (is (= [##-Inf] (char-input/read-json "[-123123e100000]"))))

(deftest i-number-real-pos-overflow-test
  (is (= [##Inf] (char-input/read-json "[123123e100000]"))))

(deftest i-number-real-underflow-test
  (is (= [0.0] (char-input/read-json "[123e-10000000]"))))

(deftest i-number-too-big-neg-int-test
  (is (= [-123123123123123123123123123123N]
         (char-input/read-json "[-123123123123123123123123123123]"))))

(deftest i-number-too-big-pos-int-test
  (is (= [100000000000000000000N] (char-input/read-json "[100000000000000000000]"))))

(deftest i-number-very-big-negative-int-test
  (is (= [-237462374673276894279832749832423479823246327846N]
         (char-input/read-json "[-237462374673276894279832749832423479823246327846]"))))

(deftest n-array-1-true-without-comma-test
  (is (thrown? Exception (char-input/read-json "[1 true]"))))

(deftest n-array-colon-instead-of-comma-test
  (is (thrown? Exception (char-input/read-json "[\"\": 1]"))))

(deftest n-array-comma-and-number-test
  (is (thrown? Exception (char-input/read-json "[,1]"))))

(deftest n-array-double-comma-test
  (is (thrown? Exception (char-input/read-json "[1,,2]"))))

(deftest n-array-double-extra-comma-test
  (is (thrown? Exception (char-input/read-json "[\"x\",,]"))))

(deftest n-array-extra-comma-test
  (is (thrown? Exception (char-input/read-json "[\"\",]"))))

(deftest n-array-incomplete-invalid-value-test
  (is (thrown? Exception (char-input/read-json "[x"))))

(deftest n-array-incomplete-test
  (is (thrown? Exception (char-input/read-json "[\"x\""))))

(deftest n-array-inner-array-no-comma-test
  (is (thrown? Exception (char-input/read-json "[3[4]]"))))

(deftest n-array-items-separated-by-semicolon-test
  (is (thrown? Exception (char-input/read-json "[1:2]"))))

(deftest n-array-just-comma-test
  (is (thrown? Exception (char-input/read-json "[,]"))))

(deftest n-array-just-minus-test
  (is (thrown? Exception (char-input/read-json "[-]"))))

(deftest n-array-missing-value-test
  (is (thrown? Exception (char-input/read-json "[   , \"\"]"))))

(deftest n-array-newlines-unclosed-test
  (is (thrown? Exception (char-input/read-json "[\"a\",\n4\n,1,"))))

(deftest n-array-number-and-comma-test
  (is (thrown? Exception (char-input/read-json "[1,]"))))

(deftest n-array-number-and-several-commas-test
  (is (thrown? Exception (char-input/read-json "[1,,]"))))

(deftest n-array-spaces-vertical-tab-formfeed-test
  (is (thrown? Exception (char-input/read-json "[\"a\"\\f]"))))

(deftest n-array-star-inside-test
  (is (thrown? Exception (char-input/read-json "[*]"))))

(deftest n-array-unclosed-test
  (is (thrown? Exception (char-input/read-json "[\"\""))))

(deftest n-array-unclosed-trailing-comma-test
  (is (thrown? Exception (char-input/read-json "[1,"))))

(deftest n-array-unclosed-with-new-lines-test
  (is (thrown? Exception (char-input/read-json "[1,\n1\n,1"))))

(deftest n-array-unclosed-with-object-inside-test
  (is (thrown? Exception (char-input/read-json "[{}"))))

(deftest n-number-++-test
  (is (thrown? Exception (char-input/read-json "[++1234]"))))

(deftest n-number-+1-test
  (is (thrown? Exception (char-input/read-json "[+1]"))))

(deftest n-number-+Inf-test
  (is (thrown? Exception (char-input/read-json "[+Inf]"))))

(deftest n-number--01-test
  (is (thrown? Exception (char-input/read-json "[-01]"))))

(deftest n-number--1.0.-test
  (is (thrown? Exception (char-input/read-json "[-1.0.]"))))

(deftest n-number--2.-test
  (is (thrown? Exception (char-input/read-json "[-2.]"))))

(deftest n-number--NaN-test
  (is (thrown? Exception (char-input/read-json "[-NaN]"))))

(deftest n-number-.-1-test
  (is (thrown? Exception (char-input/read-json "[.-1]"))))

(deftest n-number-.2e-3-test
  (is (thrown? Exception (char-input/read-json "[.2e-3]"))))

(deftest n-number-0-capital-E+-test
  (is (thrown? Exception (char-input/read-json "[0E+]"))))

(deftest n-number-0-capital-E-test
  (is (thrown? Exception (char-input/read-json "[0E]"))))

(deftest n-number-0.1.2-test
  (is (thrown? Exception (char-input/read-json "[0.1.2]"))))

(deftest n-number-0.3e+-test
  (is (thrown? Exception (char-input/read-json "[0.3e+]"))))

(deftest n-number-0.3e-test
  (is (thrown? Exception (char-input/read-json "[0.3e]"))))

(deftest n-number-0.e1-test
  (is (thrown? Exception (char-input/read-json "[0.e1]"))))

(deftest n-number-0e+-test
  (is (thrown? Exception (char-input/read-json "[0e+]"))))

(deftest n-number-0e-test
  (is (thrown? Exception (char-input/read-json "[0e]"))))

(deftest n-number-1-000-test
  (is (thrown? Exception (char-input/read-json "[1 000.0]"))))

(deftest n-number-1.0e+-test
  (is (thrown? Exception (char-input/read-json "[1.0e+]"))))

(deftest n-number-1.0e--test
  (is (thrown? Exception (char-input/read-json "[1.0e-]"))))

(deftest n-number-1.0e-test
  (is (thrown? Exception (char-input/read-json "[1.0e]"))))

(deftest n-number-1eE2-test
  (is (thrown? Exception (char-input/read-json "[1eE2]"))))

(deftest n-number-2.e+3-test
  (is (thrown? Exception (char-input/read-json "[2.e+3]"))))

(deftest n-number-2.e-3-test
  (is (thrown? Exception (char-input/read-json "[2.e-3]"))))

(deftest n-number-2.e3-test
  (is (thrown? Exception (char-input/read-json "[2.e3]"))))

(deftest n-number-9.e+-test
  (is (thrown? Exception (char-input/read-json "[9.e+]"))))

(deftest n-number-Inf-test
  (is (thrown? Exception (char-input/read-json "[Inf]"))))

(deftest n-number-NaN-test
  (is (thrown? Exception (char-input/read-json "[NaN]"))))

(deftest n-number-expression-test
  (is (thrown? Exception (char-input/read-json "[1+2]"))))

(deftest n-number-hex-1-digit-test
  (is (thrown? Exception (char-input/read-json "[0x1]"))))

(deftest n-number-hex-2-digits-test
  (is (thrown? Exception (char-input/read-json "[0x42]"))))

(deftest n-number-infinity-test
  (is (thrown? Exception (char-input/read-json "[Infinity]"))))

(deftest n-number-invalid+--test
  (is (thrown? Exception (char-input/read-json "[0e+-1]"))))

(deftest n-number-invalid-negative-real-test
  (is (thrown? Exception (char-input/read-json "[-123.123foo]"))))

(deftest n-number-minus-infinity-test
  (is (thrown? Exception (char-input/read-json "[-Infinity]"))))

(deftest n-number-minus-sign-with-trailing-garbage-test
  (is (thrown? Exception (char-input/read-json "[-foo]"))))

(deftest n-number-minus-space-1-test
  (is (thrown? Exception (char-input/read-json "[- 1]"))))

(deftest n-number-neg-int-starting-with-zero-test
  (is (thrown? Exception (char-input/read-json "[-012]"))))

(deftest n-number-neg-real-without-int-part-test
  (is (thrown? Exception (char-input/read-json "[-.123]"))))

(deftest n-number-neg-with-garbage-at-end-test
  (is (thrown? Exception (char-input/read-json "[-1x]"))))

(deftest n-number-real-garbage-after-e-test
  (is (thrown? Exception (char-input/read-json "[1ea]"))))

(deftest n-number-real-without-fractional-part-test
  (is (thrown? Exception (char-input/read-json "[1.]"))))

(deftest n-number-starting-with-dot-test
  (is (thrown? Exception (char-input/read-json "[.123]"))))

(deftest n-number-with-alpha-char-test
  (is (thrown? Exception (char-input/read-json "[1.8011670033376514H-308]"))))

(deftest n-number-with-alpha-test
  (is (thrown? Exception (char-input/read-json "[1.2a-3]"))))

(deftest n-number-with-leading-zero-test
  (is (thrown? Exception (char-input/read-json "[012]"))))

(deftest n-object-non-string-key-but-huge-number-instead-test
  (is (thrown? Exception (char-input/read-json "{9999E9999:1}"))))

(deftest n-structure-array-with-unclosed-string-test
  (is (thrown? Exception (char-input/read-json "[\"asd]"))))

(deftest n-structure-end-array-test
  (is (thrown? Exception (char-input/read-json "]"))))

(deftest n-structure-number-with-trailing-garbage-test
  (is (thrown? Exception (char-input/read-json "2@"))))

(deftest n-structure-open-array-apostrophe-test
  (is (thrown? Exception (char-input/read-json "['"))))

(deftest n-structure-open-array-comma-test
  (is (thrown? Exception (char-input/read-json "[,"))))

(deftest n-structure-open-array-open-object-test
  (is (thrown? Exception (char-input/read-json "[{"))))

(deftest n-structure-open-array-open-string-test
  (is (thrown? Exception (char-input/read-json "[\"a"))))

(deftest n-structure-open-array-string-test
  (is (thrown? Exception (char-input/read-json "[\"a\""))))

(deftest n-structure-open-object-close-array-test
  (is (thrown? Exception (char-input/read-json "{]"))))

(deftest n-structure-open-object-open-array-test
  (is (thrown? Exception (char-input/read-json "{["))))

(deftest n-structure-unclosed-array-partial-null-test
  (is (thrown? Exception (char-input/read-json "[ false, nul"))))

(deftest n-structure-unclosed-array-test
  (is (thrown? Exception (char-input/read-json "[1"))))

(deftest n-structure-unclosed-array-unfinished-false-test
  (is (thrown? Exception (char-input/read-json "[ true, fals"))))

(deftest n-structure-unclosed-array-unfinished-true-test
  (is (thrown? Exception (char-input/read-json "[ false, tru"))))

(deftest y-array-arraysWithSpaces-test
  (is (= [[]] (char-input/read-json "[[]   ]"))))

(deftest y-array-empty-string-test
  (is (= [""] (char-input/read-json "[\"\"]"))))

(deftest y-array-empty-test
  (is (= [] (char-input/read-json "[]"))))

(deftest y-array-ending-with-newline-test
  (is (= ["a"] (char-input/read-json "[\"a\"]"))))

(deftest y-array-false-test
  (is (= [false] (char-input/read-json "[false]"))))

(deftest y-array-heterogeneous-test
  (is (= [nil 1 "1" {}] (char-input/read-json "[null, 1, \"1\", {}]"))))

(deftest y-array-null-test
  (is (= [nil] (char-input/read-json "[null]"))))

(deftest y-array-with-1-and-newline-test
  (is (= [1] (char-input/read-json "[1\n]"))))

(deftest y-array-with-leading-space-test
  (is (= [1] (char-input/read-json " [1]"))))

(deftest y-array-with-several-null-test
  (is (= [1 nil nil nil 2] (char-input/read-json "[1,null,null,null,2]"))))

(deftest y-array-with-trailing-space-test
  (is (= [2] (char-input/read-json "[2] "))))

(deftest y-number-0e+1-test
  (is (= [0.0] (char-input/read-json "[0e+1]"))))

(deftest y-number-0e1-test
  (is (= [0.0] (char-input/read-json "[0e1]"))))

(deftest y-number-after-space-test
  (is (= [4] (char-input/read-json "[ 4]"))))

(deftest y-number-double-close-to-zero-test
  (is (= [-1.0E-78]
         (char-input/read-json "[-0.000000000000000000000000000000000000000000000000000000000000000000000000000001]"))))

(deftest y-number-int-with-exp-test
  (is (= [200.0] (char-input/read-json "[20e1]"))))

(deftest y-number-minus-zero-test
  (is (= [0] (char-input/read-json "[-0]"))))

(deftest y-number-negative-int-test
  (is (= [-123] (char-input/read-json "[-123]"))))

(deftest y-number-negative-one-test
  (is (= [-1] (char-input/read-json "[-1]"))))

(deftest y-number-negative-zero-test
  (is (= [0] (char-input/read-json "[-0]"))))

(deftest y-number-real-capital-e-neg-exp-test
  (is (= [0.01] (char-input/read-json "[1E-2]"))))

(deftest y-number-real-capital-e-pos-exp-test
  (is (= [100.0] (char-input/read-json "[1E+2]"))))

(deftest y-number-real-capital-e-test
  (is (= [1.0E22] (char-input/read-json "[1E22]"))))

(deftest y-number-real-exponent-test
  (is (= [1.23E47] (char-input/read-json "[123e45]"))))

(deftest y-number-real-fraction-exponent-test
  (is (= [1.23456E80] (char-input/read-json "[123.456e78]"))))

(deftest y-number-real-neg-exp-test
  (is (= [0.01] (char-input/read-json "[1e-2]"))))

(deftest y-number-real-pos-exponent-test
  (is (= [100.0] (char-input/read-json "[1e+2]"))))

(deftest y-number-simple-int-test
  (is (= [123] (char-input/read-json "[123]"))))

(deftest y-number-simple-real-test
  (is (= [123.456789] (char-input/read-json "[123.456789]"))))

(deftest y-number-test
  (is (= [1.23E67] (char-input/read-json "[123e65]"))))

(deftest y-object-extreme-numbers-test
  (is (= {"min" -1.0E28, "max" 1.0E28}
         (char-input/read-json "{\"min\": -1.0e+28, \"max\": 1.0e+28}"))))

(deftest y-string-in-array-test
  (is (= ["asd"] (char-input/read-json "[\"asd\"]"))))

(deftest y-string-in-array-with-leading-space-test
  (is (= ["asd"] (char-input/read-json "[ \"asd\"]"))))

(deftest y-structure-true-in-array-test
  (is (= [true] (char-input/read-json "[true]"))))

(deftest y-structure-whitespace-array-test
  (is (= [] (char-input/read-json " [] "))))
