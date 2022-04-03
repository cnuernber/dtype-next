(ns tech.v3.datatype.char-input-test
  (:require [tech.v3.datatype.char-input :refer [read-csv read-csv-compat] :as char-input]
            [clojure.test :refer [deftest is]]))


(deftest funky-csv
  (is (= [["a,b" "c\"def\"" "one,two" "\"a,b,c\"def\"g\""]
          ["abba" "def" "1" "2"]
          ["df" "ef" "5" nil]]
         (->> (read-csv (java.io.File. "test/data/funky.csv"))
              (iterator-seq)
              (vec)))))


(def ^{:private true} simple
  "Year,Make,Model
1997,Ford,E350
2000,Mercury,Cougar
")

(def ^{:private true} simple-alt-sep
  "Year;Make;Model
1997;Ford;E350
2000;Mercury;Cougar
")

(def ^{:private true} complicated
  "1997,Ford,E350,\"ac, abs, moon\",3000.00
1999,Chevy,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00
1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",\"\",5000.00
1996,Jeep,Grand Cherokee,\"MUST SELL!
air, moon roof, loaded\",4799.00")

(deftest reading
  (let [csv (read-csv-compat simple)]
    (is (= (count csv) 3))
    (is (= (count (first csv)) 3))
    (is (= (first csv) ["Year" "Make" "Model"]))
    (is (= (last csv) ["2000" "Mercury" "Cougar"])))
  (let [csv (read-csv-compat simple-alt-sep :separator \;)]
    (is (= (count csv) 3))
    (is (= (count (first csv)) 3))
    (is (= (first csv) ["Year" "Make" "Model"]))
    (is (= (last csv) ["2000" "Mercury" "Cougar"])))
  (let [csv (read-csv-compat complicated)]
    (is (= (count csv) 4))
    (is (= (count (first csv)) 5))
    (is (= (first csv)
           ["1997" "Ford" "E350" "ac, abs, moon" "3000.00"]))
    (is (= (last csv)
           ["1996" "Jeep" "Grand Cherokee", "MUST SELL!\nair, moon roof, loaded" "4799.00"]))))


(deftest throw-if-quoted-on-eof
  (let [s "ab,\"de,gh\nij,kl,mn"]
    (try
      (doall (read-csv-compat s))
      (is false "No exception thrown")
      (catch Exception e
        (is (or (instance? java.io.EOFException e)
                (and (instance? RuntimeException e)
                     (instance? java.io.EOFException (.getCause e)))))))))

(deftest parse-line-endings
  (let [csv (read-csv-compat "Year,Make,Model\n1997,Ford,E350")]
    (is (= 2 (count csv)))
    (is (= ["Year" "Make" "Model"] (first csv)))
    (is (= ["1997" "Ford" "E350"] (second csv))))
  (let [csv (read-csv-compat "Year,Make,Model\r\n1997,Ford,E350")]
    (is (= 2 (count csv)))
    (is (= ["Year" "Make" "Model"] (first csv)))
    (is (= ["1997" "Ford" "E350"] (second csv))))
  (let [csv (read-csv-compat "Year,Make,Model\r1997,Ford,E350")]
    (is (= 2 (count csv)))
    (is (= ["Year" "Make" "Model"] (first csv)))
    (is (= ["1997" "Ford" "E350"] (second csv))))
  (let [csv (read-csv-compat "Year,Make,\"Model\"\r1997,Ford,E350")]
    (is (= 2 (count csv)))
    (is (= ["Year" "Make" "Model"] (first csv)))
    (is (= ["1997" "Ford" "E350"] (second csv)))))


(deftest trim-leading
  (let [header (first (read-csv-compat (java.io.File. "test/data/datatype_parser.csv")))]
    (is (= "word" (header 2))))
  (let [header (first (read-csv-compat (java.io.File. "test/data/datatype_parser.csv")
                                       :trim-leading-whitespace? false))]
    (is (= "   word" (header 2)))))


(deftest empty-file-test
  (let [data (seq (read-csv-compat (java.io.File. "test/data/emptyfile.csv")
                                   :column-whitelist ["firstcol"]))]
    (is (nil? data))))


(deftest whitelist-test
  (is (= [["char" "word"]
          ["t" "true"]
          ["f" "False"]
          ["y" "YES"]
          ["n" "NO"]
          ["T" "positive"]
          ["F" "negative"]
          ["Y" "yep"]
          ["N" "not"]
          ["A" "pos"]
          ["z" "neg"]]
         (vec (read-csv-compat (java.io.File. "test/data/datatype_parser.csv")
                               :column-whitelist ["char" "word"])))))
