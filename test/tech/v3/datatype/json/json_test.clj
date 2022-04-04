(ns tech.v3.datatype.json.json-test
  (:require [tech.v3.datatype.char-input :as char-input]
            [clojure.test :refer :all]
            [clojure.string :as str]))

(deftest read-from-pushback-reader
  (is (= 42 (char-input/read-json "42"))))

(deftest read-from-reader
  (let [s (java.io.StringReader. "42")]
    (is (= 42 (char-input/read-json s)))))

(deftest read-numbers
  (is (= 42 (char-input/read-json "42")))
  (is (= -3 (char-input/read-json "-3")))
  (is (= 3.14159 (char-input/read-json "3.14159")))
  (is (= 6.022e23 (char-input/read-json "6.022e23"))))

(deftest read-bigint
  (is (= 123456789012345678901234567890N
         (char-input/read-json "123456789012345678901234567890"))))


(deftest read-bigdec
  (is (= 3.14159M (char-input/read-json "3.14159" :bigdec true))))

(deftest read-null
  (is (= nil (char-input/read-json "null"))))

(deftest read-strings
  (is (= "Hello, World!" (char-input/read-json "\"Hello, World!\""))))

(deftest escaped-slashes-in-strings
  (is (= "/foo/bar" (char-input/read-json "\"\\/foo\\/bar\""))))

(deftest unicode-escapes
  (is (= " \u0beb " (char-input/read-json "\" \\u0bEb \""))))

(deftest escaped-whitespace
  (is (= "foo\nbar" (char-input/read-json "\"foo\\nbar\"")))
  (is (= "foo\rbar" (char-input/read-json "\"foo\\rbar\"")))
  (is (= "foo\tbar" (char-input/read-json "\"foo\\tbar\""))))

(deftest read-booleans
  (is (= true (char-input/read-json "true")))
  (is (= false (char-input/read-json "false"))))

(deftest ignore-whitespace
  (is (= nil (char-input/read-json "\r\n   null"))))

(deftest read-arrays
  (is (= (vec (range 35))
         (char-input/read-json "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34]")))
  (is (= ["Ole" "Lena"] (char-input/read-json "[\"Ole\", \r\n \"Lena\"]"))))

(deftest read-objects
  (is (= {:k1 1, :k2 2, :k3 3, :k4 4, :k5 5, :k6 6, :k7 7, :k8 8
          :k9 9, :k10 10, :k11 11, :k12 12, :k13 13, :k14 14, :k15 15, :k16 16}
         (char-input/read-json "{\"k1\": 1, \"k2\": 2, \"k3\": 3, \"k4\": 4,
                          \"k5\": 5, \"k6\": 6, \"k7\": 7, \"k8\": 8,
                          \"k9\": 9, \"k10\": 10, \"k11\": 11, \"k12\": 12,
                          \"k13\": 13, \"k14\": 14, \"k15\": 15, \"k16\": 16}"
                        :key-fn keyword))))

(deftest read-nested-structures
  (is (= {:a [1 2 {:b [3 "four"]} 5.5]}
         (char-input/read-json "{\"a\":[1,2,{\"b\":[3,\"four\"]},5.5]}"
                        :key-fn keyword))))

(deftest read-nested-structures-stream
  (is (= {:a [1 2 {:b [3 "four"]} 5.5]}
         (char-input/read-json (java.io.StringReader. "{\"a\":[1,2,{\"b\":[3,\"four\"]},5.5]}")
                    :key-fn keyword))))

(deftest reads-long-string-correctly
  (let [long-string (str/join "" (take 100 (cycle "abcde")))]
    (is (= long-string (char-input/read-json (str "\"" long-string "\""))))))

(deftest disallows-non-string-keys
  (is (thrown? Exception (char-input/read-json "{26:\"z\""))))

(deftest disallows-barewords
  (is (thrown? Exception (char-input/read-json "  foo  "))))

(deftest disallows-unclosed-arrays
  (is (thrown? Exception (char-input/read-json "[1, 2,  "))))

(deftest disallows-unclosed-objects
  (is (thrown? Exception (char-input/read-json "{\"a\":1,  "))))

(deftest disallows-empty-entry-in-object
  (is (thrown? Exception (char-input/read-json "{\"a\":1,}")))
  (is (thrown? Exception (char-input/read-json "{\"a\":1, }")))
  (is (thrown? Exception (char-input/read-json "{\"a\":1,,,,}")))
  (is (thrown? Exception (char-input/read-json "{\"a\":1,,\"b\":2}"))))

(deftest get-string-keys
  (is (= {"a" [1 2 {"b" [3 "four"]} 5.5]}
         (char-input/read-json "{\"a\":[1,2,{\"b\":[3,\"four\"]},5.5]}"))))

(deftest keywordize-keys
  (is (= {:a [1 2 {:b [3 "four"]} 5.5]}
         (char-input/read-json "{\"a\":[1,2,{\"b\":[3,\"four\"]},5.5]}"
                    :key-fn keyword))))

(deftest convert-values
  (is (= {:number 42 :date (java.sql.Date. 55 6 12)}
         (char-input/read-json "{\"number\": 42, \"date\": \"1955-07-12\"}"
                    :key-fn keyword
                    :value-fn (fn [k v]
                                (if (= :date k)
                                  (java.sql.Date/valueOf v)
                                  v))))))

(deftest omit-values
  (is (= {:number 42}
         (char-input/read-json "{\"number\": 42, \"date\": \"1955-07-12\"}"
                    :key-fn keyword
                    :value-fn (fn thisfn [k v]
                                (if (= :date k)
                                  thisfn
                                  v))))))

(declare pass1-string)

(deftest pass1-test
  (let [input (char-input/read-json pass1-string)]
    (is (= "JSON Test Pattern pass1" (first input)))
    (is (= "array with 1 element" (get-in input [1 "object with 1 member" 0])))
    (is (= 1234567890 (get-in input [8 "integer"])))
    (is (= "rosebud" (last input)))))

; from http://www.json.org/JSON_checker/test/pass1.json
(def pass1-string
     "[
    \"JSON Test Pattern pass1\",
    {\"object with 1 member\":[\"array with 1 element\"]},
    {},
    [],
    -42,
    true,
    false,
    null,
    {
        \"integer\": 1234567890,
        \"real\": -9876.543210,
        \"e\": 0.123456789e-12,
        \"E\": 1.234567890E+34,
        \"\":  23456789012E66,
        \"zero\": 0,
        \"one\": 1,
        \"space\": \" \",
        \"quote\": \"\\\"\",
        \"backslash\": \"\\\\\",
        \"controls\": \"\\b\\f\\n\\r\\t\",
        \"slash\": \"/ & \\/\",
        \"alpha\": \"abcdefghijklmnopqrstuvwyz\",
        \"ALPHA\": \"ABCDEFGHIJKLMNOPQRSTUVWYZ\",
        \"digit\": \"0123456789\",
        \"0123456789\": \"digit\",
        \"special\": \"`1~!@#$%^&*()_+-={':[,]}|;.</>?\",
        \"hex\": \"\\u0123\\u4567\\u89AB\\uCDEF\\uabcd\\uef4A\",
        \"true\": true,
        \"false\": false,
        \"null\": null,
        \"array\":[  ],
        \"object\":{  },
        \"address\": \"50 St. James Street\",
        \"url\": \"http://www.JSON.org/\",
        \"comment\": \"// /* <!-- --\",
        \"# -- --> */\": \" \",
        \" s p a c e d \" :[1,2 , 3

,

4 , 5        ,          6           ,7        ],\"compact\":[1,2,3,4,5,6,7],
        \"jsontext\": \"{\\\"object with 1 member\\\":[\\\"array with 1 element\\\"]}\",
        \"quotes\": \"&#34; \\u0022 %22 0x22 034 &#x22;\",
        \"\\/\\\\\\\"\\uCAFE\\uBABE\\uAB98\\uFCDE\\ubcda\\uef4A\\b\\f\\n\\r\\t`1~!@#$%^&*()_+-=[]{}|;:',./<>?\"
: \"A key can be any string\"
    },
    0.5 ,98.6
,
99.44
,

1066,
1e1,
0.1e1,
1e-1,
1e00,2e+00,2e-00
,\"rosebud\"]")


(defn- double-value [_ v]
  (if (and (instance? Double v)
           (or (.isNaN ^Double v)
               (.isInfinite ^Double v)))
    (str v)
    v))

(deftest default-throws-on-eof
  (is (thrown? java.io.EOFException (char-input/read-json ""))))

(deftest throws-eof-in-unterminated-array
  (is (thrown? java.io.EOFException
        (char-input/read-json "[1, "))))

(deftest throws-eof-in-unterminated-string
  (is (thrown? java.io.EOFException
        (char-input/read-json "\"missing end quote"))))

(deftest throws-eof-in-escaped-chars
  (is (thrown? java.io.EOFException
        (char-input/read-json "\"\\"))))

(deftest accept-eof
  (is (= ::eof (char-input/read-json "" :eof-error? false :eof-value ::eof))))
