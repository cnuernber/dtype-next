(ns tech.v3.datatype.ffi.clang
  "General utilites to work with clang in order to define things like structs or
  enumerations.

  "
  (:require [tech.v3.datatype.ffi.size-t :as ffi-size-t]
            [tech.v3.datatype.struct :as dt-struct]
            [tech.v3.datatype.errors :as errors]
            [camel-snake-kebab.core :as csk]
            [clojure.string :as s]))


;;Exactly three spaces, else we are into the inner stuff.
(def ^:private line-regex #"\s+(\d+)\s\|\s\s\s(\w.+)")
(def ^:private ary-regex #"\[(\d+)\]")


(def ^:private simple-type-table
  {"int8_t" :int8
   "char" :int8
   "uint8_t" :uint8
   "int16_t" :int16
   "uint16_t" :uint16
   "int32_t" :int32
   "uint32_t" :uint32
   "int64_t" :int64
   "uint64_t" :uint64
   "float" :float32
   "double" :float64})


(defn- line-struct-dec
  [^String str-line failed-line-parser]
  ;;Thankfully clang outputs datatypes in a semi-normalized format.
  (let [line-split (s/split str-line #"\s+")
        member-name (csk/->kebab-case-keyword (last line-split))
        n-elems (if-let [ary-len (re-find ary-regex str-line)]
                  (Integer/parseInt (second ary-len))
                  1)]
    (merge
     {:name member-name
      :n-elems n-elems}
     (cond
       (.contains str-line "(*)(")
       {:datatype (ffi-size-t/ptr-t-type)}
       (.contains str-line "*")
       (merge {:datatype (ffi-size-t/ptr-t-type)})
       (.startsWith str-line "struct")
       {:datatype (->> (s/split str-line #"\s+")
                       (second)
                       (csk/->kebab-case-keyword))}
       (.startsWith str-line "enum")
       {:datatype :int32}
       :else
       (let [ltype (first line-split)
             [ltype unsigned?] (if (= ltype "unsigned")
                                 [(second line-split) true]
                                 [ltype false])
             line-dtype (if-let [stype (get simple-type-table ltype)]
                          stype
                          (cond
                            (= ltype "size_t")
                            (ffi-size-t/size-t-type)
                            (or (= ltype "int") (= ltype "long"))
                            (if unsigned?
                              :uint32
                              :int32)
                            :else
                            (if failed-line-parser
                              (failed-line-parser str-line)
                              (errors/throwf "Unrecognized line: %s" str-line))))]
         {:datatype line-dtype})))))


(defn- dump->struct-members
  [dump-data failed-line-parser]
  (->> (s/split dump-data #"\n+")
       (map (fn [line-str]
              (when-let [grps (re-matches line-regex line-str)]
                (let [[_ offset typedec] grps]
                  (merge (line-struct-dec typedec failed-line-parser)
                         {:calculated-offset (Integer/parseInt offset)})))))
       (remove nil?)))


(defn- check-struct-def
  [struct-def layout failed-line-parser]
  (let [members (dump->struct-members layout failed-line-parser)
        dtype-members (:data-layout struct-def)]
    (->>
     (map (fn [smem dmem]
            (when-not (== (long (:calculated-offset smem))
                          (long (:offset dmem)))
              (errors/throwf "mismatch - expected %s got %s"
                             smem dmem)))
          members dtype-members)
     (dorun))
    struct-def))


(defn defstruct-from-layout
    "Function to take a partial clang record dump and make a struct defintion.
  In order to generate a file containing dumps we do something like:

```console
clang avcodec.cpp -I/usr/include/x86_64-linux-gnu/libavcodec -Xclang -fdump-record-layouts > recordlayouts64.txt
```
  Note the `-fdump-record-layouts` argument.

  Then we take the record layouts and save them somewhere we can get to them at
  code generation time.

  Example:

```clojure
(def packet-layout
  \"     0 |   AVBufferRef * buf
         8 |   int64_t pts
        16 |   int64_t dts
        24 |   uint8_t * data
        32 |   int size
        36 |   int stream_index
        40 |   int flags
        48 |   AVPacketSideData * side_data
        56 |   int side_data_elems
        64 |   int64_t duration
        72 |   int64_t pos
        80 |   int64_t convergence_duration\")

(def packet-def* (delay (ffi-clang/defstruct-from-layout :av-packet packet-layout)))
```"
  ([struct-dtype layout {:keys [failed-line-parser]}]
   (let [struct-def
         (dt-struct/define-datatype!
           struct-dtype
           (dump->struct-members layout failed-line-parser))]
     (check-struct-def struct-def layout failed-line-parser)))
  ([struct-dtype layout]
   (defstruct-from-layout struct-dtype layout nil)))
