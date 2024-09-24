(ns tech.v3.datatype.ffi-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.ffi :as dt-ffi]
            [tech.v3.datatype.ffi.clang :as ffi-clang]
            [tech.v3.datatype.struct :as dt-struct]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.nio-buffer]
            [clojure.test :refer [deftest is]]
            [clojure.tools.logging :as log])
  (:import [tech.v3.datatype.ffi Pointer]))


(defn generic-define-library
  []
  (let [libmem-def (dt-ffi/define-library
                     ;;function definitions
                     {:memset {:rettype :pointer
                               :argtypes [['buffer :pointer]
                                          ['byte-value :int32]
                                          ['n-bytes :size-t]]}
                      :memcpy {:rettype :pointer
                               ;;dst src size-t
                               :argtypes [['dst :pointer]
                                          ['src :pointer]
                                          ['n-bytes :size-t]]}
                      :qsort {:rettype :void
                              :argtypes [['data :pointer]
                                         ['nitems :size-t]
                                         ['item-size :size-t]
                                         ['comparator :pointer]]}}
                     ;;symbol definitions
                     [:memmove]
                     ;;no extra options
                     nil)
        ;;nil meaning find the symbols in the current process
        libmem-inst (dt-ffi/instantiate-library libmem-def nil)
        libmem-fns @libmem-inst
        memcpy (:memcpy libmem-fns)
        memset (:memset libmem-fns)
        qsort (:qsort libmem-fns)
        comp-iface-def (dt-ffi/define-foreign-interface :int32 [:pointer :pointer])
        comp-iface-inst (dt-ffi/instantiate-foreign-interface
                         comp-iface-def
                         (fn [^Pointer lhs ^Pointer rhs]
                           (let [lhs (.getDouble (native-buffer/unsafe) (.address lhs))
                                 rhs (.getDouble (native-buffer/unsafe) (.address rhs))]
                             (Double/compare lhs rhs))))
        comp-iface-ptr (dt-ffi/foreign-interface-instance->c
                        comp-iface-def
                        comp-iface-inst)
        first-buf (dtype/make-container :native-heap :float32 (range 10))
        second-buf (dtype/make-container :native-heap :float32 (range 10))
        dbuf (dtype/make-container :native-heap :float64 (shuffle (range 100)))]
    (memset first-buf 0 40)
    (memcpy second-buf first-buf 40)
    (qsort dbuf (dtype/ecount dbuf) Double/BYTES comp-iface-ptr)
    (is (dfn/equals first-buf (vec (repeat 10 0.0))))
    (is (dfn/equals second-buf (vec (repeat 10 0.0))))
    (is (dfn/equals dbuf (range 100)))
    (is (= (.findSymbol libmem-inst "qsort")
           (.findSymbol libmem-inst "qsort")))
    (is (not= (.findSymbol libmem-inst "memmove")
              (.findSymbol libmem-inst "qsort")))))


(deftest jna-ffi-test
  (dt-ffi/set-ffi-impl! :jna)
  (generic-define-library))

(defn nested-byvalue
  []
  (let [anon1 (dt-struct/define-datatype! :anon1 [{:name :a :datatype :int32}
                                                  {:name :b :datatype :float64}])
        anon2 (dt-struct/define-datatype! :anon2 [{:name :c :datatype :float64}
                                                  {:name :d :datatype :int32}])
        bv-type (dt-struct/define-datatype! :by-value [{:name :abcd :datatype :int32}
                                                       {:name :first-struct :datatype :anon1}
                                                       {:name :second-struct :datatype :anon2}])
        bv (dt-struct/new-struct :by-value {:container-type :native-heap})
        ^java.util.Map a1 (get bv :first-struct)
        ^java.util.Map a2 (get bv :second-struct)
        _ (do 
            (.put bv :abcd 10)
            (.put a1 :a 5)
            (.put a1 :b 4.0)
            (.put a2 :c 3.0)
            (.put a2 :d 9))
        bv-lib-def (dt-ffi/define-library
                     ;;function definitions
                     {:byvalue_nested {:rettype '(by-value :by-value)
                                       :argtypes [[bv '(by-value :by-value)]]}}
                     nil nil)
        lib (dt-ffi/instantiate-library bv-lib-def (str (System/getProperty "user.dir")
                                                        "/test/cpp/libffi_test.so"))
        lib-fns @lib
        test-fn (get lib-fns :byvalue_nested)
        bbv (test-fn bv)]
    (is (and (= (dt-struct/struct->clj bv)
                (dt-struct/struct->clj bbv))))))

(comment
  (nested-byvalue)
  )

(deftest jna-byvalue-test
  (dt-ffi/set-ffi-impl! :jna)
  (nested-byvalue))


(if (dt-ffi/jdk-ffi?)
  (deftest jdk-byvalue-test
    (dt-ffi/set-ffi-impl! :jdk-21)
    (nested-byvalue))
  (log/warn "JDK-21 FFI pathway not tested."))


(deftest library-instance-test
  (let [library-def* (atom {:memset {:rettype :pointer
                                     :argtypes [['buffer :pointer]
                                                ['byte-value :int32]
                                                ['n-bytes :size-t]]
                                     :doc "set memory to value"}})
        singleton (dt-ffi/library-singleton library-def*)]
    ;;set-library! hasn't been called
    (is (thrown? Exception (dt-ffi/library-singleton-find-fn singleton :memset)))
    ;;Users calls initialize!
    (dt-ffi/library-singleton-set! singleton nil)
    ;;does some repl exploration
    (is (dt-ffi/library-singleton-find-fn singleton :memset))
    (is (thrown? Exception (dt-ffi/library-singleton-find-fn singleton :memcpy)))
    ;;Decides they need a new function.  They recompile the namespace thus generating
    ;;a new var value (but not a new var).
    (reset! library-def* {:memset {:rettype :pointer
                                   :argtypes [['buffer :pointer]
                                              ['byte-value :int32]
                                              ['n-bytes :size-t]]
                                   :doc "set memory to value"}
                          :memcpy {:rettype :pointer
                                   ;;dst src size-t
                                   :argtypes [['dst :pointer]
                                              ['src :pointer]
                                              ['n-bytes :size-t]]}})
    ;;Reset rebuilds the library and generates new functions
    (dt-ffi/library-singleton-reset! singleton)
    ;;Everything works
    (is (dt-ffi/library-singleton-find-fn singleton :memset))
    (is (dt-ffi/library-singleton-find-fn singleton :memcpy))))


(def frame-layout
  "      0 |   uint8_t *[8] data
        64 |   int [8] linesize
        96 |   uint8_t ** extended_data
       104 |   int width
       108 |   int height
       112 |   int nb_samples
       116 |   int format
       120 |   int key_frame
       124 |   enum AVPictureType pict_type
       128 |   struct AVRational sample_aspect_ratio
       128 |     int num
       132 |     int den
       136 |   int64_t pts
       144 |   int64_t pkt_pts
       152 |   int64_t pkt_dts
       160 |   int coded_picture_number
       164 |   int display_picture_number
       168 |   int quality
       176 |   void * opaque
       184 |   uint64_t [8] error
       248 |   int repeat_pict
       252 |   int interlaced_frame
       256 |   int top_field_first
       260 |   int palette_has_changed
       264 |   int64_t reordered_opaque
       272 |   int sample_rate
       280 |   uint64_t channel_layout
       288 |   AVBufferRef *[8] buf
       352 |   AVBufferRef ** extended_buf
       360 |   int nb_extended_buf
       368 |   AVFrameSideData ** side_data
       376 |   int nb_side_data
       380 |   int flags
       384 |   enum AVColorRange color_range
       388 |   enum AVColorPrimaries color_primaries
       392 |   enum AVColorTransferCharacteristic color_trc
       396 |   enum AVColorSpace colorspace
       400 |   enum AVChromaLocation chroma_location
       408 |   int64_t best_effort_timestamp
       416 |   int64_t pkt_pos
       424 |   int64_t pkt_duration
       432 |   AVDictionary * metadata
       440 |   int decode_error_flags
       444 |   int channels
       448 |   int pkt_size
       456 |   int8_t * qscale_table
       464 |   int qstride
       468 |   int qscale_type
       472 |   AVBufferRef * qp_table_buf
       480 |   AVBufferRef * hw_frames_ctx
       488 |   AVBufferRef * opaque_ref
       496 |   size_t crop_top
       504 |   size_t crop_bottom
       512 |   size_t crop_left
       520 |   size_t crop_right
       528 |   AVBufferRef * private_ref")


(deftest struct-layout
  (let [rational-def (dt-struct/define-datatype!
                       :av-rational [{:name :num :datatype :int32}
                                     {:name :den :datatype :int32}])
        struct-def (ffi-clang/defstruct-from-layout :av-frame frame-layout)]
    (is (= 54 (count (:data-layout struct-def))))))
