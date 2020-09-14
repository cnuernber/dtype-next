(ns tech.v3.datatype.bitmap
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.clj-range :as clj-range]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.array-buffer])
  (:import [it.unimi.dsi.fastutil.longs LongSet LongIterator]
           [org.roaringbitmap RoaringBitmap ImmutableBitmapDataProvider]
           [tech.v3.datatype SimpleLongSet LongReader LongBitmapIter BitmapMap
            PrimitiveList PrimitiveIO]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [clojure.lang IFn LongRange]
           [java.lang.reflect Field]))


(set! *warn-on-reflection* true)


(def int-array-class (Class/forName "[I"))


(defn ensure-int-array
  ^ints [item]
  (when-not (instance? int-array-class item)
    (let [ary-buf (dtype-cmc/make-container :uint32 item)]
      (.ary-data ^ArrayBuffer ary-buf))))


(defn long-range->bitmap
  [^LongRange item]
  (let [long-reader (dtype-base/->reader item)
        step (long (.get ^Field clj-range/lr-step-field item))
        n-elems (.lsize long-reader)]
    (when-not (== 1 step)
      (throw (Exception.
              "Only monotonically incrementing ranges can be made into bitmaps")))
    (if (= 0 n-elems)
      (RoaringBitmap.)
      (let [start (.readLong long-reader 0)]
        (doto (RoaringBitmap.)
          (.add (unchecked-int start)
                (unchecked-int (+ start n-elems))))))))


(declare ->bitmap)


(extend-type RoaringBitmap
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [bitmap] :uint32)
  dtype-proto/PECount
  (ecount [bitmap] (.getLongCardinality bitmap))
  dtype-proto/PToReader
  (convertible-to-reader? [bitmap] true)
  (->reader [bitmap]
    (let [n-elems (dtype-base/ecount bitmap)]
      (reify
        LongReader
        (elemwiseDatatype [rdr] :uint32)
        (lsize [rdr] n-elems)
        (readLong [rdr idx] (Integer/toUnsignedLong (.select bitmap (int idx))))
        dtype-proto/PConstantTimeMinMax
        (has-constant-time-min-max? [rdr] true)
        (constant-time-min [rdr] (.first bitmap))
        (constant-time-max [rdr] (.last bitmap))
        dtype-proto/PToBitmap
        (convertible-to-bitmap? [item] true)
        (as-roaring-bitmap [item] bitmap)
        Iterable
        (iterator [rdr] (LongBitmapIter. (.getIntIterator bitmap))))))
  dtype-proto/PConstantTimeMinMax
  (has-constant-time-min-max? [bitmap] true)
  (constant-time-min [bitmap] (.first bitmap))
  (constant-time-max [bitmap] (.last bitmap))
  dtype-proto/PClone
  (clone [bitmap] (.clone bitmap))
  dtype-proto/PToBitmap
  (convertible-to-bitmap? [item] true)
  (as-roaring-bitmap [item] item)
  dtype-proto/PBitmapSet
  (set-and [lhs rhs] (RoaringBitmap/and lhs (->bitmap rhs)))
  (set-and-not [lhs rhs] (RoaringBitmap/andNot lhs (->bitmap rhs)))
  (set-or [lhs rhs] (RoaringBitmap/or lhs (->bitmap rhs)))
  (set-xor [lhs rhs] (RoaringBitmap/xor lhs (->bitmap rhs)))
  (set-offset [bitmap offset] (RoaringBitmap/addOffset bitmap (unchecked-int offset)))
  (set-add-range! [bitmap start end]
    (.add bitmap (unchecked-int start) (unchecked-int end))
    bitmap)
  (set-add-block! [bitmap data]
    (.add bitmap ^ints (ensure-int-array data))
    bitmap)
  (set-remove-range! [bitmap start end]
    (.remove bitmap (unchecked-int start) (unchecked-int end))
    bitmap)
  (set-remove-block! [bitmap data]
    (.remove bitmap ^ints (ensure-int-array data))
    bitmap))


(extend-type BitmapMap
  dtype-proto/PToBitmap
  (convertible-to-bitmap? [item] true)
  (as-roaring-bitmap [item] (.-slots item)))


(defmethod print-method RoaringBitmap
  [buf w]
  (let [^java.io.Writer w w]
    (.write w "#")
    (.write w (.toString ^Object buf))))


(defn bitmap->array-buffer
  ^ArrayBuffer [^RoaringBitmap bitmap]
  (dtype-base/->array-buffer (.toArray bitmap)))


(deftype BitmapSet [^RoaringBitmap bitmap]
  SimpleLongSet
  (getDatatype [item] :uin32)
  (lsize [item] (.getLongCardinality bitmap))
  (lcontains [item arg] (.contains bitmap (unchecked-int arg)))
  (ladd [item arg] (.add bitmap (unchecked-int arg)) true)
  (lremove [item arg] (.remove bitmap (unchecked-int arg)) true)
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item]
    (dtype-proto/->reader bitmap))
  dtype-proto/PClone
  (clone [item] (BitmapSet. (dtype-proto/clone bitmap)))
  dtype-proto/PConstantTimeMinMax
  (has-constant-time-min-max? [item] (not (.isEmpty bitmap)))
  (constant-time-min [item] (.first bitmap))
  (constant-time-max [item] (.last bitmap))
  dtype-proto/PToBitmap
  (convertible-to-bitmap? [item] true)
  (as-roaring-bitmap [item] bitmap)
  Iterable
  (iterator [item] (.iterator ^Iterable (dtype-proto/->reader bitmap)))
  dtype-proto/PBitmapSet
  (set-and [lhs rhs] (BitmapSet. (dtype-proto/set-and bitmap rhs)))
  (set-and-not [lhs rhs] (BitmapSet. (dtype-proto/set-and-not bitmap rhs)))
  (set-or [lhs rhs] (BitmapSet. (dtype-proto/set-or bitmap rhs)))
  (set-xor [lhs rhs] (BitmapSet. (dtype-proto/set-xor bitmap rhs)))
  (set-offset [item offset] (BitmapSet. (dtype-proto/set-offset bitmap offset)))
  (set-add-range! [item start end]
    (dtype-proto/set-add-range! bitmap start end)
    item)
  (set-add-block! [item data]
    (dtype-proto/set-add-block! item data)
    item)
  (set-remove-range! [item start end]
    (dtype-proto/set-remove-range! bitmap start end)
    item)
  (set-remove-block! [item data]
    (dtype-proto/set-remove-block! item data)
    item))


(defn ->bitmap
  (^RoaringBitmap [item]
   (cond
     (nil? item)
     (RoaringBitmap.)
     (dtype-proto/convertible-to-bitmap? item)
     (dtype-proto/as-roaring-bitmap item)
     (instance? LongRange item)
     (long-range->bitmap item)
     :else
     (let [ary-buf (dtype-cmc/->array-buffer :uint32 item)]
       (doto (RoaringBitmap.)
         (.addN ^ints (.ary-data ary-buf)
                (int (.offset ary-buf))
                (int (.n-elems ary-buf)))))))
  (^RoaringBitmap []
   (RoaringBitmap.)))


(defn ->unique-bitmap
  (^RoaringBitmap [item]
   (if (dtype-proto/convertible-to-bitmap? item)
     (.clone ^RoaringBitmap (dtype-proto/as-roaring-bitmap item))
     (->bitmap item)))
  (^RoaringBitmap []
   (RoaringBitmap.)))


(defn bitmap->efficient-random-access-reader
  [bitmap]
  (when (dtype-proto/convertible-to-bitmap? bitmap)
    (let [^RoaringBitmap bitmap (dtype-proto/as-roaring-bitmap bitmap)
          typed-buf (bitmap->array-buffer bitmap)
          src-reader (dtype-base/->reader typed-buf)
          n-elems (dtype-base/ecount typed-buf)]
      (if (== 0 n-elems)
        src-reader
        (let [cmin (dtype-proto/constant-time-min bitmap)
              cmax (dtype-proto/constant-time-max bitmap)]
          (reify
            LongReader
            (lsize [rdr] n-elems)
            (readLong [rdr idx] (.readLong src-reader idx))
            dtype-proto/PToBitmap
            (convertible-to-bitmap? [item] true)
            (as-roaring-bitmap [item] bitmap)
            dtype-proto/PConstantTimeMinMax
            (has-constant-time-min-max? [item] true)
            (constant-time-min [item] cmin)
            (constant-time-max [item] cmax)))))))


(defn bitmap-value->bitmap-map
  "Given a bitmap and a value return an efficient implementation of
  clojure.lang.IPersistentMap that has the given value at the given indexes."
  [bitmap value]
  (BitmapMap. (->bitmap bitmap) value))


(deftype BitmapPrimitiveList [^RoaringBitmap bitmap
                              ^:unsynchronized-mutable ^PrimitiveIO cached-io]
  PrimitiveList
  (ensureCapacity [this cap])
  (addLong [this arg]
    (.add bitmap (unchecked-int arg))
    (set! cached-io nil))
  (addDouble [this arg]
    (.addLong this (unchecked-long arg)))
  (addObject [this arg]
    (.addLong this (long arg)))
  (addAll [this other]
    (if (instance? BitmapPrimitiveList other)
      (.or bitmap (.bitmap ^BitmapPrimitiveList other))
      (parallel-for/doiter
       value other
       (.add bitmap (unchecked-int value))))
    true)
  (lsize [this] (.getCardinality bitmap))
  (readBoolean [this idx] (dtype-proto/->reader this) (.readBoolean cached-io idx))
  (readByte [this idx] (dtype-proto/->reader this) (.readByte cached-io idx))
  (readShort [this idx] (dtype-proto/->reader this) (.readShort cached-io idx))
  (readChar [this idx] (dtype-proto/->reader this) (.readChar cached-io idx))
  (readInt [this idx] (dtype-proto/->reader this) (.readInt cached-io idx))
  (readLong [this idx] (dtype-proto/->reader this) (.readLong cached-io idx))
  (readFloat [this idx] (dtype-proto/->reader this) (.readFloat cached-io idx))
  (readDouble [this idx] (dtype-proto/->reader this) (.readDouble cached-io idx))
  (readObject [this idx] (dtype-proto/->reader this) (.readObject cached-io idx))
  dtype-proto/PToReader
  (convertible-to-reader? [this] true)
  (->reader [this]
    (when-not cached-io
      (set! cached-io (dtype-proto/->reader bitmap)))
    cached-io)
  dtype-proto/PToBitmap
  (convertible-to-bitmap? [item] true)
  (as-roaring-bitmap [item] bitmap))


(defn bitmap-as-primitive-list
  (^PrimitiveList [bitmap]
   (let [bitmap (dtype-proto/as-roaring-bitmap bitmap)]
     (BitmapPrimitiveList. bitmap nil)))
  (^PrimitiveList []
   (bitmap-as-primitive-list (RoaringBitmap.))))
