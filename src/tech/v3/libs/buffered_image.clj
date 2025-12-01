(ns tech.v3.libs.buffered-image
  "Bindings to buffered images for the datatype system"
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.tensor :as dtt]
            [tech.v3.tensor.dimensions :as dims]
            [clojure.java.io :as io]
            [clojure.set :as c-set])
  (:import [java.awt.image BufferedImage
            DataBufferByte DataBufferDouble DataBufferFloat
            DataBufferInt DataBufferShort DataBufferUShort
            DataBuffer]
           [java.awt RenderingHints Graphics2D]
           [java.io InputStream]
           [tech.v3.datatype LongBuffer]
           [javax.imageio ImageIO])
  (:refer-clojure :exclude [load]))

(set! *warn-on-reflection* true)


(def ^{:doc "Mapping from keywords to integer buffered image types"}
  image-types
  {:byte-bgr BufferedImage/TYPE_3BYTE_BGR
   :byte-abgr BufferedImage/TYPE_4BYTE_ABGR
   :byte-abgr-pre BufferedImage/TYPE_4BYTE_ABGR_PRE
   :byte-binary BufferedImage/TYPE_BYTE_BINARY
   :byte-gray BufferedImage/TYPE_BYTE_GRAY
   :byte-indexed BufferedImage/TYPE_BYTE_INDEXED
   :custom BufferedImage/TYPE_CUSTOM
   :int-argb BufferedImage/TYPE_INT_ARGB
   :int-argb-pre BufferedImage/TYPE_INT_ARGB_PRE
   :int-bgr BufferedImage/TYPE_INT_BGR
   :int-rgb BufferedImage/TYPE_INT_RGB
   :ushort-555-rgb BufferedImage/TYPE_USHORT_555_RGB
   :ushort-565-rgb BufferedImage/TYPE_USHORT_565_RGB
   :ushort-gray BufferedImage/TYPE_USHORT_GRAY})


(def ^{:doc "Mapping from buffered image type to keyword"
       :private true} image-enum->image-type-map (c-set/map-invert image-types))


(defn image-type
  "Get the image type of a buffered image as a keyword."
  [^BufferedImage img]
  (get image-enum->image-type-map (.getType img)))


(defn image-channel-format
  "Get the image channel format of the buffered image.  Formats returned may be:
  :gray :bgr :rgb :abgr :argb :abgr-pre :argb-pre"
  [img]
  (case (image-type img)
    :byte-bgr :bgr
    :byte-abgr :abgr
    :byte-abgr-pre :abgr-pre
    :byte-gray :gray
    :int-argb :bgra
    :int-argb-pre :bgra-pre
    :int-bgr :rgb
    :int-rgb :bgr
    :ushort-555-rgb :rgb
    :ushort-565-rgb :rgb
    :ushort-gray :gray))


(defn image-channel-map
  "Get a map from keyword channel name to channel index in a ubyte tensor"
  [img]
  (case (image-type img)
    :byte-bgr {:b 0 :g 1 :r 2}
    :byte-abgr {:a 0 :b 1 :g 2 :r 3}
    :byte-abgr-pre {:a 0 :b 1 :g 2 :r 3}
    :byte-gray {:gray 0}
    :int-argb {:b 0 :g 1 :r 2 :a 3}
    :int-argb-pre {:b 0 :g 1 :r 2 :a 3}
    :int-bgr {:r 0 :g 1 :b 2}
    :int-rgb {:b 0 :g 1 :r 2}))


(def ^:private data-buffer-types
  {:uint8 DataBuffer/TYPE_BYTE
   :float64 DataBuffer/TYPE_DOUBLE
   :float32 DataBuffer/TYPE_FLOAT
   :int32 DataBuffer/TYPE_INT
   :int16 DataBuffer/TYPE_SHORT
   :unknown DataBuffer/TYPE_UNDEFINED
   :uint16 DataBuffer/TYPE_USHORT})


(def ^:private data-buffer-type-enum->type-map (c-set/map-invert data-buffer-types))


(defprotocol PDataBufferAccess
  (data-buffer-banks [item]))


(extend-type DataBuffer
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [item]
    (get data-buffer-type-enum->type-map (.getDataType item)))

  dtype-proto/PECount
  (ecount [item]
    (long (* (.getSize item)
             (.getNumBanks item))))

  dtype-proto/PShape
  (shape [item]
    (if (> (.getNumBanks item) 1)
      [(.getNumBanks item) (.getSize item)]
      [(.getSize item)]))

  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [item]
    (= 1 (.getNumBanks item)))
  (->array-buffer [item]
    (when (dtype-proto/convertible-to-array-buffer? item)
      (-> (data-buffer-banks item)
          first
          (array-buffer/array-buffer (dtype-base/elemwise-datatype item))
          (dtype-proto/sub-buffer (.getOffset item) (.getSize item))))))


(extend-type DataBufferByte
  PDataBufferAccess
  (data-buffer-banks [item] (.getBankData item)))

(extend-type DataBufferShort
  PDataBufferAccess
  (data-buffer-banks [item] (.getBankData item)))

(extend-type DataBufferUShort
  PDataBufferAccess
  (data-buffer-banks [item] (.getBankData item)))

(extend-type DataBufferInt
  PDataBufferAccess
  (data-buffer-banks [item] (.getBankData item)))

(extend-type DataBufferFloat
  PDataBufferAccess
  (data-buffer-banks [item] (.getBankData item)))

(extend-type DataBufferDouble
  PDataBufferAccess
  (data-buffer-banks [item] (.getBankData item)))


(defn buffered-image->data-buffer
  "Given a buffered image, return it's data buffer."
  ^DataBuffer [^BufferedImage img]
  (.. img getRaster getDataBuffer))

(defn img->guess-at-channel-n
  [img]
  ;; If this throws, we may need to know more about the input image
  (.getNumBands (.getRaster img)))

(extend-type BufferedImage
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [item]
    (dtype-proto/elemwise-datatype (buffered-image->data-buffer item)))
  dtype-proto/PECount
  (ecount [item]
    (dtype-proto/ecount
     (buffered-image->data-buffer item)))
  dtype-proto/PShape
  (shape [item]
    (let [height (.getHeight item)
          width (.getWidth item)]
      (case (image-type item)
        :byte-bgr [height width 3]
        :byte-abgr [height width 4]
        :byte-abgr-pre [height width 4]
        :byte-gray [height width 1]
        :custom [height width (img->guess-at-channel-n item)]
        [height width 1])))
  dtype-proto/PClone
  (clone [item]
    (dtype-cmc/copy! item
                     (BufferedImage. (.getWidth item) (.getHeight item)
                                     (.getType item))))
  dtype-proto/PToTensor
  (as-tensor [item]
    (dtt/construct-tensor
     (dtype-proto/->array-buffer item)
     (dims/dimensions (dtype-base/shape item))))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [item]
    (dtype-proto/convertible-to-array-buffer?
     (buffered-image->data-buffer item)))
  (->array-buffer [item]
    (dtype-proto/->array-buffer
     (buffered-image->data-buffer item))))


(deftype PackedIntUbyteBuffer [int-buffer n-elems shape n-channels]
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [_item] :uint8)
  dtype-proto/PECount
  (ecount [_item] n-elems)
  dtype-proto/PShape
  (shape [_item] shape)
  dtype-proto/PClone
  (clone [_item]
    (PackedIntUbyteBuffer. (dtype-proto/clone int-buffer)
                           n-elems
                           shape
                           n-channels))
  dtype-proto/PToTensor
  (as-tensor [item] (dtt/construct-tensor
                     item
                     (dims/dimensions (dtype-base/shape item))))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [_item] true)
  (->buffer [item]
    (let [n-channels (long n-channels)
          src-io (dtype-base/->buffer int-buffer)]
      (reify LongBuffer
        (elemwiseDatatype [rdr] :uint8)
        (lsize [rdr] n-elems)
        (allowsRead [rdr] true)
        (allowsWrite [rdr] true)
        (readLong [rdr idx]
          (let [pix-idx (quot idx n-channels)
                chan-idx (rem idx n-channels)
                src-data (.readLong src-io pix-idx)]
            (case chan-idx
              0 (bit-and 0xFF src-data)
              1 (-> (bit-and src-data 0xFF00)
                    (bit-shift-right 8))
              2 (-> (bit-and src-data 0xFF0000)
                    (bit-shift-right 16))
              3 (-> (bit-and src-data 0xFF000000)
                    (bit-shift-right 24)))))
        (writeLong [rdr idx value]
          (let [pix-idx (quot idx n-channels)
                chan-idx (rem idx n-channels)
                bit-mask (long (case chan-idx
                                 0 0xFF
                                 1 0xFF00
                                 2 0xFF0000
                                 3 0xFF000000))
                shift-amount (long (case chan-idx
                                     0 0
                                     1 8
                                     2 16
                                     3 24))
                value (unchecked-int
                       (-> (casting/datatype->cast-fn :int16 :uint8 value)
                           (bit-shift-left shift-amount)))]
            (locking item
              (.writeLong src-io pix-idx
                      (bit-or (bit-and (.readLong src-io pix-idx)
                                       (bit-not bit-mask))
                              value)))))))))


(defn as-ubyte-tensor
  "Get the buffered image as a uint8 tensor.  Works for byte and integer-buffer
  backed images."
  [^BufferedImage img]
  (let [img-width (.getWidth img)
        img-height (.getHeight img)]
    (->
     (case (image-type img)
       :byte-bgr img
       :byte-abgr img
       :byte-abgr-pre img
       :byte-gray img
       :int-argb (PackedIntUbyteBuffer. (buffered-image->data-buffer img)
                                        (* img-width img-height 4)
                                        [img-height img-width 4]
                                        4)
       :int-argb-pre (PackedIntUbyteBuffer. (buffered-image->data-buffer img)
                                            (* img-width img-height 4)
                                            [img-height img-width 4]
                                            4)
       :int-bgr (PackedIntUbyteBuffer. (buffered-image->data-buffer img)
                                       (* img-width img-height 3)
                                       [img-height img-width 3]
                                       3)
       :int-rgb (PackedIntUbyteBuffer. (buffered-image->data-buffer img)
                                       (* img-width img-height 3)
                                       [img-height img-width 3]
                                       3))
     (dtt/ensure-tensor))))


(defn new-image
  "Create a new buffered image.  img-type is a keyword and must be one of the
  keys in the image-types map.
  ** Arguments are reverse of the buffered image constructor **"
  ^BufferedImage [height width img-type]
  (if-let [buf-img-type (get image-types img-type)]
    (BufferedImage. width height buf-img-type)
    (throw (Exception. "Unrecognized image type: %s" img-type))))


(defn load
  "Load an image.  There are better versions of this in tech.io"
  ^BufferedImage [fname-or-stream]
  (with-open [istream (io/input-stream fname-or-stream)]
    (ImageIO/read ^InputStream istream)))


(defn save!
  "Save an image.  Format-str can be things like \"PNG\" or \"JPEG\".
  There are better versions of this in tech.io."
  ([^BufferedImage img ^String format-str fname-or-stream]
   (with-open [ostream (io/output-stream fname-or-stream)]
     (ImageIO/write img format-str ostream)))
  ([img ^String fname-str]
   (let [format-str (.substring fname-str (inc (.lastIndexOf fname-str ".")))]
     (save! img format-str fname-str))))


(def ^{:doc "Map of keyword to buffered image rendering hint."}
  interpolation-types
  {:bilinear RenderingHints/VALUE_INTERPOLATION_BILINEAR
   :cubic RenderingHints/VALUE_INTERPOLATION_BICUBIC
   :nearest RenderingHints/VALUE_INTERPOLATION_NEAREST_NEIGHBOR})


(defn draw-image!
  "Draw a source image onto a destination image.  This can be used for scaling,
  cropping, or copying images."
  [^BufferedImage src-img ^BufferedImage dst-image
   & {:keys [src-x-offset src-y-offset
             src-rect-width src-rect-height
             dst-x-offset dst-y-offset
             dst-rect-width dst-rect-height
             interpolation-type]}]
  (let [dst-x-offset (long (or dst-x-offset 0))
        dst-y-offset (long (or dst-y-offset 0))
        src-x-offset (long (or src-x-offset 0))
        src-y-offset (long (or src-y-offset 0))
        min-width (min (.getWidth src-img) (.getWidth dst-image))
        min-height (min (.getHeight src-img) (.getHeight dst-image))
        src-rect-width (long (or src-rect-width min-width))
        src-rect-height (long (or src-rect-height min-height))
        dst-rect-width (long (or dst-rect-width min-width))
        dst-rect-height (long (or dst-rect-height min-height))
        g (.getGraphics dst-image)]
    (when interpolation-type
      (if-let [inter-num (get interpolation-types interpolation-type)]
        (.setRenderingHint ^Graphics2D g
                           RenderingHints/KEY_INTERPOLATION
                           inter-num)
        (throw (Exception. (format "Invalid interpolation type: %s"
                                   interpolation-type)))))
    (doto g
      (.drawImage src-img dst-x-offset dst-y-offset
                  (+ dst-rect-width dst-x-offset)
                  (+ dst-rect-height dst-y-offset)
                  src-x-offset src-y-offset
                  (+ src-rect-width src-x-offset)
                  (+ src-rect-height src-y-offset)
                  nil)
      (.dispose))
    dst-image))


(defn downsample-bilinear
  "Reduce an image size using bilinear filtering.  This is a buffered-image->buffered->image
  transformation."
  ^BufferedImage [^BufferedImage src-img & {:keys [dst-img-width
                                                   dst-img-height
                                                   dst-img-type]}]
  (let [src-img-width (.getWidth src-img)
        src-img-height (.getHeight src-img)
        dst-img-width (long (or dst-img-width
                                (quot src-img-width 2)))
        dst-img-height (long (or dst-img-height
                                 (quot src-img-height 2)))
        dst-img-type (or dst-img-type (image-type src-img))
        resized (new-image dst-img-height dst-img-width dst-img-type)]
    (draw-image! src-img resized {:src-rect-width src-img-width
                                  :src-rect-height src-img-height
                                  :dst-rect-width dst-img-width
                                  :dst-rect-height dst-img-height}
                 :interpolation-type :bilinear)))


(defn resize
  "Resize an image.
  Options -
    * resize-algorithm - One of #{:bilinear :cubic :nearest}.
      Defaults to - if the new width is larger than then old width, bilinear is chosen
      else nearest is chosen.
    * dst-img-type - Defaults to the src image type.  Should be one of the keys of image-types."
  [src-img new-width new-height {:keys [resize-algorithm
                                        dst-img-type]}]
  (let [[src-height src-width _n-channels] (dtype-base/shape src-img)
        retval (new-image new-height new-width
                          (or dst-img-type
                              (image-type src-img)))
         resize-algorithm (or resize-algorithm
                              (if (> (int new-width)
                                     (int src-width))
                                :bilinear
                                :nearest))]
    (draw-image! src-img retval
                 {:src-rect-width src-width
                  :src-rect-height src-height
                  :dst-rect-width new-width
                  :dst-rect-height new-height
                  :interpolation-type resize-algorithm})))

(defn clone
  "Clone an image into a new buffered image."
  [src-img]
  (dtype-proto/clone src-img))


(defmethod dtype-proto/make-container :buffered-image
  [_container-type datatype _options img-shape]
  (when-not (= datatype :uint8)
    (throw (Exception. "Only uint8 datatype allowed")))
  (when-not (= 3 (count img-shape))
    (throw (Exception. "Images must have 3 dimensions [h w c]")))
  (let [[height width channels] img-shape
        channels (long channels)]
    (when-not (or (= channels 3)
                  (= channels 4)
                  (= channels 1))
      (throw (Exception. "Byte images only support 1,3, or 4 channels.")))
    (new-image height width (case channels
                              1 :byte-gray
                              3 :byte-bgr
                              4 :byte-abgr))))


(defn tensor->image
  "Convert a tensor directly to a buffered image.  The values will be interpreted as unsigned
  bytes in the range of 0-255.

  Options:
  * `:img-type` - Force a particular type of image type - see keys of [[image-types]]."
  (^BufferedImage [t options]
   (let [t (dtt/ensure-tensor t)
         dims (dtype-proto/shape t)
         [y x c] (case (count dims)
                   2 [(first dims) (second dims) 1]
                   3 dims
                   (throw (RuntimeException. "Unrecognized tensor shape for image conversion - " dims)))
         img-type (or (get options :img-type)
                      (case (long c)
                        1 :byte-gray
                        3 :byte-bgr
                        4 :byte-abgr))
         dst (new-image y x img-type)]
     (dtype-cmc/copy! t (as-ubyte-tensor dst))
     dst))
  (^BufferedImage [t] (tensor->image t nil)))
