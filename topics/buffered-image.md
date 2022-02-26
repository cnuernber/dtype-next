# dtype-next Buffered Image Support


`dtype-next` contains support for loading/saving buffered images
and creating tensors from buffered images.


## Usage


Buffered images implement the protocols required to be part of the datatype system.

### Basics



```clojure
user> (require '[tech.v3.libs.buffered-image :as bufimg])
nil
user> (bufimg/load "https://raw.githubusercontent.com/cnuernber/dtype-next/master/test/data/test.jpg")
#object[java.awt.image.BufferedImage 0x579ce3c2 "BufferedImage@579ce3c2: type = 5 ColorModel: #pixelBits = 24 numComponents = 3 color space = java.awt.color.ICC_ColorSpace@5bd9ef6d transparency = 1 has alpha = false isAlphaPre = false ByteInterleavedRaster: width = 512 height = 288 #numDataElements 3 dataOff[0] = 2"]
user> (def test-img *1)
#'user/test-img
user> (require '[tech.v3.datatype :as dtype])
nil
user> (dtype/elemwise-datatype test-img)
:uint8
user> (dtype/shape test-img)
[288 512 3]
user> (bufimg/image-channel-format test-img)
:bgr
user> (require '[tech.v3.tensor :as dtt])
nil

;;Tensors implement a set of interfaces such as Indexed and IFn.  In most cases those interfaces
;;allow you to either read or write to a subrect of the tensor.

user> (def tens (dtt/ensure-tensor test-img))
user> ;;nth indexes into the first dimension.
user> (nth tens 0)
#tech.v3.tensor<uint8>[512 3]
[[172 170 170]
 [172 170 170]
 [171 169 169]
 ...
 [ 24  18  23]
 [ 24  18  23]
 [ 24  18  23]]
user> ;;IFn indexes into provided dimensions
user> (tens 0)
#tech.v3.tensor<uint8>[512 3]
[[172 170 170]
 [172 170 170]
 [171 169 169]
 ...
 [ 24  18  23]
 [ 24  18  23]
 [ 24  18  23]]
user> (tens 0 0)
#tech.v3.tensor<uint8>[3]
[172 170 170]
user> (tens 0 0 0)
172
```

* One important thing to note is the return value of ensure-tensor shares the backing
data store with the source object.  So you can write to the tensor with any of
the datatype methods and the result will be written into the buffered image.

```clojure
user> ;;mget, mset! allow you to retrieve/assign values to subrects (or scalars)
user> (dtt/mget tens 0 0)
#tech.v3.tensor<uint8>[3]
[172 170 170]
user> (dtt/mset! tens 0 0 0 255)
#tech.v3.tensor<uint8>[288 512 3]
[[[255 170 170]
  [172 170 170]
  [171 169 169]
  ...
  [ 24  18  23]
  [ 24  18  23]
  [ 24  18  23]]
  ...
user> (dtt/mget (dtt/ensure-tensor test-img) 0 0 0)
255
user> (dtt/mset! tens 0 0 [255 245 235])
#tech.v3.tensor<uint8>[288 512 3]
[[[255 245 235]
  [172 170 170]
  [171 169 169]
  ...
  [ 24  18  23]
  [ 24  18  23]
  [ 24  18  23]]
```


### Different Image Types


```clojure

;;If you have images of other base storage types you may get a different tensor than
;;you want:

user> (vec (keys bufimg/image-types))
[:int-bgr
 :byte-gray
 :byte-binary
 :ushort-gray
 :ushort-555-rgb
 :int-argb-pre
 :byte-indexed
 :custom
 :byte-bgr
 :byte-abgr-pre
 :int-rgb
 :ushort-565-rgb
 :byte-abgr
 :int-argb]


user> (def test-img (bufimg/new-image 4 4 :int-bgr))
#'user/test-img
user> (dtt/ensure-tensor test-img)
#tech.v3.tensor<int32>[4 4 1]
[[[0]
  [0]
  [0]
  [0]]
 [[0]
  [0]
  [0]
  [0]]
 [[0]
  [0]
  [0]
  [0]]
 [[0]
  [0]
  [0]
  [0]]]

;;So we have a convenience method that will always return a uint8 tensor:

user> (bufimg/as-ubyte-tensor test-img)
#tech.v3.tensor<uint8>[4 4 3]
[[[0 0 0]
  [0 0 0]
  [0 0 0]
  [0 0 0]]
 [[0 0 0]
  [0 0 0]
  [0 0 0]
  [0 0 0]]
 [[0 0 0]
  [0 0 0]
  [0 0 0]
  [0 0 0]]
 [[0 0 0]
  [0 0 0]
  [0 0 0]
  [0 0 0]]]
```

### Tensor Operations

All the tensor operations return data in-place.  So in the transpose call below a view
is returned without actually doing any copies.  Iterating over tensors iterates
over the outermost dimension returning a sequence of tensors or a sequence of numbers
if this is a one dimension tensor.


Our image is in BGR-interleaved format, so what we first do is transpose the image into
BGR planar format.  We then use the statistical methods in datatype in order to get
the per-channel statistics for the image.

```clojure

;;Having images be tensors is useful for a few things, but stats is one of them.
user> (def test-img (bufimg/load "https://raw.githubusercontent.com/cnuernber/dtype-next/master/test/data/test.jpg"))
#'user/test-img
user> (def planar-tens (dtt/transpose test-img [2 0 1]))
#'user/planar-tens
user> planar-tens
#tech.v3.tensor<uint8>[3 288 512]
[[[172 172 171 ... 24 24 24]
  [173 174 173 ... 23 23 23]
  [174 175 174 ... 23 22 22]
  ...
  [ 37  37  37 ... 55 54 55]
  [ 36  35  35 ... 52 50 53]
  [ 39  37  34 ... 51 49 53]]
 [[170 170 169 ... 18 18 18]
  [171 172 171 ... 17 17 17]
  [172 173 172 ... 17 16 16]
  ...
  [ 46  46  46 ... 51 50 51]
  [ 45  44  44 ... 51 49 52]
  [ 48  46  43 ... 50 48 52]]
 [[170 170 169 ... 23 23 23]
  [171 172 171 ... 22 22 22]
  [172 173 172 ... 22 21 21]
  ...
  [ 84  84  83 ... 56 55 56]
  [ 83  82  81 ... 55 53 56]
  [ 86  84  80 ... 54 52 56]]]


user> (map dfn/descriptive-statistics planar-tens)
({:min 0.0,
  :mean 97.47846137152777,
  :standard-deviation 60.73587071027433,
  :max 248.0,
  :n-values 147456}
 {:min 5.0,
  :mean 101.00441487630208,
  :standard-deviation 58.55138215866232,
  :max 255.0,
  :n-values 147456}
 {:min 5.0,
  :mean 113.43905978732639,
  :standard-deviation 57.797302391956244,
  :max 255.0,
  :n-values 147456})
```

### Cropping/Resizing Images

For simple resize operations, we provide a resize convenience function that uses the
buffered image graphics canvas to render a resized image into another image.

Cropping can be done 2 ways.  The draw-image! method can crop or you can select
regions of the images using the tensor api and then us the datatype copy! operation.

```clojure
;;Drawing images work well in order to copy parts of one:
(def new-img (bufimg/new-image 512 512 :int-argb))
#'user/new-img
user> (bufimg/draw-image! test-img new-img :dst-y-offset 128)
#object[java.awt.image.BufferedImage 0x1ec0f94c "BufferedImage@1ec0f94c: type = 2 DirectColorModel: rmask=ff0000 gmask=ff00 bmask=ff amask=ff000000 IntegerInterleavedRaster: width = 512 height = 512 #Bands = 4 xOff = 0 yOff = 0 dataOffset[0] 0"]
```


### API Reference



* `load` - load an image.  clojure.java.io/input-stream is called on the fname.
* `new-image` - Create a new image.  Arguments are in row-major format:
   height,width,img-type.
* `image-type` - Returns the image type of the image.
* `image-channel-map` - Returns a map of the channel names to indexes when the image
   is interpreted as a uint8 tensor.
* `as-ubyte-tensor` - interpret image as a uint8 tensor.  Works for byte and int image    types.
* `draw-image!` - Draw a source image onto a dest image.  You can specify the
   source/dest rectangles and an interpolation method when resizing.
* `downsample-bilinear` - Convenience method around `draw-image`.  Default is to halve
  the height and width of the image.
* `resize` - Convenience method that auto-determines the interpolation type based on
  source/dest width ratio.
* `clone` - Clone an image.  Simply calls `tech.v2.datatype/clone`.
* `save!` - save the image.  You can specify a format optionally aside from the fname
   or it will be inferred from the part of the fname following the last '.'.


It is important to note that all of the datatype methods work on the image - `ecount`,
`shape`, `elemwise-datatype`, `copy!`, `set-constant!`.  All of the tensor methods work on
the image - `ensure-tensor`, `select`, `reshape`, `transpose`.  You can create a
tensor reader or writer from the image to get fully typed access to it in
height,width,channel as demonstrated above.
