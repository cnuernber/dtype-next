(ns tech.v3.compute.driver
  "Base set of protocols required to move information from the host to the device as well as
  enable some form of computation on a given device.  There is a cpu implementation provided for
  reference.

  Base datatypes are defined:
   * Driver: Enables enumeration of devices and creation of host buffers.
   * Device: Creates streams and device buffers.
   * Stream: Stream of execution occuring on the device.
   * Event: A synchronization primitive emitted in a stream to notify other
            streams that might be blocking.")


(defprotocol PDriverProvider
  "Get a driver from an object"
  (get-driver [impl]))


(defprotocol PDeviceProvider
  "Get a device from an object."
  (get-device [impl]))


(defprotocol PStreamProvider
  "Get a stream from an object"
  (get-stream [impl]))


(defprotocol PDriver
  "A driver is a generic compute abstraction.  Could be a group of threads, could be a
  machine on a network or it could be a CUDA or OpenCL driver.  A stream is a stream of
  execution (analogous to a thread) where subsequent calls are serialized.  All buffers
  implement a few of the datatype interfaces, at least get-datatype and ecount.  Host
  buffers are expected to implement enough of the datatype interfaces to allow a copy
  operation from generic datatypes into them.  This means at least PAccess."
  (driver-name [driver]
    "A system-unique name for this driver.  The keyworded namespace it is implemented in
    is ideal.")
  (get-devices [driver]
    "Get a list of devices accessible to the system.")
  (allocate-host-buffer [driver elem-count elem-type options]
    "Allocate a host buffer.  Transfer from host to device requires data first copied
into a host buffer and then uploaded to a device buffer.
options:
:usage-type
usage-type: #{:one-time :reusable}
  Hint to allow implementations to allocate different types of host buffers each
  optimized for the desired use case.  Default is one-time.")
  (acceptable-host-buffer? [driver buffer]
    "Will this buffer work as a host buffer?"))

(defonce host-buffer-usage-types #{:one-time :reusable})


(defprotocol PDevice
  (supports-create-stream? [device]
    "Does this device support create-stream?")
  (default-stream [device]
    "All devices must have a default stream whether they support create or not.")
  (create-stream [device]
    "Create a stream of execution.  Streams are indepenent threads of execution.  They
can be synchronized with each other and the main thread using events.")
  (allocate-device-buffer [device elem-count elem-type options]
    "Allocate a device buffer.  This is the generic unit of data storage used for
computation.  No options at this time.")
  (device->device-copy-compatible? [src-device dst-device]
    "When two devices differ, it may be possible to copy from src to dest device.")
  (acceptable-device-buffer? [device item]
    "Do whatever checks necessary to ensure that this item can be used as a device
buffer for this device."))


(defprotocol PStream
  "Basic functionality expected of streams.  Streams are an abstraction of a stream of
  execution and can be synchonized with the host or with each other using events.  CPU's
  are considered devices."
  (copy-device->device [stream dev-a dev-a-off dev-b dev-b-off elem-count]
    "copy from one device to another.  Used to initiate host->device, device->host,
host->host and device->device copies with memcpy semantics.")
  (sync-with-host [stream]
    "Block host until stream's queue is finished executing")
  (sync-with-stream [src-stream dst-stream]
    "Create an event in src-stream's execution queue, then have dst stream wait on
that event.  This allows dst-stream to ensure src-stream has reached a certain point
of execution before continuing.  Both streams must be of the same driver."))
