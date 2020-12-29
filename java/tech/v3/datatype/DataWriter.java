package tech.v3.datatype;


public interface DataWriter extends ElemwiseDatatype, ECount {
  void writeBytes(byte[] data, int offset, int length);
  default void writeBytes(byte[] data) {
    writeBytes(data, 0, data.length);
  }
  void writeData(Object data);
}
