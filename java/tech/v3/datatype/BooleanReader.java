package tech.v3.datatype;

import clojure.lang.Keyword;
import java.util.stream.IntStream;


public interface BooleanReader extends PrimitiveReader
{
  boolean read(long idx);
  default boolean readBoolean(long idx) {return read(idx);}
  default byte readByte(long idx) {return (byte) (read(idx) ? 1 : 0);}
  default short readShort(long idx) {return (short) (read(idx) ? 1 : 0);}
  default char readChar(long idx) {return (char) (read(idx) ? 1 : 0);}
  default int readInt(long idx) {return (int) (read(idx) ? 1 : 0);}
  default long readLong(long idx) {return (long) (read(idx) ? 1 : 0);}
  default float readFloat(long idx) {return (float) (read(idx) ? 1 : 0);}
  default double readDouble(long idx) {return (double) (read(idx) ? 1 : 0);}
  default Object readObject(long idx) {return read(idx);}
  default Object elemwiseDatatype () {return Keyword.intern(null, "boolean");}
  default IntStream typedStream() {
    return intStream();
  }
};
