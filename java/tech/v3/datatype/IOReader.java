package tech.v3.datatype;

import java.io.Reader;

public interface IOReader {
  int doRead(char[] cbuf, int off, int len);
  void doClose();
  default Reader makeReader(Object lock) {
    return new Reader (lock) {
      public int read(char[] cbuf, int off, int len) {
	if (lock != null) {
	    synchronized(lock) {
	      return doRead(cbuf, off, len);
	    }
	  } else {
	  return doRead(cbuf, off, len);
	}
      }
      public void close() { doClose(); }
    };
  }
}
