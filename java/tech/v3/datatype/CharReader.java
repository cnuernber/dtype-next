package tech.v3.datatype;


import java.util.Iterator;
import java.util.ArrayList;
import java.util.function.Consumer;
import clojure.lang.IFn;


public final class CharReader implements AutoCloseable
{
  IFn buffers;
  char[] curBuffer;
  int curPos;

  public static class SingletonFn implements IFnDef {
    Object data;
    SingletonFn(Object _d) {
      data = _d;
    }
    public Object invoke() {
      Object retval = data;
      data = null;
      return retval;
    }
  }

  public CharReader(IFn _buf) {
    buffers = _buf;
    nextBuffer();
  }

  public CharReader(char[] data) {
    this(new SingletonFn(data));
  }

  public CharReader(String data) {
    this(data.toCharArray());
  }

  public final char[] buffer() { return curBuffer; }
  public final int position() { return curPos; }
  public final void position(int pos) { curPos = pos; }
  public final int bufferLength() {
    return curBuffer != null ? curBuffer.length : 0;
  }
  public final int remaining() {
    return bufferLength() - curPos;
  }
  public final char eatwhite() {
    char[] buffer = curBuffer;
    while(buffer != null) {
      final int len = buffer.length;
      int pos = curPos;
      for(; pos < len && Character.isWhitespace(buffer[pos]); ++pos);
      if (pos < len) {
	final char retval = buffer[pos];
	position(pos+1);
	return retval;
      }
      buffer = nextBuffer();
    }
    return 0;
  }
  public boolean eof() { return curBuffer == null; }
  public final char[] nextBuffer() {
    char[] nextbuf;
    if (buffers != null)
      nextbuf = (char[])buffers.invoke();
    else
      nextbuf = null;

    if (nextbuf != null) {
      curBuffer = nextbuf;
      curPos = 0;
    } else {
      curBuffer = null;
      curPos = -1;
    }
    return nextbuf;
  }

  public final int read() {
    //common case
    if (remaining() > 0) {
      char retval = curBuffer[curPos];
      ++curPos;
      return retval;
    } else {
      nextBuffer();
      if (eof())
	return -1;
      return read();
    }
  }
  public final int read(char[] buffer, int off, int len) {
    //common case first
    int leftover = len;
    char[] srcBuf = curBuffer;
    while(leftover > 0 && srcBuf != null) {
      final int pos = curPos;
      final int rem = srcBuf.length - pos;
      int readlen = Math.min(rem,leftover);
      System.arraycopy(srcBuf, pos, buffer, off, readlen);
      off += readlen;
      leftover -= readlen;
      if (leftover == 0) {
	curPos = pos + readlen;
	return len;
      }
      nextBuffer();
      srcBuf = buffer;
    }
    return -1;
  }

  public final void unread() {
    --curPos;
    //Unread only works until you reach the beginning of the current buffer.  If you just
    //loaded a new buffer and read a char and then have to unread more than one character
    //then we need to allocate a new buffer and reset curPos and such.  Will implement
    //if needed.
    if (curPos < 0) {
      throw new RuntimeException("Unread too far - current buffer empty");
    }
  }

  final int readFrom(int pos) {
    curPos = pos;
    return read();
  }

  public void close() throws Exception {
    if (buffers != null) {
      if (buffers instanceof AutoCloseable) {
	((AutoCloseable)buffers).close();
      }
      buffers = null;
    }
  }
}
