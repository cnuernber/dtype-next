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

  public CharReader(IFn _buf) {
    buffers = _buf;
    nextBuffer();
  }

  public final char[] buffer() { return curBuffer; }
  public final int position() { return curPos; }
  public final void position(int pos) { curPos = pos; }
  public final int bufferLength() {
    if (curBuffer != null)
      return curBuffer.length;
    return 0;
  };
  public final int remaining() {
    return bufferLength() - curPos;
  }

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
    if (remaining() <= 0)
      nextBuffer();

    if (curBuffer == null)
      return -1;
    char retval = curBuffer[curPos];
    ++curPos;
    return retval;
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
