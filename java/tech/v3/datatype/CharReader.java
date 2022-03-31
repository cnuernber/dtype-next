package tech.v3.datatype;


import java.util.Iterator;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.function.Consumer;
import clojure.lang.IFn;


public final class CharReader implements AutoCloseable
{
  public final IFn buffers;

  final char quot;
  final char sep;
  //there were a few others but outside entities only need 1.
  public static final int EOF=-1;
  public static final int EOL=-2;
  public static final int SEP=1;
  public static final int QUOT=2;
  char[] curBuffer;
  int curPos;
  int buflen;

  final void nextBuffer() {
    char[] nextbuf = (char[])buffers.invoke();
    if (nextbuf != null) {
      curBuffer = nextbuf;
      curPos = 0;
      buflen = curBuffer.length;
    } else {
      curBuffer = null;
      curPos = -1;
      buflen = 0;
    }
  }

  public CharReader(IFn _buf, char _quot, char _sep) {
    buffers = _buf;
    nextBuffer();
    quot = _quot;
    sep = _sep;
  }

  public CharReader(IFn _buf) {
    this(_buf, '"', ',');
  }

  public final int read() {
    if (curPos >= buflen)
      nextBuffer();

    if (curBuffer == null)
      return -1;
    char retval = curBuffer[curPos];
    ++curPos;
    return retval;
  }
  public final void unread() {
    --curPos;
  }

  final int readFrom(int pos) {
    curPos = pos;
    return read();
  }

  final void csvReadQuote(CharBuffer sb) throws EOFException {
    while(curBuffer != null) {
      char[] buffer = curBuffer;
      int len = buffer.length;
      for(int pos = curPos; pos < len; ++pos) {
	final char curChar = buffer[pos];
	if (curChar != quot) {
	  sb.append(curChar);
	} else {
	  if (readFrom(pos+1) == quot) {
	    sb.append(quot);
	    buffer = curBuffer;
	    len = buffer.length;
	    //account for loop increment
	    pos = curPos - 1;
	  } else {
	    unread();
	    return;
	  }
	}
      }
      nextBuffer();
    }
    throw new EOFException("EOF encounted within quote");
  }
  //Read a row from a CSV file.
  final int csvRead(CharBuffer sb) throws EOFException {
    while(curBuffer != null) {
      final char[] buffer = curBuffer;
      final int len = buffer.length;
      for(int pos = curPos; pos < len; ++pos) {
	final char curChar = buffer[pos];
	if (curChar == quot) {
	  curPos = pos + 1;
	  return QUOT;
	} else if (curChar == sep) {
	  curPos = pos + 1;
	  return SEP;
	} else if (curChar == '\n') {
	  curPos = pos + 1;
	  return EOL;
	} else if (curChar == '\r') {
	  if (readFrom(pos+1) != '\n') {
	    unread();
	  }
	  return EOL;
	} else {
	  sb.append(curChar);
	}
      }
      nextBuffer();
    }
    return EOF;
  }

  public void close() throws Exception {
    if (buffers instanceof AutoCloseable) {
      ((AutoCloseable)buffers).close();
    }
  }

  public static final class RowReader
  {
    final CharReader rdr;
    final CharBuffer sb;
    final ArrayList row;
    UnaryPredicate pred;

    public RowReader(CharReader _r, CharBuffer _sb, ArrayList _al, UnaryPredicate _pred) {
      rdr = _r;
      sb = _sb;
      row = _al;
      pred = _pred;
    }
    public void setPredicate(UnaryPredicate p) { pred = p; }
    public static final boolean emptyStr(String s) {
      return s == null || s.length() == 0;
    }
    public final boolean emptyRow() {
      int sz = row.size();
      return sz == 0 || (sz == 1 && emptyStr((String)row.get(0)));
    }
    public final ArrayList currentRow() { return row; }
    public final ArrayList nextRow() throws EOFException {
      row.clear();
      sb.clear();
      int tag;
      int colidx = 0;
      final UnaryPredicate p = pred;
      do {
	tag = rdr.csvRead(sb);
	if(tag != QUOT) {
	  if (p.unaryLong(colidx))
	    row.add(sb.toString());
	  ++colidx;
	  sb.clear();
	} else if (tag == QUOT) {
	  rdr.csvReadQuote(sb);
	}
      } while(tag > 0);
      if (!(tag == EOF && emptyRow()))
	return row;
      else
	return null;
    }
  }
}
