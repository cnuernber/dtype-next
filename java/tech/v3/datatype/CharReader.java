package tech.v3.datatype;


import java.util.Iterator;
import java.io.EOFException;
import java.util.ArrayList;
import clojure.lang.IFn;

public final class CharReader implements AutoCloseable
{
  public final IFn buffers;
  public static final char lf = '\n';
  public static final char cr = '\r';

  public final char quot;
  public final char sep;
  public static final long EOF=-1;
  public static final long QUOT=1;
  public static final long SEP=2;
  public static final long EOL=3;
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

  public final long csvRead(CharBuffer sb) {
    while(curBuffer != null) {
      for(; curPos < buflen; ++curPos) {
	final char curChar = curBuffer[curPos];
	if (curChar == quot) {
	  ++curPos;
	  return QUOT;
	} else if (curChar == sep) {
	  ++curPos;
	  return SEP;
	} else if (curChar == lf) {
	  ++curPos;
	  return EOL;
	} else if (curChar == cr) {
	  ++curPos;
	  if (read() != lf) {
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

  public final long csvReadQuote(CharBuffer sb) throws EOFException {
    while(curBuffer != null) {
      for(; curPos < buflen; ++curPos) {
	final char curChar = curBuffer[curPos];
	if (curChar == quot) {
	  ++curPos;
	  if (read() == quot) {
	    --curPos;
	    sb.append(quot);
	  } else {
	    unread();
	    return csvRead(sb);
	  }
	} else {
	  sb.append(curChar);
	}
      }
      nextBuffer();
    }
    throw new EOFException("EOF encounted within quote");
  }
  public void close() throws Exception {
    if (buffers instanceof AutoCloseable) {
      ((AutoCloseable)buffers).close();
    }
  }

  public static final class RowReader
  {
    public final CharReader rdr;
    public final CharBuffer sb;
    public final ArrayList row;
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
    public final long nextTag(long tag) throws EOFException {
      if (tag == QUOT)
	return rdr.csvReadQuote(sb);
      return rdr.csvRead(sb);
    }
    public final ArrayList nextRow() throws EOFException {
      row.clear();
      int colidx = 0;
      long tag = SEP;
      for(tag = nextTag(tag);
	  tag != EOL && tag != EOF;
	  tag = nextTag(tag)) {
	if (tag == SEP) {
	  if (pred.unaryLong(colidx))
	    row.add(sb.toString());
	  sb.clear();
	  ++colidx;
	}
      }
      if (pred.unaryLong(colidx))
	row.add(sb.toString());
      sb.clear();
      if (!(tag == EOF && emptyRow()))
	return row;
      return null;
    }
  }
}
