package tech.v3.datatype;


import java.util.Iterator;
import java.io.EOFException;

public class CharReader
{
  public final Iterator buffers;
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
    if (buffers.hasNext()) {
      curBuffer = (char[])buffers.next();
      curPos = 0;
      buflen = curBuffer.length;
    } else {
      curBuffer = null;
      curPos = -1;
      buflen = 0;
    }
  }

  public CharReader(Iterator _buf, char _quot, char _sep) {
    buffers = _buf;
    nextBuffer();
    quot = _quot;
    sep = _sep;
  }

  public CharReader(Iterator _buf) {
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

  public final long csvRead(StringBuilder sb) {
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

  public final long csvReadQuote(StringBuilder sb) throws EOFException {
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

}
