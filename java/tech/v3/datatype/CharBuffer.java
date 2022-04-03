package tech.v3.datatype;


public final class CharBuffer
{
  public final boolean trimLeading;
  public final boolean trimTrailing;
  public final boolean nilEmpty;
  char[] buffer;
  int len;

  public CharBuffer(boolean _trimLeading, boolean _trimTrailing, boolean _nilEmpty) {
    trimLeading = _trimLeading;
    trimTrailing = _trimTrailing;
    nilEmpty = _nilEmpty;
    buffer = new char[32];
    len = 0;
  }
  public CharBuffer() {
    this(false, false, false);
  }
  public final boolean isspace(char val) {
    return val == ' ' || val == '\t';
  }
  public final void ensureCapacity(int newlen) {
    if (newlen >= buffer.length) {
      char[] newbuffer = new char[newlen * 2];
      System.arraycopy(buffer, 0, newbuffer, 0, len);
      buffer = newbuffer;
    }
  }
  public final void append(char val) {
    if(!trimLeading ||
       len != 0 ||
       !isspace(val)) {
      ensureCapacity(len+1);
      buffer[len] = val;
      ++len;
    }
  }
  public final void append(char[] data, int startoff, int endoff) {
    int soff = startoff;
    if(trimLeading && len == 0) {
      for(; soff < endoff && isspace(data[soff]); ++soff );
    }
    if(soff != endoff) {
      int nchars = endoff - soff;
      int newlen = len + nchars;
      ensureCapacity(newlen);
      for(; soff < endoff; ++soff, ++len) {
	buffer[len] = data[soff];
      }
    }
  }
  public final void clear() { len = 0; }
  public char[] buffer() { return buffer; }
  public final int length() { return len; }
  public final int capacity() { return buffer.length; }
  public final String toString() {
    int strlen = len;
    if(len != 0 && trimTrailing) {
      int idx = len - 1;
      for (; idx >= 0 && isspace(buffer[idx]); --idx);
      strlen = idx + 1;
    }
    if(strlen == 0) {
      if(nilEmpty) {
	return null;
      }
      return "";
    } else {
      return new String(buffer, 0, strlen);
    }
  }
}
