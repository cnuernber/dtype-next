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
  public final boolean isspace(char val) {
    return val == ' ' || val == '\t';
  }
  public final void append(char val) {
    if(!trimLeading ||
       len != 0 ||
       !isspace(val)) {
      if (len == buffer.length) {
	char[] newbuffer = new char[buffer.length * 2];
	System.arraycopy(buffer, 0, newbuffer, 0, len);
	buffer = newbuffer;
      }
      buffer[len] = val;
      ++len;
    }
  }
  public final void clear() { len = 0; }
  public final int length() { return len; }
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
