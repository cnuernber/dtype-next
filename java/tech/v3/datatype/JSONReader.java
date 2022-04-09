package tech.v3.datatype;


import java.io.EOFException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Function;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.math.BigDecimal;
import java.math.BigInteger;
import clojure.lang.IFn;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashMap;
import clojure.lang.LazilyPersistentVector;
import clojure.lang.Keyword;
import clojure.lang.ITransientVector;
import clojure.lang.ITransientMap;
import clojure.lang.PersistentVector;


public final class JSONReader implements AutoCloseable {
  CharReader reader;
  public interface ObjReader {
    public Object newObj();
    public Object onKV(Object obj, Object k, Object v);
    public default Object finalizeObj(Object obj) { return obj; }
  };
  public static final ObjReader immutableObjReader = new ObjReader() {
      public Object newObj() { return PersistentHashMap.EMPTY.asTransient(); }
      public Object onKV(Object obj, Object k, Object v) {
	return ((ITransientMap)obj).assoc(k, v);
      }
      public Object finalizeObj(Object obj) {
	return ((ITransientMap)obj).persistent();
      }
    };
  public static final ObjReader mutableObjReader = new ObjReader() {
      public Object newObj() { return new HashMap<Object,Object>(); }
      public Object onKV(Object obj, Object k, Object v) {
	((HashMap<Object,Object>)obj).put(k,v);
	return obj;
      }
    };
  public static class JSONObj {
    public final ArrayList<Object> data;
    public JSONObj(ArrayList<Object> d) { data = d; }
  }
  public static final ObjReader rawObjReader = new ObjReader() {
      public Object newObj() { return new ArrayList<Object>(8); }
      public Object onKV(Object obj, Object k, Object v) {
	final ArrayList<Object> ary = (ArrayList<Object>)obj;
	ary.add(k);
	ary.add(v);
	return ary;
      }
      public Object finalizeObj(Object obj) {
	return new JSONObj((ArrayList<Object>)obj);
      }
    };

  public interface ArrayReader {
    public Object newArray();
    public Object onValue(Object ary, Object v);
    public default Object finalizeArray(Object ary) { return ary; }
  };
  public static final ArrayReader immutableArrayReader = new ArrayReader() {
      public Object newArray() { return PersistentVector.EMPTY.asTransient(); }
      public Object onValue(Object ary, Object v) {
        return ((ITransientVector)ary).conj(v);
      }
      public Object finalizeArray(Object obj) {
	return ((ITransientVector)obj).persistent();
      }
    };
  public static final ArrayReader mutableArrayReader = new ArrayReader() {
      public Object newArray() { return new ArrayList<Object>(8); }
      public Object onValue(Object ary, Object v) {
	((ArrayList<Object>)ary).add(v);
	return ary;
      }
    };
  //Parsing specialization functions
  public final Function<String,Object> doubleFn;
  public final Supplier<Object> eofFn;
  public final ObjReader objReader;
  public final ArrayReader aryReader;
  //We only need one temp buffer for various string building activities
  final CharBuffer charBuffer = new CharBuffer();
  //A temp buffer for reading out fixed sequences of characters
  //final char[] tempBuf = new char[8];
  public static final Function<String,Object> defaultDoubleParser = data -> Double.parseDouble(data);
  public static final Supplier<Object> defaultEOFFn = () -> { throw new RuntimeException("EOF encounted while reading stream."); };
  public static final <T> T orDefault(T val, T defVal) { return val != null ? val : defVal; }

  public static final Keyword elidedValue = Keyword.intern("tech.v3.datatype.char-input", "elided");

  public JSONReader(Function<String,Object> _doubleFn,
		    ArrayReader _aryReader,
		    ObjReader _objReader,
		    Supplier<Object> _eofFn) {
    doubleFn = orDefault(_doubleFn, defaultDoubleParser);
    aryReader = _aryReader != null ? _aryReader : immutableArrayReader;
    objReader = _objReader != null ? _objReader : immutableObjReader;
    eofFn = orDefault(_eofFn, defaultEOFFn);
  }

  public static boolean numberChar(char v) {
    return (v >= '0' && v <= '9') || v == '-';
  }
  public static boolean isAsciiDigit(char v) {
    return v >= '0' && v <= '9';
  }

  final char[] tempRead(int nchars) throws EOFException {
    final char[] tempBuf = new char[nchars];
    if (reader.read(tempBuf, 0, nchars) == -1)
      throw new EOFException();
    return tempBuf;
  }

  final CharBuffer getCharBuffer() {
    final CharBuffer cb = charBuffer;
    cb.clear();
    return cb;
  }

  final String readString() throws Exception {
    final CharBuffer cb = getCharBuffer();
    char[] buffer = reader.buffer();
    while(buffer != null) {
      int startpos = reader.position();
      int len = buffer.length;
      for(int pos = startpos; pos < len; ++pos) {
	final char curChar = buffer[pos];
	if (curChar == '"') {
	  cb.append(buffer,startpos,pos);
	  reader.position(pos + 1);
	  return cb.toString();
	} else if (curChar == '\\') {
	  cb.append(buffer,startpos,pos);
	  final int idata = reader.readFrom(pos+1);
	  if (idata == -1)
	    throw new EOFException();
	  final char data = (char)idata;
	  switch(data) {
	  case '"':
	  case '\\':
	  case '/': cb.append(data); break;
	  case 'b': cb.append('\b'); break;
	  case 'f': cb.append('\f'); break;
	  case 'r': cb.append('\r'); break;
	  case 'n': cb.append('\n'); break;
	  case 't': cb.append('\t'); break;
	  case 'u':
	    final char[] temp = tempRead(4);
	    cb.append((char)(Integer.parseInt(new String(temp, 0, 4), 16)));
	    break;
	  default: throw new Exception("Unrecognized escape character: " + data);
	  }
	  startpos = reader.position();
	  //pos will be incremented in loop update
	  pos = startpos - 1;
	}
      }
      cb.append(buffer,startpos,len);
      buffer = reader.nextBuffer();
    }
    throw new EOFException("EOF while reading string: " + cb.toString());
  }
  final Object finalizeNumber(CharBuffer cb, boolean integer, final char firstChar,
			      final int dotIndex)
    throws Exception {
    final char[] cbBuffer = cb.buffer();
    final int nElems = cb.length();
    if (integer) {
      //Definitely an integer
      if ((nElems > 1 && firstChar == '0') ||
	  (nElems > 2 && firstChar == '-' && cbBuffer[1] == '0'))
	throw new Exception("JSON parse error - integer starting with 0: "
			    + cb.toString());
      if (nElems == 1) {
	long retval = Character.digit(cbBuffer[0], 10);
	if (retval < 0) throw new Exception("JSON parse error - invalid integer: " +
					    cb.toString());
	return retval;
      }
      else {
	final String strdata = cb.toString();
	if (nElems < 18)
	  return Long.parseLong(strdata);
	else {
	  try {
	    return Long.parseLong(strdata);
	  } catch (Exception e) {
	    return new BigInteger(strdata);
	  }
	}
      }
    } else {
      final String strdata = cb.toString();
      if (dotIndex != -1) {
	final char bufChar = cbBuffer[dotIndex];
	//sanity check
	if (bufChar != '.')
	  throw new RuntimeException("Programming error - dotIndex incorrect: "
				     + String.valueOf(dotIndex) + " - " +  strdata);
	//If there is a period it must have a number on each side.
	if (dotIndex == nElems - 1 || !isAsciiDigit(cbBuffer[dotIndex+1]) ||
	    dotIndex == 0 || !isAsciiDigit(cbBuffer[dotIndex-1]))
	  throw new Exception("JSON parse error - period must be preceded and followed by a digit: " +
			      strdata);
      }
      return doubleFn.apply(strdata);
    }
  }
  final Object readNumber(final char firstChar) throws Exception {
    final CharBuffer cb = getCharBuffer();
    cb.clear();
    cb.append(firstChar);
    boolean integer = true;
    char[] buffer = reader.buffer();
    int dotIndex = -1;
    while(buffer != null) {
      int startpos = reader.position();
      int pos = startpos;
      int len = buffer.length;
      for (; pos < len; ++pos) {
	final char nextChar = buffer[pos];
	if (Character.isWhitespace(nextChar) ||
	    nextChar == ']' ||
	    nextChar == '}' ||
	    nextChar == ',' ) {
	  cb.append(buffer, startpos, pos);
	  //Not we do not increment position here as the next method
	  //needs to restart parsing from this place.
	  reader.position(pos);
	  return finalizeNumber(cb, integer, firstChar, dotIndex);
	}
	else if (nextChar == 'e' ||
		 nextChar == 'E' ||
		 nextChar == '.') {
	  if (nextChar == '.') {
	    //if appending in blocks
	    dotIndex = cb.length() +  pos - startpos;
	    //if appending in singles
	    //dotIndex = cb.length() - 1;
	  }
	  integer = false;
	}
      }
      cb.append(buffer, startpos, pos);
      buffer = reader.nextBuffer();
    }
    return finalizeNumber(cb, integer, firstChar, dotIndex);
  }

  final Object readList() throws Exception {
    boolean hasNext = true;
    boolean first = true;
    Object aryObj = aryReader.newArray();
    while (!reader.eof()) {
      final char nextChar = reader.eatwhite();
      if (nextChar == ']') {
	if (hasNext && !first)
	  throw new Exception("One too many commas in your list my friend");
	return aryReader.finalizeArray(aryObj);
      } else if (nextChar != 0) {
	if (!hasNext)
	  throw new Exception("One too few commas in your list my friend");
	first = false;
	reader.unread();
	aryObj = aryReader.onValue(aryObj, readObject());
	hasNext = reader.eatwhite() == ',';
	if (!hasNext)
	  reader.unread();
      }
    }
    throw new EOFException("EOF while reading list");
  }

  // Unused for now, used for JSON5 encoding in which keys may be unquoted
  /* final String readKey(final char firstChar) throws Exception { */
  /*   final CharBuffer cb = charBuffer; */
  /*   cb.clear(); */
  /*   cb.append(firstChar); */
  /*   char[] buffer = reader.buffer(); */
  /*   while(buffer != null) { */
  /*     int len = buffer.length; */
  /*     final int startpos = reader.position(); */
  /*     int pos = startpos; */
  /*     for(; pos < len; ++pos) { */
  /* 	final char curChar = buffer[pos]; */
  /* 	if (Character.isWhitespace(curChar) || curChar == ':') { */
  /* 	  //unread the character */
  /* 	  reader.position(pos); */
  /* 	  cb.append(buffer, startpos, pos); */
  /* 	  final String retval = cb.toString(); */
  /* 	  if (retval.equals("")) { */
  /* 	    throw new RuntimeException("Invalid empty key."); */
  /* 	  } */
  /* 	  return retval; */
  /* 	} */
  /*     } */
  /*     cb.append(buffer,startpos,pos); */
  /*     buffer = reader.nextBuffer(); */
  /*   } */
  /*   throw new EOFException("EOF while reading a string."); */
  /* } */

  final Object readMap() throws Exception {
    boolean hasNext = true;
    boolean first = true;
    //By the json specification, keys must be strings.
    Object mapObj = objReader.newObj();
    while(!reader.eof()) {
      char nextChar = reader.eatwhite();
      if (nextChar == '}') {
	if (hasNext && !first)
	  throw new Exception("One too many commas in your map my friend: "
			      + String.valueOf(objReader.finalizeObj(mapObj)));
	return objReader.finalizeObj(mapObj);
      } else {
	first = false;
	if (!hasNext)
	  throw new Exception ("One too few commas in your map my friend: "
			       + String.valueOf(objReader.finalizeObj(mapObj)));
	String keyVal = null;
	if (nextChar == '"')
	  keyVal = readString();
	else
	  throw new Exception("JSON keys must be quoted strings.");

	nextChar = reader.eatwhite();
	if (nextChar != ':')
	  throw new Exception("Map keys must be followed by a ':'");
	Object valVal = readObject();
	mapObj = objReader.onKV(mapObj, keyVal, valVal);
	nextChar = reader.eatwhite();
	if ( nextChar == 0 )
	  throw new EOFException("EOF while reading map: " + String.valueOf(objReader.finalizeObj(mapObj)));
	hasNext = nextChar == ',';
	if (!hasNext)
	  reader.unread();
      }
    }
    throw new EOFException("EOF while reading map.");
  }

  public final Object readObject() throws Exception {
    if (reader == null) return null;

    final char val = reader.eatwhite();
    if (numberChar(val)) {
      return readNumber(val);
    } else {
      switch(val) {
      case '"': return readString();
      case 't': {
	final char[] data = tempRead(3);
	if (data[0] == 'r' && data[1] == 'u' && data[2] == 'e')
	  return true;
	throw new Exception("JSON parse error - bad boolean value.");
      }
      case 'f': {
	final char[] data = tempRead(4);
	if (data[0] == 'a' && data[1] == 'l' && data[2] == 's' && data[3] == 'e')
	  return false;
	throw new Exception("JSON parse error - bad boolean value.");
      }
      case 'n': {
	final char[] data = tempRead(3);
	if (data[0] == 'u' && data[1] == 'l' && data[2] == 'l')
	  return null;
	throw new Exception("JSON parse error - unrecognized 'null' entry.");
      }
      case '[': return readList();
      case '{': return readMap();
      case 0:
	if (reader.eof()) {
	  close();
	  return eofFn.get();
	}
	//fallthrough intentional
      default:
	throw new Exception("JSON parse error - Unexpected character - " + val);
      }
    }
  }

  public void beginParse(CharReader rdr) {
    reader = rdr;
  }

  public void close() throws Exception {
    if (reader != null)
      reader.close();
    reader = null;
  }
}
