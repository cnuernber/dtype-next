package tech.v3.datatype;


import java.io.EOFException;
import java.util.ArrayList;
import java.util.HashMap;
import java.math.BigDecimal;
import java.math.BigInteger;
import clojure.lang.IFn;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashMap;
import clojure.lang.LazilyPersistentVector;


public final class JSONReader implements AutoCloseable {
  CharReader reader;
  //Parsing specialization functions
  public final IFn doubleFn;
  public final IFn keyFn;
  public final IFn valFn;
  public final IFn mapFn;
  public final IFn listFn;
  public final IFn eofFn;

  //Top level object parsing context
  final ArrayList<ArrayList<Object>> contextStack = new ArrayList<ArrayList<Object>>();
  int stackDepth = 0;
  //We only need one temp buffer for various string building activities
  final CharBuffer charBuffer = new CharBuffer();
  //A temp buffer for reading out fixed sequences of characters
  final char[] tempBuf = new char[8];
  public static final IFn defaultDoubleParser = new IFnDef() {
      public Object invoke(Object data) {
	return Double.parseDouble((String)data);
      }
    };
  public static final IFn identityFn = new IFnDef() {
      public Object invoke(Object data) { return data; }
      //value fns get passed the key
      public Object invoke(Object a0, Object data) { return data; }
    };
  public static final IFn defaultMapFn = new IFnDef() {
      public Object invoke(Object data) {
	Object[] dldata = (Object[])data;
	if (dldata.length <= 16)
	  return PersistentArrayMap.createAsIfByAssoc(dldata);
	else
	  return PersistentHashMap.create(null, dldata);
      }
    };
  public static final IFn defaultListFn = new IFnDef() {
      public Object invoke(Object data) {
	return LazilyPersistentVector.createOwning((Object[])data);
      }
    };
  public static final IFn defaultEOFFn = new IFnDef() {
      public Object invoke() {
	throw new RuntimeException("EOF encounted while reading stream.");
      }
    };

  static final IFn orDefault(IFn arg, IFn def) {
    return arg != null ? arg : def;
  }

  final void pushContext() {
    ++stackDepth;
  }
  final void popContext() {
    --stackDepth;
    if (stackDepth < 0)
      throw new RuntimeException("Stack underflow");
  }
  final ArrayList<Object> currentContext() {
    for(int needed = stackDepth - contextStack.size() + 1; needed > 0; --needed)
      contextStack.add(new ArrayList<Object>());
    return contextStack.get(stackDepth);
  }

  public JSONReader(CharReader rdr, IFn _doubleFn, IFn _keyFn, IFn _valFn, IFn _listFn,
		    IFn _mapFn, IFn _eofFn) {
    reader = rdr;
    doubleFn = orDefault(_doubleFn, defaultDoubleParser);
    keyFn = orDefault(_keyFn, identityFn);
    valFn = orDefault(_valFn, identityFn);
    listFn = orDefault(_listFn, defaultListFn);
    mapFn = orDefault(_mapFn, defaultMapFn);
    eofFn = orDefault(_eofFn, defaultEOFFn);
  }

  public static boolean numberChar(char v) {
    return (v >= '0' && v <= '9') || v == '-';
  }

  final char[] tempRead(int nchars) throws EOFException {
    if (reader.read(tempBuf, 0, nchars) == -1)
      throw new EOFException();
    return tempBuf;
  }

  final String readString() throws Exception {
    final CharBuffer cb = charBuffer;
    cb.clear();
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
      buffer = reader.nextBuffer();
    }
    throw new EOFException("EOF while reading string: " + cb.toString());
  }
  final Object finalizeNumber(CharBuffer cb, boolean integer, final char firstChar,
			      final int dotIndex)
    throws Exception {
    final char[] cbBuffer = cb.buffer();
    final String strdata = cb.toString();
    final int nElems = strdata.length();
    if (integer) {
      //Definitely an integer
      if ((nElems > 1 && firstChar == '0') ||
	  (nElems > 2 && firstChar == '-' && cbBuffer[1] == '0'))
	throw new Exception("JSON parse error - integer starting with 0: "
			    + strdata);
      if (strdata.length() < 18)
	return Long.parseLong(strdata);
      else {
	try {
	  return Long.parseLong(strdata);
	} catch (Exception e) {
	  return new BigInteger(strdata);
	}
      }
    } else {
      if (dotIndex != -1) {
	final char bufChar = cbBuffer[dotIndex];
	//sanity check
	if (bufChar != '.')
	  throw new RuntimeException("Programming error - dotIndex incorrect: "
				     + String.valueOf(dotIndex) + " - " +  strdata);
	if (dotIndex == nElems - 1 || !Character.isDigit(cbBuffer[dotIndex+1]) ||
	    dotIndex == 0 || !Character.isDigit(cbBuffer[dotIndex-1]))
	  throw new Exception("JSON parse error - period must be followed by digit: " +
			      strdata);
      }
      return doubleFn.invoke(strdata);
    }
  }
  final Object readNumber(final char firstChar) throws Exception {
    final CharBuffer cb = charBuffer;
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
	  if (nextChar == '.')
	    dotIndex = cb.length() + pos - startpos;
	  integer = false;
	}
      }
      cb.append(buffer, startpos, len);
      buffer = reader.nextBuffer();
    }
    return finalizeNumber(cb, integer, firstChar, dotIndex);
  }

  final Object readList() throws Exception {
    final ArrayList<Object> dataBuf = currentContext();
    dataBuf.clear();
    boolean hasNext = true;
    boolean first = true;
    while (!reader.eof()) {
      final char nextChar = reader.eatwhite();
      if (nextChar == ']') {
	if (hasNext && !first)
	  throw new Exception("One too many commas in your list my friend");
	return listFn.invoke(dataBuf.toArray());
      } else if (nextChar != 0) {
	if (!hasNext)
	  throw new Exception("One too few commas in your list my friend");
	first = false;
	reader.unread();
	dataBuf.add(readObject());
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
    final ArrayList<Object> dataBuf = currentContext();
    dataBuf.clear();
    boolean hasNext = true;
    boolean first = true;
    //By the json specification, keys must be strings.
    while(!reader.eof()) {
      char nextChar = reader.eatwhite();
      if (nextChar == '}') {
	if (hasNext && !first)
	  throw new Exception("One too many commas in your map my friend: "
			      + dataBuf.toString());
	return mapFn.invoke(dataBuf.toArray());
      } else {
	first = false;
	if (!hasNext)
	  throw new Exception ("One too few commas in your map my friend: "
			       + dataBuf.toString());
	Object keyVal = null;
	if (nextChar == '"')
	  keyVal = keyFn.invoke(readString());
	else
	  throw new Exception("JSON keys must be quoted strings.");

	nextChar = reader.eatwhite();
	if (nextChar != ':')
	  throw new Exception("Map keys must be followed by a ':'");
	Object valVal = readObject();
	valVal = valFn.invoke(keyVal, valVal);
	//Note reference equality here
	if (valVal != valFn) {
	  dataBuf.add(keyVal);
	  dataBuf.add(valVal);
	}
	nextChar = reader.eatwhite();
	if ( nextChar == 0 )
	  throw new EOFException("EOF while reading map: " + dataBuf.toString());
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
      case '[': {
	pushContext();
	final Object retval = readList();
	popContext();
	return retval;
      }
      case '{': {
	pushContext();
	final Object retval = readMap();
	popContext();
	return retval;
      }
      case 0:
	if (reader.eof()) {
	  close();
	  return eofFn.invoke();
	}
	//fallthrough intentional
      default:
	throw new Exception("JSON parse error - Unexpected character - " + val);
      }
    }
  }

  public void close() throws Exception {
    if (reader != null)
      reader.close();
    reader = null;
  }
}
