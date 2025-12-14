package tech.v3.datatype;
import clojure.lang.RT;
import clojure.lang.Keyword;
import clojure.lang.IHashEq;
public class KeywordSets {
  public static final Keyword k_boolean = RT.keyword(null, "boolean");
  public static final Keyword k_char = RT.keyword(null, "char");
  public static final Keyword k_float32 = RT.keyword(null, "float32");
  public static final Keyword k_float64 = RT.keyword(null, "float64");
  public static final Keyword k_int16 = RT.keyword(null, "int16");
  public static final Keyword k_int32 = RT.keyword(null, "int32");
  public static final Keyword k_int64 = RT.keyword(null, "int64");
  public static final Keyword k_int8 = RT.keyword(null, "int8");
  public static final Keyword k_keyword = RT.keyword(null, "keyword");
  public static final Keyword k_object = RT.keyword(null, "object");
  public static final Keyword k_string = RT.keyword(null, "string");
  public static final Keyword k_uint16 = RT.keyword(null, "uint16");
  public static final Keyword k_uint32 = RT.keyword(null, "uint32");
  public static final Keyword k_uint64 = RT.keyword(null, "uint64");
  public static final Keyword k_uint8 = RT.keyword(null, "uint8");
  public static final Keyword k_uuid = RT.keyword(null, "uuid");
  public static boolean isBaseType(final Object obj) {
    if(!(obj instanceof IHashEq)) return false;
    final long hv = Integer.toUnsignedLong(((IHashEq)obj).hasheq()) & 63;
    switch((int)hv) {
    case 1: return obj == k_int16;
    case 16: return obj == k_int8;
    case 28: return obj == k_boolean;
    case 32: return obj == k_int32;
    case 33: return obj == k_float32;
    case 34: return obj == k_float64;
    case 38: return obj == k_int64;
    case 61: return obj == k_object;
    case 62: return obj == k_char;
    default: return false;
    }
  }
  public static boolean isBaseNumericType(final Object obj) {
    if(!(obj instanceof IHashEq)) return false;
    final long hv = Integer.toUnsignedLong(((IHashEq)obj).hasheq()) & 63;
    switch((int)hv) {
    case 1: return obj == k_int16;
    case 16: return obj == k_int8;
    case 32: return obj == k_int32;
    case 33: return obj == k_float32;
    case 34: return obj == k_float64;
    case 38: return obj == k_int64;
    default: return false;
    }
  }
  public static boolean isPrimitiveType(final Object obj) {
    if(!(obj instanceof IHashEq)) return false;
    final long hv = Integer.toUnsignedLong(((IHashEq)obj).hasheq()) & 63;
    switch((int)hv) {
    case 1: return obj == k_int16;
    case 16: return obj == k_int8;
    case 28: return obj == k_boolean;
    case 32: return obj == k_int32;
    case 33: return obj == k_float32;
    case 34: return obj == k_float64;
    case 38: return obj == k_int64;
    default: return false;
    }
  }
  public static boolean isKnownType(final Object obj) {
    if(!(obj instanceof IHashEq)) return false;
    final long hv = Integer.toUnsignedLong(((IHashEq)obj).hasheq()) & 255;
    switch((int)hv) {
    case 16: return obj == k_int8;
    case 46: return obj == k_string;
    case 51: return obj == k_keyword;
    case 65: return obj == k_int16;
    case 126: return obj == k_char;
    case 146: return obj == k_uint32;
    case 160: return obj == k_int32;
    case 161: return obj == k_float32;
    case 168: return obj == k_uint64;
    case 174: return obj == k_uint16;
    case 189: return obj == k_object;
    case 191: return obj == k_uint8;
    case 217: return obj == k_uuid;
    case 220: return obj == k_boolean;
    case 226: return obj == k_float64;
    case 230: return obj == k_int64;
    default: return false;
    }
  }
  public static boolean isIntegerType(final Object obj) {
    if(!(obj instanceof IHashEq)) return false;
    final long hv = Integer.toUnsignedLong(((IHashEq)obj).hasheq()) & 31;
    switch((int)hv) {
    case 0: return obj == k_int32;
    case 1: return obj == k_int16;
    case 6: return obj == k_int64;
    case 8: return obj == k_uint64;
    case 14: return obj == k_uint16;
    case 16: return obj == k_int8;
    case 18: return obj == k_uint32;
    case 31: return obj == k_uint8;
    default: return false;
    }
  }
  public static boolean isUnsignedIntegerType(final Object obj) {
    if(!(obj instanceof IHashEq)) return false;
    final long hv = Integer.toUnsignedLong(((IHashEq)obj).hasheq()) & 7;
    switch((int)hv) {
    case 0: return obj == k_uint64;
    case 2: return obj == k_uint32;
    case 6: return obj == k_uint16;
    case 7: return obj == k_uint8;
    default: return false;
    }
  }
  public static boolean isSignedIntegerType(final Object obj) {
    if(!(obj instanceof IHashEq)) return false;
    final long hv = Integer.toUnsignedLong(((IHashEq)obj).hasheq()) & 31;
    switch((int)hv) {
    case 0: return obj == k_int32;
    case 1: return obj == k_int16;
    case 6: return obj == k_int64;
    case 16: return obj == k_int8;
    default: return false;
    }
  }
  public static boolean isFloatType(final Object obj) {
    if(!(obj instanceof IHashEq)) return false;
    final long hv = Integer.toUnsignedLong(((IHashEq)obj).hasheq()) & 1;
    switch((int)hv) {
    case 0: return obj == k_float64;
    case 1: return obj == k_float32;
    default: return false;
    }
  }
}
