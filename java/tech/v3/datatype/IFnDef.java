package tech.v3.datatype;


import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.Util;
import clojure.lang.RT;
import clojure.lang.ArityException;


public interface IFnDef extends IFn
{
  
  default Object call() {
    return invoke();
  }

  default void run(){
    invoke();
  }

  default Object invoke() {
    return throwArity(0);
  }

  default Object invoke(Object arg1) {
    return throwArity(1);
  }

  default Object invoke(Object arg1, Object arg2) {
    return throwArity(2);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3) {
    return throwArity(3);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4) {
    return throwArity(4);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
    return throwArity(5);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) {
    return throwArity(6);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7)
  {
    return throwArity(7);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
		       Object arg8) {
    return throwArity(8);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
		       Object arg8, Object arg9) {
    return throwArity(9);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
		       Object arg8, Object arg9, Object arg10) {
    return throwArity(10);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
		       Object arg8, Object arg9, Object arg10, Object arg11) {
    return throwArity(11);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
		       Object arg8, Object arg9, Object arg10, Object arg11, Object arg12) {
    return throwArity(12);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
		       Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13)
  {
    return throwArity(13);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
		       Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14)
  {
    return throwArity(14);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
		       Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
		       Object arg15) {
    return throwArity(15);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
		       Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
		       Object arg15, Object arg16) {
    return throwArity(16);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
		       Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
		       Object arg15, Object arg16, Object arg17) {
    return throwArity(17);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
		       Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
		       Object arg15, Object arg16, Object arg17, Object arg18) {
    return throwArity(18);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
		       Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
		       Object arg15, Object arg16, Object arg17, Object arg18, Object arg19) {
    return throwArity(19);
  }

  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
		       Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
		       Object arg15, Object arg16, Object arg17, Object arg18, Object arg19, Object arg20)
  {
    return throwArity(20);
  }


  default Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
		       Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
		       Object arg15, Object arg16, Object arg17, Object arg18, Object arg19, Object arg20,
		       Object... args)
  {
    return throwArity(21);
  }

  default Object applyTo(ISeq arglist) {
    return applyToHelper(this, Util.ret1(arglist,arglist = null));
  }

  default Object applyToHelper(IFn ifn, ISeq arglist) {
    switch(RT.boundedLength(arglist, 20))
      {
      case 0:
	arglist = null;
	return ifn.invoke();
      case 1:
	return ifn.invoke(Util.ret1(arglist.first(),arglist = null));
      case 2:
	return ifn.invoke(arglist.first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 3:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 4:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 5:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 6:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 7:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 8:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 9:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 10:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 11:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 12:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 13:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 14:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 15:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 16:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 17:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 18:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 19:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      case 20:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , Util.ret1((arglist = arglist.next()).first(),arglist = null)
			  );
      default:
	return ifn.invoke(arglist.first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , (arglist = arglist.next()).first()
			  , RT.seqToArray(Util.ret1(arglist.next(),arglist = null)));
      }
  }

  default Object throwArity(int n){
    String name = getClass().getName();
    throw new ArityException(n, name);
  }
}
