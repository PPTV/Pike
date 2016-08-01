package com.pplive.pike.expression.unary;

import com.pplive.pike.function.builtin.ConvertToBoolean;

public final class NotOp {

	public static final String Op = "NOT";
	
	public static Boolean eval(Boolean obj) {
		if (obj == null)
			return null;
		return !obj;
	}
	
	public static Boolean eval(Object obj) {
		if (obj == null)
			return null;
		return !ConvertToBoolean.convert(obj);
	}

}
