package com.pplive.pike.expression.unary;

public final class IsNullOp {

	public static final String Op = "IsNULL";
	
	public static boolean eval(Object o) {
		return o == null;
	}

}
