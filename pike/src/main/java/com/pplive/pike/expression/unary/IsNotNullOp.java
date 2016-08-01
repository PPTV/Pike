package com.pplive.pike.expression.unary;

public final class IsNotNullOp {

	public static final String Op = "IsNotNULL";
	
	public static boolean eval(Object o) {
		return o != null;
	}

}
