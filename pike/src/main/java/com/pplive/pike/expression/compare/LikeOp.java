package com.pplive.pike.expression.compare;

public final class LikeOp extends AbstractCompareOp {

	private static final long serialVersionUID = -6379226283580152656L;

	public static final String Op = "LIKE";
	
	public static Boolean eval(String left, String pattern) {
		if (left == null || pattern == null)
			return null;
		
		// TODO: should follow SQL standard, here is regular expression pattern.
		return left.matches(pattern);
	}
}
