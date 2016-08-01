package com.pplive.pike.expression.compare;

public final class LessThanOp extends AbstractCompareOp {

	private static final long serialVersionUID = 304399580303447941L;

	public static final String Op = "<";
	
	public static Boolean eval(Byte left, Byte right) {
		if (left == null || right == null)
			return null;
		return left < right;
	}
	
	public static Boolean eval(Byte left, Short right) {
		if (left == null || right == null)
			return null;
		return left.shortValue() < right;
	}
	
	public static Boolean eval(Short left, Byte right) {
		if (left == null || right == null)
			return null;
		return left < right.shortValue();
	}
	
	public static Boolean eval(Short left, Short right) {
		if (left == null || right == null)
			return null;
		return left < right;
	}
	
	public static Boolean eval(Byte left, Integer right) {
		if (left == null || right == null)
			return null;
		return left.intValue() < right;
	}
	
	public static Boolean eval(Integer left, Byte right) {
		if (left == null || right == null)
			return null;
		return left < right.intValue();
	}
	
	public static Boolean eval(Short left, Integer right) {
		if (left == null || right == null)
			return null;
		return left.shortValue() < right;
	}
	
	public static Boolean eval(Integer left, Short right) {
		if (left == null || right == null)
			return null;
		return left < right.shortValue();
	}
	
	public static Boolean eval(Integer left, Integer right) {
		if (left == null || right == null)
			return null;
		return left < right;
	}
	
	public static Boolean eval(Byte left, Long right) {
		if (left == null || right == null)
			return null;
		return left.longValue() < right;
	}
	
	public static Boolean eval(Long left, Byte right) {
		if (left == null || right == null)
			return null;
		return left < right.longValue();
	}
	
	public static Boolean eval(Short left, Long right) {
		if (left == null || right == null)
			return null;
		return left.longValue() < right;
	}
	
	public static Boolean eval(Long left, Short right) {
		if (left == null || right == null)
			return null;
		return left < right.longValue();
	}
	
	public static Boolean eval(Integer left, Long right) {
		if (left == null || right == null)
			return null;
		return left.longValue() < right;
	}
	
	public static Boolean eval(Long left, Integer right) {
		if (left == null || right == null)
			return null;
		return left < right.longValue();
	}
	
	public static Boolean eval(Long left, Long right) {
		if (left == null || right == null)
			return null;
		return left < right;
	}
	
	public static Boolean eval(Byte left, Float right) {
		if (left == null || right == null)
			return null;
		return left.floatValue() < right;
	}
	
	public static Boolean eval(Float left, Byte right) {
		if (left == null || right == null)
			return null;
		return left < right.floatValue();
	}
	
	public static Boolean eval(Short left, Float right) {
		if (left == null || right == null)
			return null;
		return left.floatValue() < right;
	}
	
	public static Boolean eval(Float left, Short right) {
		if (left == null || right == null)
			return null;
		return left < right.floatValue();
	}
	
	public static Boolean eval(Integer left, Float right) {
		if (left == null || right == null)
			return null;
		return left.floatValue() < right;
	}
	
	public static Boolean eval(Float left, Integer right) {
		if (left == null || right == null)
			return null;
		return left < right.floatValue();
	}
	
	public static Boolean eval(Long left, Float right) {
		if (left == null || right == null)
			return null;
		return left.floatValue() < right;
	}
	
	public static Boolean eval(Float left, Long right) {
		if (left == null || right == null)
			return null;
		return left < right.floatValue();
	}
	
	public static Boolean eval(Float left, Float right) {
		if (left == null || right == null)
			return null;
		return left < right;
	}
	
	public static Boolean eval(Byte left, Double right) {
		if (left == null || right == null)
			return null;
		return left.doubleValue() < right;
	}
	
	public static Boolean eval(Double left, Byte right) {
		if (left == null || right == null)
			return null;
		return left < right.doubleValue();
	}
	
	public static Boolean eval(Short left, Double right) {
		if (left == null || right == null)
			return null;
		return left.doubleValue() < right;
	}
	
	public static Boolean eval(Double left, Short right) {
		if (left == null || right == null)
			return null;
		return left < right.doubleValue();
	}
	
	public static Boolean eval(Integer left, Double right) {
		if (left == null || right == null)
			return null;
		return left.doubleValue() < right;
	}
	
	public static Boolean eval(Double left, Integer right) {
		if (left == null || right == null)
			return null;
		return left < right.doubleValue();
	}
	
	public static Boolean eval(Long left, Double right) {
		if (left == null || right == null)
			return null;
		return left.doubleValue() < right;
	}
	
	public static Boolean eval(Double left, Long right) {
		if (left == null || right == null)
			return null;
		return left < right.doubleValue();
	}
	
	public static Boolean eval(Float left, Double right) {
		if (left == null || right == null)
			return null;
		return left.doubleValue() < right;
	}
	
	public static Boolean eval(Double left, Float right) {
		if (left == null || right == null)
			return null;
		return left < right.doubleValue();
	}
	
	public static Boolean eval(Double left, Double right) {
		if (left == null || right == null)
			return null;
		return left < right;
	}
	
	public static Boolean eval(Object left, String right) {
		if (left == null || right == null)
			return null;
		return left.toString().compareToIgnoreCase(right) < 0;
	}
	
	public static Boolean eval(String left, Object right) {
		if (left == null || right == null)
			return null;
		return left.compareToIgnoreCase(right.toString()) < 0;
	}
	
	public static Boolean eval(String left, String right) {
		if (left == null || right == null)
			return null;
		return left.compareToIgnoreCase(right) < 0;
	}
}
