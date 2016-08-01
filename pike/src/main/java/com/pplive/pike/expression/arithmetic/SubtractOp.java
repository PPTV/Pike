package com.pplive.pike.expression.arithmetic;

public final class SubtractOp extends AbstractArithmeticOp{

	private static final long serialVersionUID = -4245002812999832222L;

	public static final String Op = "-";
	
	// promote to Integer
	public static Integer eval(Byte left, Byte right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Integer eval(Byte left, Short right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Integer eval(Short left, Byte right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Integer eval(Short left, Short right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Integer eval(Byte left, Integer right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Integer eval(Integer left, Byte right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Integer eval(Short left, Integer right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Integer eval(Integer left, Short right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Integer eval(Integer left, Integer right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	// promote to Long
	public static Long eval(Byte left, Long right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Long eval(Long left, Byte right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Long eval(Short left, Long right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Long eval(Long left, Short right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Long eval(Integer left, Long right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Long eval(Long left, Integer right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Long eval(Long left, Long right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	// promote to Float
	public static Float eval(Byte left, Float right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Float eval(Float left, Byte right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Float eval(Short left, Float right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Float eval(Float left, Short right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Float eval(Integer left, Float right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Float eval(Float left, Integer right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Float eval(Long left, Float right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Float eval(Float left, Long right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Float eval(Float left, Float right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	// promote to Double
	public static Double eval(Byte left, Double right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Double eval(Double left, Byte right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Double eval(Short left, Double right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Double eval(Double left, Short right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Double eval(Integer left, Double right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Double eval(Double left, Integer right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Double eval(Long left, Double right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Double eval(Double left, Long right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Double eval(Float left, Double right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Double eval(Double left, Float right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
	
	public static Double eval(Double left, Double right) {
		if (left == null || right == null)
			return null;
		return left - right;
	}
}
