package com.pplive.pike.expression.unary;

public final class InverseOp {

	public static final String Op = "-";
	
	public static Byte eval(Byte n){
		if (n == null)
			return null;
		return  (byte) -n.byteValue();
	}
	
	public static Short eval(Short n){
		if (n == null)
			return null;
		return  (short) -n.shortValue();
	}
	
	public static Integer eval(Integer n){
		if (n == null)
			return null;
		return  -n.intValue();
	}
	
	public static Long eval(Long n){
		if (n == null)
			return null;
		return -n.longValue();
	}
	
	public static Float eval(Float n){
		if (n == null)
			return null;
		return -n.floatValue();
	}
	
	public static Double eval(Double n){
		if (n == null)
			return null;
		return -n.doubleValue();
	}
}
