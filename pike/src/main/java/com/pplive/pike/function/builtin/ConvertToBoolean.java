package com.pplive.pike.function.builtin;

public final class ConvertToBoolean extends ConvertBase {
	
	private static final long serialVersionUID = -6354336992063863610L;
	
	@Override
	public Class<?> targetType() { return Boolean.class; }

	public Boolean evaluate(Object obj){
		if(obj == null)
			return null;
		return convert(obj);
	}
	
	public static boolean convert(Object obj) {
		if (obj == null)
			return false;
		Class<?> t = obj.getClass();
		if (t == Boolean.class)
			return (Boolean)obj;
		if (t == String.class){
			String s = (String)obj;
			return s.isEmpty() == false
					&& s.equals("0") == false
					&& s.equalsIgnoreCase("false") == false;
		}
		if (Number.class.isAssignableFrom(t)){
			return ((Number)obj).longValue() != 0;
		}
		return false;
	}
	
}
