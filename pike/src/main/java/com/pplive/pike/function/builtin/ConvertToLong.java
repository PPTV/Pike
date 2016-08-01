package com.pplive.pike.function.builtin;

public final class ConvertToLong extends ConvertBase {
	
	private static final long serialVersionUID = -800079927410626788L;
	
	@Override
	public Class<?> targetType() { return Long.class; }

	public Long evaluate(Object obj){
		if (obj == null)
			return null;
		Class<?> t = obj.getClass();
		try{
			if (t == Long.class){
				return (Long)obj;
			}
			if (t == String.class){
				return Long.parseLong((String)obj);
			}
			if (t == Boolean.class){
				return ((Boolean)obj).booleanValue() ? 1L : 0L;
			}
			if (Number.class.isAssignableFrom(t)){
				return ((Number)obj).longValue();
			}
		}
		catch(NumberFormatException e){
			return null;
		}
		return null;
	}
	
}