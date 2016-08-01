package com.pplive.pike.function.builtin;

public final class ConvertToFloat extends ConvertBase {
	
	private static final long serialVersionUID = 254458968953275659L;
	
	@Override
	public Class<?> targetType() { return Float.class; }

	public Float evaluate(Object obj){
		if (obj == null)
			return null;
		Class<?> t = obj.getClass();
		try{
			if (t == Float.class){
				return (Float)obj;
			}
			if (t == String.class){
				return Float.parseFloat((String)obj);
			}
			if (t == Boolean.class){
				return ((Boolean)obj).booleanValue() ? 1.0f : 0.0f;
			}
			if (Number.class.isAssignableFrom(t)){
				return ((Number)obj).floatValue();
			}
		}
		catch(NumberFormatException e){
			return null;
		}
		return null;
	}
	
}
