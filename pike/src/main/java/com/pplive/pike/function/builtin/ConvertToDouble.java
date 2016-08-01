package com.pplive.pike.function.builtin;

public final class ConvertToDouble extends ConvertBase {
	
	private static final long serialVersionUID = 254458968953275659L;
	
	@Override
	public Class<?> targetType() { return Double.class; }

	public Double evaluate(Object obj){
		if (obj == null)
			return null;
		Class<?> t = obj.getClass();
		try{
			if (t == Double.class){
				return (Double)obj;
			}
			if (t == String.class){
				return Double.parseDouble((String)obj);
			}
			if (t == Boolean.class){
				return ((Boolean)obj).booleanValue() ? 1.0 : 0.0;
			}
			if (Number.class.isAssignableFrom(t)){
				return ((Number)obj).doubleValue();
			}
		}
		catch(NumberFormatException e){
			return null;
		}
		return null;
	}
	
}
