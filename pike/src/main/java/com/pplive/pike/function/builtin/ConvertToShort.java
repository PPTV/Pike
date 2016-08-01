package com.pplive.pike.function.builtin;

public final class ConvertToShort extends ConvertBase {
	
	private static final long serialVersionUID = -3470474696112848430L;
	
	@Override
	public Class<?> targetType() { return Short.class; }

	public Short evaluate(Object obj){
		if (obj == null)
			return null;
		Class<?> t = obj.getClass();
		try{
			if (t == Short.class){
				return (Short)obj;
			}
			if (t == String.class){
				return Short.parseShort((String)obj);
			}
			if (t == Boolean.class){
				return (short)( ((Boolean)obj).booleanValue() ? 1 : 0 );
			}
			if (Number.class.isAssignableFrom(t)){
				return ((Number)obj).shortValue();
			}
		}
		catch(NumberFormatException e){
			return null;
		}
		return null;
	}
	
}