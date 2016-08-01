package com.pplive.pike.function.builtin;

public final class ConvertToByte extends ConvertBase {
	
	private static final long serialVersionUID = 1448544172266103883L;
	
	@Override
	public Class<?> targetType() { return Byte.class; }

	public Byte evaluate(Object obj){
		if (obj == null)
			return null;
		Class<?> t = obj.getClass();
		try{
			if (t == Byte.class){
				return (Byte)obj;
			}
			if (t == String.class){
				return Byte.parseByte((String)obj);
			}
			if (t == Boolean.class){
				return (byte)( ((Boolean)obj).booleanValue() ? 1 : 0 );
			}
			if (Number.class.isAssignableFrom(t)){
				return ((Number)obj).byteValue();
			}
		}
		catch(NumberFormatException e){
			return null;
		}
		return null;
	}
	
}