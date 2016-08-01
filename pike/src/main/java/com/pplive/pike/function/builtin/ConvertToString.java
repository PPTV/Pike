package com.pplive.pike.function.builtin;

public final class ConvertToString extends ConvertBase {
	
	private static final long serialVersionUID = -2520069421826045189L;
	
	@Override
	public Class<?> targetType() { return String.class; }

	public String evaluate(Object obj){
		if (obj == null)
			return null;
		return obj.toString();
	}
	
}
