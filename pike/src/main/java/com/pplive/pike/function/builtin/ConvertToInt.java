package com.pplive.pike.function.builtin;

import com.pplive.pike.base.AbstractUDF;
import com.pplive.pike.base.CaseIgnoredString;

public final class ConvertToInt extends ConvertBase {
	
	private static final long serialVersionUID = 3953408457321877598L;
	
	@Override
	public Class<?> targetType() { return Integer.class; }

	public Integer evaluate(Object obj){
		if (obj == null)
			return null;
		Class<?> t = obj.getClass();
		try{
			if (t == Integer.class){
				return (Integer)obj;
			}
			if (t == String.class){
				return Integer.parseInt((String)obj);
			}
			if (t == Boolean.class){
				return ((Boolean)obj).booleanValue() ? 1 : 0;
			}
			if (Number.class.isAssignableFrom(t)){
				return ((Number)obj).intValue();
			}
		}
		catch(NumberFormatException e){
			return null;
		}
		return null;
	}
	
}