package com.pplive.pike.function.builtin;

import java.lang.reflect.Method;

import com.pplive.pike.base.AbstractUDF;
import com.pplive.pike.util.ReflectionUtils;

public abstract class ConvertBase extends AbstractUDF {

	private static final long serialVersionUID = 1L;

	public abstract Class<?> targetType();
	
	public static Class<?> getTargetType(Class<?> convertClass){
		if (ConvertBase.class.isAssignableFrom(convertClass) == false){
			throw new IllegalArgumentException(String.format("%s is not a convert class", convertClass.getSimpleName()));
		}
		
		Method m = ReflectionUtils.tryGetMethod("targetType", convertClass);
		assert m != null;
		try {
			Class<?> t = (Class<?>)m.invoke(convertClass.newInstance());
			return t;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
