package com.pplive.pike.util;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import com.pplive.pike.base.AbstractUDF;

public class ReflectionUtils {
	
	public static<T> T newInstance(Class<T> type) {
		try {
			return type.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public static Method getEvalMethod(List<Class<?>> passedArgTypes, Class<?> clazz) {
		return getMethod("evaluate", passedArgTypes, clazz);
	} 
	
	public static Method tryGetEvalMethod(List<Class<?>> passedArgTypes, Class<?> clazz) {
		return tryGetMethod("evaluate", passedArgTypes, clazz);
	} 
	
	public static Method getMethod(String method, List<Class<?>> passedArgTypes, Class<?> clazz) {
		Method result = tryGetMethod(method, passedArgTypes, clazz);
		if (result != null)
			return result;
		
		// not found, generate exception message and throw
		String argString = "(";
		for(Class<?> t : passedArgTypes){
			argString += t.getName();
			argString += ", ";
		}
		argString += ")";
		String message = String.format("class %s has no %s() method with parameters convertible from %s", clazz.getName(), method, argString);
		throw new RuntimeException(message);
	} 
	
	public static Method tryGetMethod(String method, Class<?> clazz) {
		if (method == null || method.isEmpty())
			throw new IllegalArgumentException("method cannot be null or empty");
		if (clazz == null)
			throw new IllegalArgumentException("clazz");

		Method[] methods = clazz.getMethods();
		for(Method m : methods) {
			if (m.getName().equals(method) && m.getParameterTypes().length == 0)
				return m;
		}
		
		return null;
	} 
	
	public static Method tryGetMethod(String method, List<Class<?>> passedArgTypes,
			Class<?> clazz) {
		if (method == null || method.isEmpty())
			throw new IllegalArgumentException("method cannot be null or empty");
		if (passedArgTypes == null)
			throw new IllegalArgumentException("passedArgTypes");
		if (clazz == null)
			throw new IllegalArgumentException("clazz");

		Method[] methods = clazz.getMethods();
		for(Method m : methods) {
			if (m.getName().equals(method) && isExactlyMatch(m, passedArgTypes))
				return m;
		}
		for(Method m : methods) {
			if (m.getName().equals(method) && isMatch(m, passedArgTypes))
				return m;
		}
		
		return null;
	} 
	
	private static boolean isExactlyMatch(Method m, List<Class<?>> passedArgTypes) {
		Class<?>[] expectedArgTypes = m.getParameterTypes();
		if (expectedArgTypes.length != passedArgTypes.size())
			return false;
		int n = -1;
		for (Class<?> t : passedArgTypes){
			n += 1;
			if (t != expectedArgTypes[n])
				return false;
		}
		return true;
	}
	
	private static boolean isMatch(Method m, List<Class<?>> passedArgTypes) {
		Class<?>[] expectedArgTypes = m.getParameterTypes();
		if (expectedArgTypes.length != passedArgTypes.size())
			return false;
		int n = 0;
		for (Class<?> t : passedArgTypes){
			if (isConvertible(t, expectedArgTypes[n]) == false)
				return false;
			n += 1;
		}
		return true;
	}

	private static boolean isConvertible(Class<?> fromType, Class<?> toType) {
		assert fromType.isPrimitive()
				|| isBoxedClassOfPrimitive(fromType)
				|| fromType == String.class
				|| Map.class.isAssignableFrom(fromType)
				|| java.sql.Date.class.isAssignableFrom(fromType)
				|| java.sql.Time.class.isAssignableFrom(fromType)
				|| java.sql.Timestamp.class.isAssignableFrom(fromType)
				;
		if (fromType == toType || toType == Object.class)
			return true;

		if (fromType == Boolean.class && toType == boolean.class)
			return true;
		if (fromType == boolean.class || fromType == Boolean.class
				|| toType == boolean.class || toType == Boolean.class)
			return false;
		
		if ((fromType.isPrimitive() || isBoxedClassOfPrimitive(fromType))
				&& toType.isPrimitive()) {
			int fromTypeOrder = getPrimitiveConvertOrder(fromType);
			int toTypeOrder = getPrimitiveConvertOrder(toType);
			return fromTypeOrder <= toTypeOrder;
		}
		
		if (isBoxedClassOfPrimitive(fromType) && isBoxedClassOfPrimitive(toType)) {
			int fromTypeOrder = getPrimitiveConvertOrder(fromType);
			int toTypeOrder = getPrimitiveConvertOrder(toType);
			return fromTypeOrder <= toTypeOrder;
		}
		
		return toType.isAssignableFrom(fromType);
	}
	
	private static boolean isBoxedClassOfPrimitive(Class<?> t) {
		assert t != null;
		return (t == Boolean.class
				|| t == Byte.class
				|| t == Character.class
				|| t == Short.class
				|| t == Integer.class
				|| t == Long.class
				|| t == Float.class
				|| t == Double.class);
			
	}
	
	private static int getPrimitiveConvertOrder(Class<?> t) {
		if (t == byte.class || t == Byte.class) return 2;
		if (t == char.class || t == Character.class) return 3;
		if (t == short.class || t == Short.class) return 4;
		if (t == int.class || t == Integer.class) return 5;
		if (t == long.class || t == Long.class) return 6;
		if (t == float.class || t == Float.class) return 7;
		if (t == double.class || t == Double.class) return 8;
		throw new RuntimeException("should never happen: passed type is non-primitive or is boolean");
	}
}
