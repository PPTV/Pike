package com.pplive.pike.base;

public final class HashFailThrower {

	public static int throwOnHash(Object o){
		String msg = String.format("class %s has no overridden hashCode(), cannot be used HashMap<>, etc", o.getClass().getSimpleName());
		throw new UnsupportedOperationException(msg);
	}
}
