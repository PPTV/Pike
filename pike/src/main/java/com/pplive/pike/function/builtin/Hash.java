package com.pplive.pike.function.builtin;

import com.pplive.pike.base.AbstractUDF;

public final class Hash extends AbstractUDF {
	
	private static final long serialVersionUID = -3546473849148905820L;

	public Integer evaluate(Object o) {
		return o != null ? o.hashCode() : null; 
	}
	
}
