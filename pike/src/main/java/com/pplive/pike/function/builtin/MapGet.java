package com.pplive.pike.function.builtin;

import java.util.Map;

import com.pplive.pike.base.AbstractUDF;

public final class MapGet extends AbstractUDF {
	
	private static final long serialVersionUID = -3546473849148905820L;

	public String evaluate(Map<?, ?> map, Object key) {
		if (map == null || key == null)
			return null;
		Object o = map.get(key);
		return o != null ? o.toString() : null;
	}
	
}
