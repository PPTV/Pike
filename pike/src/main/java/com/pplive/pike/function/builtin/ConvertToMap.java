package com.pplive.pike.function.builtin;

import java.util.Map;

import com.pplive.pike.exec.spoutproto.ColumnType;

public final class ConvertToMap extends ConvertBase {
	
	private static final long serialVersionUID = 1L;
	
	@Override
	public Class<?> targetType() { return Map.class; }

	public Map<Object, String> evaluate(Object obj){
		if (obj instanceof Map<?, ?>){
			@SuppressWarnings("unchecked") Map<Object, String> res = (Map<Object, String>)obj;
			return res;
		}
		else if (obj instanceof String){
			Object mapObj = ColumnType.Map_ObjString.tryParse((String)obj);
			@SuppressWarnings("unchecked") Map<Object, String> res = (Map<Object, String>)mapObj;
			return res;
		}
		else{
			return null;
		}
	}
	
}
