package com.pplive.pike.exec.spoutproto;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;


public enum ColumnType implements Serializable {
	Unknown,
	Boolean,
	String,
	Byte,
	Short,
	Int,
	Long,
	Float, 
	Double,
	Map_ObjString,  // key is Object, value is String
	Complex,
	Date, 
	Time,
	Timestamp;
		
	public Object tryParse(String value) {
		try {
			if (value != null && value.isEmpty() == false){
				return parseImpl(value);
			}
			return (this == String ? "" : null);
		}
		catch(NumberFormatException e) {
			return null;
		}
		catch(IllegalArgumentException e){
			return null;
		}
	}

	private Object parseImpl(String value) {
		switch (this) {
		case Boolean:
			if (StringUtils.equals("0", value) || value.equalsIgnoreCase("false"))
				return false;
			else if (StringUtils.equals("1", value) || value.equalsIgnoreCase("true"))
				return true;
			throw new IllegalArgumentException(value);
		case Byte:
			return java.lang.Byte.parseByte(value);
		case Short:
			return java.lang.Short.parseShort(value);
		case Int:
			return java.lang.Integer.parseInt(value);
		case Long:
			return java.lang.Long.parseLong(value);
		case Float:
			return java.lang.Float.parseFloat(value);
		case Double:
			return java.lang.Double.parseDouble(value);
		case Map_ObjString:
			return simpleParseStringMap(value);
		case Date:
			return java.sql.Date.valueOf(value);
		case Time:
			return java.sql.Time.valueOf(value);
		case Timestamp:
			return java.sql.Timestamp.valueOf(value);
		default:
			return value;
		}
	}

    public Class<?> mappingJavaType() {
        return ColumnType.convert(this);
    }

	public static Class<?> convert(ColumnType columnType) {
		switch(columnType){
		case Unknown:
			return Object.class;
		case Boolean:
			return Boolean.class;
		case String:
			return String.class;
		case Byte:
			return Byte.class;
		case Short:
			return Short.class;
		case Int:
			return Integer.class;
		case Long:
			return Long.class;
		case Float:
			return Float.class;
		case Double:
			return Double.class;
		case Map_ObjString:
			return Map.class;
		case Date:
			return java.sql.Date.class;
		case Time:
			return java.sql.Time.class;
		case Timestamp:
			return java.sql.Timestamp.class;
		default:
			return null;
		}
	}
	
	public static boolean isSimpleValueType(Class<?> columnType) {
		return columnType == Boolean.class
				|| columnType == String.class
				|| columnType == Byte.class
				|| columnType == Short.class
				|| columnType == Integer.class
				|| columnType == Long.class
				|| columnType == Float.class
				|| columnType == Double.class
				;
	}
	
	public static ColumnType convert(Class<?> columnType) {
		if(columnType == Object.class)
			return ColumnType.Unknown;
		
		if(columnType == Boolean.class)
			return ColumnType.Boolean;
		
		if(columnType == String.class)
			return ColumnType.String;
		
		if(columnType == Byte.class)
			return ColumnType.Byte;
		
		if(columnType == Short.class)
			return ColumnType.Short;
		
		if(columnType == Integer.class)
			return ColumnType.Int;
		
		if(columnType == Long.class)
			return ColumnType.Long;
		
		if(columnType == Float.class)
			return ColumnType.Float;
		
		if(columnType == Double.class)
			return ColumnType.Double;
		
		if(Map.class.isAssignableFrom(columnType))
			return ColumnType.Map_ObjString;
		
		if(java.sql.Date.class.isAssignableFrom(columnType))
			return ColumnType.Date;
		
		if(java.sql.Time.class.isAssignableFrom(columnType))
			return ColumnType.Time;
		
		if(java.sql.Timestamp.class.isAssignableFrom(columnType))
			return ColumnType.Timestamp;
		
		return ColumnType.Unknown;
	}
	
	// map text format: {key1:value1,key2:value2,...}
	// simple logic, no handle value contain ':' or ','
	private static Map<Object, String> simpleParseStringMap(String text){
        if (text == null) {
            return null;
        }
        if (text.length() > 1 && text.charAt(0) == '{' && text.charAt(text.length() - 1) == '}'){
        	text = text.substring(1, text.length() - 1);
        }

        Map<Object, String> map = new HashMap<Object, String>();
        String[] nameValues = text.split(",", -1);
        if (nameValues == null) {
            return map;
        }

        for (String nameValue : nameValues) {
        	if (nameValue.trim().isEmpty())
        		continue;
            String[] nameAndValue = nameValue.trim().split(":", 2);
            String name = nameAndValue[0].trim();
            String value =  (nameAndValue.length == 2 ? nameAndValue[1].trim() : "");
            map.put(name, value);
        }

        return map;
	}
}
