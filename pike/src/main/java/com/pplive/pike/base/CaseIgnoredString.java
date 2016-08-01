package com.pplive.pike.base;

import java.util.ArrayList;
import java.util.Locale;

@Immutable
public final class CaseIgnoredString  implements java.io.Serializable, Comparable<CaseIgnoredString>{

	private static final long serialVersionUID = -7869531784281421152L;
	private final String _value;
	private int _hash; // though _hash is not final, it's only changed in hashCode() and impossible to be set to two different numbers,
	                   // so immutability never be break in concurrent running. this is same implementation as java.lang.String
	
	public CaseIgnoredString() {
		this._value = "";
	}
	
	public CaseIgnoredString(String s) {
		this._value = (s != null ? s : "");
	}
	
	public CaseIgnoredString(CaseIgnoredString other) {
		this._value = other._value;
		this._hash = other._hash;
	}
	
	public boolean isEmpty() {
		return this._value.isEmpty();
	}
	
	public int length() {
		return this._value.length();
	}
	
	public String value() {
		return this._value;
	}
	
	@Override
	public String toString() {
		return this._value;
	}

	@Override
	public int compareTo(CaseIgnoredString other) {
		if (other == null)
			throw new NullPointerException("CaseIgnoredString other is null"); 
		if (other._value == this._value)
			return 0;
		return this._value.compareToIgnoreCase(other._value);
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj == null){
			return false;
		}
		if(obj instanceof CaseIgnoredString){
			return equals((CaseIgnoredString)obj);
		}
		else {
			return false;
		}
	}
	
	public boolean equals(String s){
		assert false;
		throw new UnsupportedOperationException("for comparing with String, use equalsString() method");
	}
	
	public boolean equals(CaseIgnoredString other){
		if (other ==  null)
			return false;
		return this._value.equalsIgnoreCase(other._value);
	}
	
	public boolean equalsString(String s){
		if (s == null)
			return false;
		return this._value.equalsIgnoreCase(s);
	}

	@Override
	public int hashCode(){
        int h = this._hash;
        if (h == 0 && this._value.length() > 0) {
            h = this._value.toLowerCase(Locale.ENGLISH).hashCode();
            this._hash = h;
        }
        return h;
	}

	public static String[] toStringArray(Iterable<CaseIgnoredString> items) {
		ArrayList<String> result = new ArrayList<String>();
		for(CaseIgnoredString x : items)
			result.add(x.value());
		return result.toArray(new String[0]);
	}
}
