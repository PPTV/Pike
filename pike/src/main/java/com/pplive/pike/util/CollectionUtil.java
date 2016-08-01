package com.pplive.pike.util;

import java.util.ArrayList;
import java.util.Collection;

public class CollectionUtil {
	
	public static<T> int sizeOf(Iterable<T> items){
		if (items == null)
			throw new IllegalArgumentException("items cannot be null");
		
		if (items instanceof Collection<?>){
			return ((Collection<T>)items).size();
		}
		
		int n = 0;
		for(T item : items){
			n += 1;
		}
		return n;
	}
	
	public static<T> ArrayList<T> copyArrayList(Iterable<T> items){
		if (items == null)
			throw new IllegalArgumentException("items cannot be null");
		ArrayList<T> result;
		if (items instanceof Collection<?>){
			Collection<T> coll = (Collection<T>)items;
			result = new ArrayList<T>(coll.size());
			result.addAll(coll);
		}
		else{
			result = new ArrayList<T>(10);
			for(T item : items){
				result.add(item);
			}
		}
		return result;
	}

	private CollectionUtil() {}
}
