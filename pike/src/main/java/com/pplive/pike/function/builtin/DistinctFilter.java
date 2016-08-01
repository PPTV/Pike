package com.pplive.pike.function.builtin;

import java.io.Serializable;
import java.util.HashSet;

import com.pplive.pike.base.ISizeAwareIterable;

final class DistinctFilter implements Serializable {

	private static final long serialVersionUID = 5853917803924185578L;

	private final transient HashSet<Object> _values;
	
	public DistinctFilter() {
		this._values = new HashSet<Object>();
	}
	
	private DistinctFilter(HashSet<Object> values) {
		assert values != null;
		this._values = values;
	}
	
	public boolean addIfNew(Object obj) {
		if(obj == null)
			return false;
		boolean result = this._values.add(obj);
		return result;
	}
	
	public long size() {
		return this._values.size();
	}
	
	@Override
	public String toString() {
		return Long.valueOf(size()).toString();
	}
	
	public static DistinctFilter merge(ISizeAwareIterable<DistinctFilter> distinctFilters) {
		if (distinctFilters.size() == 1) {
			return distinctFilters.iterator().next();
		}
		HashSet<Object> values = new HashSet<Object>();
		for(DistinctFilter f : distinctFilters) {
			values.addAll(f._values);
		}
		return new DistinctFilter(values);
	}
}

final class DistinctFilterN implements Serializable {

	private static final long serialVersionUID = -5448579849162376842L;
	
	private final transient HashSet<ObjectsKey> _values;
	
	public DistinctFilterN() {
		this._values = new HashSet<ObjectsKey>();
	}
	
	private DistinctFilterN(HashSet<ObjectsKey> values) {
		assert values != null;
		this._values = values;
	}
	
	public boolean addIfNew(Object ... objs) {
		boolean result = this._values.add(new ObjectsKey(objs));
		return result;
	}

	public long size() {
		return this._values.size();
	}
	
	@Override
	public String toString() {
		return Long.valueOf(size()).toString();
	}
	
	public static DistinctFilterN merge(ISizeAwareIterable<DistinctFilterN> distinctFilters) {
		if (distinctFilters.size() == 1) {
			return distinctFilters.iterator().next();
		}
		HashSet<ObjectsKey> values = new HashSet<ObjectsKey>();
		for(DistinctFilterN f : distinctFilters) {
			values.addAll(f._values);
		}
		return new DistinctFilterN(values);
	}
}

final class ObjectsKey implements Serializable {

	private static final long serialVersionUID = -2686399233205682167L;
	private final Object[] _objs;
	private int _hashCode;

	public ObjectsKey(Object ... objs) {
		this._objs = objs.clone();
	}

	@Override
	public int hashCode() {
		int h = this._hashCode;
		if (h == 0) {
			for(Object o : this._objs) {
				if (o != null)
					h ^= o.hashCode();
			}
			this._hashCode = h;
		}
		return h;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || (o instanceof ObjectsKey) == false)
			return false;
		ObjectsKey other = (ObjectsKey)o;
		if (this._objs.length != other._objs.length)
			return false;
		if (hashCode() != other.hashCode())
			return false;

		for (int n = 0; n < this._objs.length; n += 1){
			Object l = this._objs[n];
			Object r = other._objs[n];
			if (l == null){
				if (r != null) return false;
			}
			else{
				if (l.equals(r) == false) return false;
			}
		}
		return true;
	}

}
