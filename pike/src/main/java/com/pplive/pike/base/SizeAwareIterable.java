package com.pplive.pike.base;

import java.util.*;

public final class SizeAwareIterable<T> implements ISizeAwareIterable<T>{

	private static final class ReadonlyIterator<T> implements Iterator<T>{
		private final Iterator<T> _iter;
		public ReadonlyIterator(Iterator<T> iter){
			this._iter = iter;
		}
		
		@Override
		public boolean hasNext() {
			return this._iter.hasNext();
		}
		
		@Override
		public T next() {
			return this._iter.next();
		}
		
		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove() unsupported");
		}
	}
	
	private final Collection<T> _collection;

    public static<T> SizeAwareIterable<T> of(Collection<T> coll) {
        return new SizeAwareIterable<T>(coll);
    }

    public static<T> SizeAwareIterable<T> of(T ... objs) {
        return new SizeAwareIterable<T>(Arrays.asList(objs));
    }

    public static<T> T last(ISizeAwareIterable<T> items) {
        if (items == null){
            return null;
        }
        if (items instanceof SizeAwareIterable){
            SizeAwareIterable<T> si = (SizeAwareIterable<T>)items;
            return si.last();
        }
        T lastOne = null;
        Iterator<T> iter = items.iterator();
        while (iter.hasNext()){
            lastOne = iter.next();
        }
        return lastOne;
    }
	
	public SizeAwareIterable(Collection<T> coll){
		if (coll == null){
			throw new IllegalArgumentException("coll cannot be null");
		}
		this._collection = coll;
	}

	@Override
	public Iterator<T> iterator() {
		return new ReadonlyIterator<T>(this._collection.iterator());
	}
	
	@Override
	public int size() {
		return this._collection.size();
	}

    public T last() {
        if (size() == 0)
            return null;

        if (this._collection instanceof ArrayList) {
            return ((ArrayList<T>)this._collection).get(this.size() - 1);
        }
        T lastOne = null;
        Iterator<T> iter = this._collection.iterator();
        while (iter.hasNext()){
            lastOne = iter.next();
        }
        return lastOne;
    }
}

