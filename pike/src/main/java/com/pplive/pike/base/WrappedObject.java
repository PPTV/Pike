package com.pplive.pike.base;

public class WrappedObject<T> {
	public T obj;
	public WrappedObject(T o){
		this.obj = o;
	}

    @Override
    public String toString() {
        return "WrappedObject: " + this.obj;
    }
}
