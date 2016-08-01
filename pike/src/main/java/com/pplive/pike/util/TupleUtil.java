package com.pplive.pike.util;

import java.util.List;

import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;
import storm.trident.tuple.TridentTupleViewHelper;

public class TupleUtil {

	public static int findIndex(TridentTuple tuple, String field) {
		if (tuple == null){
			throw new IllegalArgumentException("tuple cannot be null");
		}
		if (field == null || field.isEmpty()){
			throw new IllegalArgumentException("field cannot be null or empty");
		}
		
		if (tuple instanceof TridentTupleView) {
			return TridentTupleViewHelper.findFieldIndex((TridentTupleView)tuple, field);
		}
		
		// Warning: since TridentTuple has no method to get field index,
		// we have to use tricky way, it's possible get incorrect result,
		// if tuple has two fields reference to same object.
		Object obj = tuple.getValueByField(field);

		if (obj == null)
			return -1;
		List<Object> objs = tuple.getValues();
		int n = 0;
		for(Object o : objs){
			if (o == obj)
				return n;
			n += 1;
		}
		assert false;
		return -1;
	}
}
