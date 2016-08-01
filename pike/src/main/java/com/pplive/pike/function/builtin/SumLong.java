package com.pplive.pike.function.builtin;

public class SumLong extends BuiltinAggBase<Long, Long> {

	private static final long serialVersionUID = 1L;

	@Override
	public Long convert(Object o){
		return super.convertLong(o);
	}
	
	@Override
	public Long combineNonNull(Long left, Long right) {
		assert left != null && right != null; 
		return left + right;
	}

	@Override
	protected Long finishNonNull(Long val) {
		assert val != null;
		return val;
	}
}

