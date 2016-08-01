package com.pplive.pike.function.builtin;

public class MaxDouble extends BuiltinAggBase<Double, Double> {

	private static final long serialVersionUID = 1L;

	@Override
	public Double convert(Object o){
		return super.convertDouble(o);
	}
	
	@Override
	public Double combineNonNull(Double left, Double right) {
		assert left != null && right != null; 
		return left >= right ? left : right;
	}

	@Override
	protected Double finishNonNull(Double val) {
		assert val != null;
		return val;
	}
	
}

