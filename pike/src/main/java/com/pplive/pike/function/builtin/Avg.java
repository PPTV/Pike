package com.pplive.pike.function.builtin;

import java.io.Serializable;

import com.pplive.pike.base.Immutable;

public class Avg extends BuiltinAggBase<AvgDoubleState, Double> {

	private static final long serialVersionUID = 1L;

	@Override
	public AvgDoubleState convert(Object o){
		if (o instanceof AvgDoubleState)
			return (AvgDoubleState) o;
		Double d = super.convertDouble(o);
		return d == null ? null : new AvgDoubleState(d, 1);
	}
	
	@Override
	public AvgDoubleState combineNonNull(AvgDoubleState left, AvgDoubleState right) {
		assert left != null && right != null; 
		return left.combine(right);
	}
	
	@Override
	public AvgDoubleState accumulateNonNull(AvgDoubleState accumulatedValue, Object val) {
		assert accumulatedValue != null;
		Double d = super.convertDouble(val);
		if (d == null)
			return accumulatedValue;
		return accumulatedValue.addOne(d);
	}

	@Override
	protected Double finishNonNull(AvgDoubleState state) {
		assert state != null; 
		return state.value();
	}
}

@Immutable
final class AvgDoubleState implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private final double _sum;
	public double sum() { return this._sum; }
	
	private final long _count;
	public double count() { return this._count; }
	
	public AvgDoubleState(){
		this._sum = 0;
		this._count = 0;
	}
	
	public AvgDoubleState(double sum, long count){
		this._sum = sum;
		this._count = count;
	}
	
	public AvgDoubleState combine(AvgDoubleState other) {
		if (other == null)
			return this;
		return new AvgDoubleState(this._sum + other._sum, this._count + other._count);
	}
	
	public AvgDoubleState addOne(double sum) {
		return new AvgDoubleState(this._sum + sum, this._count + 1);
	}
	
	public double value() {
		return (this._count != 0 ? this._sum / this._count : Double.NaN);
	}
	
	@Override
	public int hashCode(){
		long bits = Double.doubleToLongBits(this._sum);
		return (int)(bits ^ (bits >>> 32)) ^ (int)this._count;
	}
	
	@Override
	public boolean equals(Object o){
		if (o == this)
			return true;
		if (o == null || o.getClass() != AvgDoubleState.class)
			return false;
		AvgDoubleState other = (AvgDoubleState)o;
		return this._sum == other._sum && this._count == other._count;
	}
	
	@Override
	public String toString(){
		return Double.valueOf(value()).toString();
	}
}
