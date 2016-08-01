package com.pplive.pike.function.builtin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.generator.trident.ICombinable;

public class LinearCount {

	public static ICombinable<LinearCountState, Long> createCombinable(int maxCardinality, List<AbstractExpression> params) {
		if (params == null){
			throw new IllegalArgumentException("params cannot be null");
		}
		return new LinearCountCombiner(maxCardinality, 0, params);
	}

	public static ICombinable<LinearCountState, Long> createCombinableEx(int bitmapBytes, List<AbstractExpression> params) {
		if (params == null){
			throw new IllegalArgumentException("params cannot be null");
		}
		return new LinearCountCombiner(0, bitmapBytes, params);
	}

}

class LinearCountCombiner implements ICombinable<LinearCountState, Long> {
	
	private static final long serialVersionUID = -166056224783723673L;
	
	private final int _bitmapBytes;
	private final int _maxCardinality;
	private final ArrayList<AbstractExpression> _params;
	
	public LinearCountCombiner(int maxCardinality, int bitmapBytes, List<AbstractExpression> params) {
		assert (maxCardinality > 0 && bitmapBytes == 0) || (maxCardinality == 0 && bitmapBytes > 0);
		assert params != null && params.size() > 0;

		this._maxCardinality = maxCardinality;
		this._bitmapBytes = bitmapBytes;
		this._params = new ArrayList<AbstractExpression>(params);
	}

    @Override
    public ICombinable<LinearCountState, Long> cloneForStateCombine(AbstractExpression stateExpr) {
        return new LinearCountCombiner(this._maxCardinality, this._bitmapBytes, Arrays.asList(stateExpr));
    }

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}

	@Override
	public LinearCountState evalToCombinable(TridentTuple tuple) {
		Object[] paramValues = new Object[this._params.size()];

		for (int i = 0; i < this._params.size(); i += 1) {
			paramValues[i] = this._params.get(i).eval(tuple);
		}
        if (paramValues.length == 1 && paramValues[0] instanceof LinearCountState) {
            return (LinearCountState)paramValues[0];
        }
		return new LinearCountState(paramValues);
	}

	@Override
	public LinearCountState init() {
		LinearCounting c;
		if (this._bitmapBytes == 0) {
			assert this._maxCardinality > 0;
			c= LinearCounting.Builder.onePercentError(this._maxCardinality).build();
		}
		else {
			assert this._maxCardinality == 0;
            if (this._bitmapBytes > 0)
			    c = new LinearCounting(this._bitmapBytes, true);
            else
                c = new LinearCounting(-this._bitmapBytes, false); // reserve option for old behavior for unexcepted situation
		}
		return new LinearCountState(new SerializableLinearCounting(c));
	}
	
	@Override
    public LinearCountState combine(LinearCountState left, LinearCountState right) {
		if (left == null) 
			return right;
		if (right == null)
			return left;
		return LinearCountState.combineNonNull(left, right);
	}

	@Override
	public Long finish(LinearCountState val) {
		if (val == null)
			return null;
		return val.cardinality();
	}
}
