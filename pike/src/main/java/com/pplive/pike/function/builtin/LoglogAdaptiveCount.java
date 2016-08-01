package com.pplive.pike.function.builtin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.generator.trident.ICombinable;

public class LoglogAdaptiveCount {

	public static ICombinable<LoglogAdaptiveCountState, Long> createCombinable(int k, List<AbstractExpression> params) {
		if (params == null){
			throw new IllegalArgumentException("params cannot be null");
		}
		return new LoglogAdaptiveCountCombiner(k, params);
	}

}

class LoglogAdaptiveCountCombiner implements ICombinable<LoglogAdaptiveCountState, Long> {
	
	private static final long serialVersionUID = 1L;
	
	private final int _k;
	private final ArrayList<AbstractExpression> _params;
	
	public LoglogAdaptiveCountCombiner(int k, List<AbstractExpression> params) {
		assert k > 0;
		assert params != null && params.size() > 0;

		this._k = k;
		this._params = new ArrayList<AbstractExpression>(params);
	}

    @Override
    public ICombinable<LoglogAdaptiveCountState, Long> cloneForStateCombine(AbstractExpression stateExpr) {
        return new LoglogAdaptiveCountCombiner(this._k, Arrays.asList(stateExpr));
    }

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}

	@Override
	public LoglogAdaptiveCountState evalToCombinable(TridentTuple tuple) {
		Object[] paramValues = new Object[this._params.size()];

		for (int i = 0; i < this._params.size(); i += 1) {
			paramValues[i] = this._params.get(i).eval(tuple);
		}

        if (paramValues.length == 1 && paramValues[0] instanceof LoglogAdaptiveCountState) {
            return (LoglogAdaptiveCountState)paramValues[0];
        }
		return new LoglogAdaptiveCountState(paramValues);
	}

	@Override
	public LoglogAdaptiveCountState init() {
		AdaptiveCounting c = new AdaptiveCounting(this._k);
		return new LoglogAdaptiveCountState(new SerializableAdaptiveCounting(c));
	}
	
	@Override
    public LoglogAdaptiveCountState combine(LoglogAdaptiveCountState left, LoglogAdaptiveCountState right) {
		if (left == null) 
			return right;
		if (right == null)
			return left;
		return LoglogAdaptiveCountState.combineNonNull(left, right);
	}

	@Override
	public Long finish(LoglogAdaptiveCountState val) {
		if (val == null)
			return null;
		return val.cardinality();
	}
}
