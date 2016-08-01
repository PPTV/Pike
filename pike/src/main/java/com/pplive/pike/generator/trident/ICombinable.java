package com.pplive.pike.generator.trident;

import java.io.Serializable;
import java.util.Map;

import com.pplive.pike.expression.AbstractExpression;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public interface ICombinable<TState, TResult> extends Serializable {

	void prepare(Map conf, TridentOperationContext context);
	void cleanup();

    ICombinable<TState, TResult> cloneForStateCombine(AbstractExpression stateExpr);
    
    TState evalToCombinable(TridentTuple tuple);

    TState init();
    TState combine(TState left, TState right);
	TResult finish(TState val);
}
