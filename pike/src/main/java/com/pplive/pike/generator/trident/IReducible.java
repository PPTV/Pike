package com.pplive.pike.generator.trident;

import java.io.Serializable;
import java.util.Map;

import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public interface IReducible<TState, TResult> extends Serializable {

	void prepare(Map conf, TridentOperationContext context);
	void cleanup();
	
    TState init();
    TState reduce(TState curr, TridentTuple tuple);
	TResult finish(TState val);
}
