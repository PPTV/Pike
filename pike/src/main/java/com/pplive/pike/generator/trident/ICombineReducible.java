package com.pplive.pike.generator.trident;

import java.util.List;

import com.pplive.pike.base.ISizeAwareIterable;

public interface ICombineReducible<TState, TResult> extends IReducible<TState, TResult> {

    // different from ICombinable<>,
	// localCombinePreFinish() only run in same task used by single emitter.
	// the TState data wont' go through network.
	// with this, it's possible use count(distinct ...) to
	// periodically calculate count in accumulated/moving time window
	
	TState localCombinePreFinish(ISizeAwareIterable<TState> states);
}
