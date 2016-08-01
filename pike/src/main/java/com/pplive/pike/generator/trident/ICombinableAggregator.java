package com.pplive.pike.generator.trident;

import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;

public interface ICombinableAggregator<T> extends Aggregator<T> {

	void completeWholeCombination(T val, TridentCollector collector);
}
