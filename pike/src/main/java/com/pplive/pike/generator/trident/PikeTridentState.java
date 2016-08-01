package com.pplive.pike.generator.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.IMetricsContext;

import com.pplive.pike.base.Period;
import com.pplive.pike.base.SizeAwareIterable;
import com.pplive.pike.exec.output.OutputSchema;
import com.pplive.pike.exec.output.OutputTarget;
import com.pplive.pike.exec.output.TopologyOutputManager;
import com.pplive.pike.util.CollectionUtil;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

class PikeTridentState implements State {
	public int partitionIndex;
	public int numPartitions;
	private TopologyOutputManager _outputManager;

	public PikeTridentState(Map conf, int partitionIndex, int numPartitions, Period baseProcessPeriod, OutputSchema outputSchema, Iterable<OutputTarget> outputTargets){
		this.partitionIndex = partitionIndex;
		this.numPartitions = numPartitions;
		this._outputManager = new TopologyOutputManager(conf, baseProcessPeriod, outputSchema, outputTargets);
	}
	
	public void updateState(List<TridentTuple> tuples){
		this._outputManager.write(SizeAwareIterable.of(tuples));
	}
	
	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}
	
}

class PikeTridentStateStateFactory implements StateFactory {

	private static final long serialVersionUID = 1L;

    private final Period _baseProcessPeriod;
    private final OutputSchema _outputSchema;
	private final ArrayList<OutputTarget> _outputTargets;

	PikeTridentStateStateFactory(Period baseProcessPeriod, OutputSchema oMetadata, Iterable<OutputTarget> outputTargets) {
        this._baseProcessPeriod = baseProcessPeriod;
        this._outputSchema = oMetadata;
		this._outputTargets = CollectionUtil.copyArrayList(outputTargets);
	}
	
	@Override
	public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
		return new PikeTridentState(conf, partitionIndex, numPartitions, this._baseProcessPeriod, this._outputSchema, this._outputTargets);
	}
}

class PikeTridentStateUpdater implements StateUpdater<PikeTridentState> {

	private static final long serialVersionUID = 1L;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void updateState(PikeTridentState state, List<TridentTuple> tuples, TridentCollector collector) {
		state.updateState(tuples);
	}
}
