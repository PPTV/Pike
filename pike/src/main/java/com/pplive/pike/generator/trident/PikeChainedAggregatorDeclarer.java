package com.pplive.pike.generator.trident;

import backtype.storm.tuple.Fields;

import java.util.*;

import com.pplive.pike.base.Period;

import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.expression.ColumnExpression;

import storm.trident.Stream;
import storm.trident.fluent.ChainedFullAggregatorDeclarer;
import storm.trident.fluent.ChainedPartitionAggregatorDeclarer;
import storm.trident.fluent.GlobalAggregationScheme;
import storm.trident.fluent.GroupedStream;
import storm.trident.fluent.IAggregatableStream;
import storm.trident.operation.AccumulateCombinableAggregator;
import storm.trident.operation.AccumulateReducibleAggregator;
import storm.trident.operation.Aggregator;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.CombinableAggregator;
import storm.trident.operation.CombineReducibleAggregator;
import storm.trident.operation.impl.CombinerAggregatorCombineImpl;
import storm.trident.operation.impl.CombinerAggregatorInitImpl;
import storm.trident.operation.impl.GlobalBatchToPartition;
import storm.trident.operation.impl.IndexHashBatchToPartition;
import storm.trident.operation.impl.ReducerAggregatorImpl;
import storm.trident.operation.impl.SingleEmitAggregator;
import storm.trident.operation.impl.SingleEmitAggregator.BatchToPartition;
import storm.trident.tuple.ComboList;

class PikeChainedAggregatorDeclarer implements ChainedFullAggregatorDeclarer, ChainedPartitionAggregatorDeclarer {    
    public static interface AggregationPartition {
        Stream partition(Stream input);
    }
    
    private static enum AggType {
        PARTITION,
        FULL,
        FULL_COMBINE
    }
    
    // inputFields can be equal to outFields, but multiple aggregators cannot have intersection outFields
    private static class AggSpec {
        Fields inFields;
        Aggregator<?> agg;
        Fields outFields;
        
        public AggSpec(Fields inFields, Aggregator<?> agg, Fields outFields) {
            this.inFields = inFields;
            this.agg = agg;
            this.outFields = outFields;
        }
    }
    
    List<AggSpec> _aggs = new ArrayList<AggSpec>();
    IAggregatableStream _stream;
    AggType _type = null;
    GlobalAggregationScheme _globalScheme;
    
    public PikeChainedAggregatorDeclarer(IAggregatableStream stream, GlobalAggregationScheme<?> globalScheme) {
        _stream = stream;
        _globalScheme = globalScheme;
    }

    public Stream chainEnd() {
        Fields[] inputFields = new Fields[_aggs.size()];
        Aggregator<?>[] aggs = new Aggregator[_aggs.size()];
        int[] outSizes = new int[_aggs.size()];
        List<String> allOutFields = new ArrayList<String>();
        Set<String> allInFields = new HashSet<String>();
        for(int i=0; i<_aggs.size(); i++) {
            AggSpec spec = _aggs.get(i);
            Fields infields = spec.inFields;
            if(infields==null) infields = new Fields();
            Fields outfields = spec.outFields;
            if(outfields==null) outfields = new Fields();

            inputFields[i] = infields;
            aggs[i] = spec.agg;
            outSizes[i] = outfields.size();  
            allOutFields.addAll(outfields.toList());
            allInFields.addAll(infields.toList());
        }
        if(new HashSet<String>(allOutFields).size() != allOutFields.size()) {
            throw new IllegalArgumentException("Output fields for chained aggregators must be distinct: " + allOutFields.toString());
        }
        
        Fields inFields = new Fields(new ArrayList<String>(allInFields));
        Fields outFields = new Fields(allOutFields);


        if(_type != AggType.FULL) {
            // NOTE: new instance that completeWholeCombination is false, since it's for combiners' intermediate level computing.
            Aggregator<?> combined = new PikeChainedAggregatorImpl(aggs, inputFields, new ComboList.Factory(outSizes), false);
            _stream = _stream.partitionAggregate(inFields, combined, outFields);
        }
        if(_type != AggType.PARTITION) {
            List<String> groupColumns = null;
            Aggregator<?> combined;
            if(_type == AggType.FULL_COMBINE) {
                groupColumns = ((CombinableAggregator)_aggs.get(0).agg).getGroupColumns();

                Aggregator<?>[] combineAggs = new Aggregator[_aggs.size()];
                Fields[] combineInputFields = new Fields[_aggs.size()];
                for(int i=0; i<_aggs.size(); i++) {
                    AggSpec spec = _aggs.get(i);
                    Fields outfields = spec.outFields;
                    if(outfields==null) outfields = new Fields();

                    ColumnExpression expr = new ColumnExpression(ColumnType.Unknown, outfields.get(0));
                    CombinableAggregator combinerAgg = (CombinableAggregator)spec.agg;
                    ICombinable<?,?> stateCombiner = combinerAgg.getAggObj().cloneForStateCombine(expr);
                    if (combinerAgg instanceof  AccumulateCombinableAggregator) {
                        combineAggs[i] = new AccumulateCombinableAggregator((AccumulateCombinableAggregator)combinerAgg, stateCombiner);
                    }
                    else {
                        combineAggs[i] = new CombinableAggregator(combinerAgg, stateCombiner);
                    }

                    List<String> inCols = new LinkedList<String>(groupColumns);
                    inCols.addAll(outfields.toList());
                    combineInputFields[i] = new Fields(inCols);
                }
                combined = new PikeChainedAggregatorImpl(combineAggs, combineInputFields, new ComboList.Factory(outSizes), true);
            }
            else {
                combined = new PikeChainedAggregatorImpl(aggs, inputFields, new ComboList.Factory(outSizes), true);
            }

            _stream = _globalScheme.aggPartition(_stream);
            BatchToPartition singleEmit = _globalScheme.singleEmitPartitioner();
            Aggregator<?> toAgg = combined;
            if(singleEmit!=null) {
                toAgg = new SingleEmitAggregator(combined, singleEmit);
            }

            if(_type == AggType.FULL_COMBINE) {
                assert groupColumns != null;
                List<String> inCols = new LinkedList<String>(groupColumns);
                inCols.addAll(outFields.toList());
                inFields = new Fields(inCols);
            }
            _stream = _stream.partitionAggregate(inFields, toAgg, outFields);
        }
        return _stream.toStream();
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(Aggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(Fields inputFields, Aggregator agg, Fields functionFields) {
        _type = AggType.PARTITION;
        _aggs.add(new AggSpec(inputFields, agg, functionFields));
        return this;
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(CombinerAggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        initCombiner(inputFields, agg, functionFields);
        return partitionAggregate(functionFields, new CombinerAggregatorCombineImpl(agg), functionFields);
    }  
    
    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(ReducerAggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        return partitionAggregate(inputFields, new ReducerAggregatorImpl(agg), functionFields);
    }  
    
    @Override
    public ChainedFullAggregatorDeclarer aggregate(Aggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }
    
    @Override
    public ChainedFullAggregatorDeclarer aggregate(Fields inputFields, Aggregator agg, Fields functionFields) {
        return aggregate(inputFields, agg, functionFields, false);
    }
    
    private ChainedFullAggregatorDeclarer aggregate(Fields inputFields, Aggregator agg, Fields functionFields, boolean isCombiner) {
        if(isCombiner) {
            if(_type == null) {
                _type = AggType.FULL_COMBINE;            
            }
        } else {
            _type = AggType.FULL;
        }
        _aggs.add(new AggSpec(inputFields, agg, functionFields));
        return this;
    }

    @Override
    public ChainedFullAggregatorDeclarer aggregate(CombinerAggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    @Override
    public ChainedFullAggregatorDeclarer aggregate(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        initCombiner(inputFields, agg, functionFields);
        return aggregate(functionFields, new CombinerAggregatorCombineImpl(agg), functionFields, true);
    }

    @Override
    public ChainedFullAggregatorDeclarer aggregate(ReducerAggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    @Override
    public ChainedFullAggregatorDeclarer aggregate(Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        return aggregate(inputFields, new ReducerAggregatorImpl(agg), functionFields);
    }
    
    private void initCombiner(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        _stream = _stream.each(inputFields, new CombinerAggregatorInitImpl(agg), functionFields);        
    }
    
    
    // -----------------------------------------------------
    // following are added methods/classes
    
    public PikeChainedAggregatorDeclarer aggregate(Fields inputFields, IReducible<?,?> agg, Fields functionFields, List<String> groupColumns) {
        return (PikeChainedAggregatorDeclarer)aggregate(inputFields, new ReducibleAggregator(agg), functionFields);
    }

    public PikeChainedAggregatorDeclarer aggregate(Fields inputFields, Period basePeriod, ICombinable<?,?> agg, Fields functionFields, List<String> groupColumns) {
        return aggregateMovingStat(0, inputFields, agg, basePeriod, basePeriod, functionFields, groupColumns);
    }

    public PikeChainedAggregatorDeclarer aggregateMovingStat(int aggregatorId, Fields inputFields, ICombinable<?,?> agg, Period basePeriod, Period aggregatePeriod, Fields functionFields, List<String> groupColumns) {
        return (PikeChainedAggregatorDeclarer)aggregate(inputFields, new CombinableAggregator(aggregatorId, basePeriod, aggregatePeriod, agg, groupColumns), functionFields, true);
    }
    
    public PikeChainedAggregatorDeclarer aggregateMovingStat(int aggregatorId, Fields inputFields, ICombineReducible<?,?> agg, Period basePeriod, Period aggregatePeriod, Fields functionFields, List<String> groupColumns) {
        return (PikeChainedAggregatorDeclarer)aggregate(inputFields, new CombineReducibleAggregator(aggregatorId, basePeriod, aggregatePeriod, agg, groupColumns), functionFields);
    }

    public PikeChainedAggregatorDeclarer aggregateAccumulatedStat(int aggregatorId, Fields inputFields, ICombinable<?,?> agg, Period basePeriod, Period aggregatePeriod, Fields functionFields, List<String> groupColumns) {
        return (PikeChainedAggregatorDeclarer)aggregate(inputFields, new AccumulateCombinableAggregator(aggregatorId, basePeriod, aggregatePeriod, agg, groupColumns), functionFields, true);
    }
    
    public PikeChainedAggregatorDeclarer aggregateAccumulatedStat(int aggregatorId, Fields inputFields, ICombineReducible<?,?> agg, Period basePeriod, Period aggregatePeriod, Fields functionFields, List<String> groupColumns) {
        return (PikeChainedAggregatorDeclarer)aggregate(inputFields, new AccumulateReducibleAggregator(aggregatorId, basePeriod, aggregatePeriod, agg, groupColumns), functionFields);
    }

    public static PikeChainedAggregatorDeclarer chainedAgg(GroupedStream groupedStream){
    	assert groupedStream != null;
    	// return groupedStream.chainedAgg();
    	return new PikeChainedAggregatorDeclarer(groupedStream, groupedStream);
    }
    
    public static PikeChainedAggregatorDeclarer chainedAgg(Stream stream){
    	assert stream != null;
    	// return context.stream.chainedAgg();
    	return new PikeChainedAggregatorDeclarer(stream, new BatchGlobalAggScheme());
    }
    
    public static PikeChainedAggregatorDeclarer chainedAggGlobal(Stream stream){
    	assert stream != null;
    	// return context.stream.chainedAgg();
    	return new PikeChainedAggregatorDeclarer(stream, new GlobalAggScheme());
    }
    
    static class BatchGlobalAggScheme implements GlobalAggregationScheme<Stream> {

        @Override
        public IAggregatableStream aggPartition(Stream s) {
            return s.batchGlobal();
        }

        @Override
        public BatchToPartition singleEmitPartitioner() {
            return new IndexHashBatchToPartition();
        }
        
    }
    
    static class GlobalAggScheme implements GlobalAggregationScheme<Stream> {

        @Override
        public IAggregatableStream aggPartition(Stream s) {
            return s.global();
        }

        @Override
        public BatchToPartition singleEmitPartitioner() {
            return new GlobalBatchToPartition();
        }
        
    }
    
}
