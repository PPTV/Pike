package com.pplive.pike.generator.trident;

import java.util.*;

import com.pplive.pike.parser.*;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopologyEx;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.builtin.Debug;

import com.pplive.pike.Configuration;
import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.Period;
import com.pplive.pike.exec.output.OutputField;
import com.pplive.pike.exec.output.OutputSchema;
import com.pplive.pike.exec.output.OutputTarget;
import com.pplive.pike.exec.spout.PikeBatchSpout;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.expression.AggregateExpression;
import com.pplive.pike.expression.ColumnExpression;
import com.pplive.pike.expression.ExpressionRefColumnsGetter;
import com.pplive.pike.generator.ISpoutGenerator;
import com.pplive.pike.generator.ITopologyGenerator;
import com.pplive.pike.generator.TopologyNotSupportedException;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.metadata.Table;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

public class TridentTopologyGenerator implements ITopologyGenerator, IRelationalOpVisitor {
	
	static class GenerateContext {
		public Map<String, Object> conf;
		public String topologyName;
		public LogicalQueryPlan queryPlan;
		public ISpoutGenerator spoutGenerator;
		public boolean debug;
		public boolean localDebug;
		public TridentTopologyEx topology;
		public Stream stream;
        public int aggregatorId;
		
		public GenerateContext(String topologyName, LogicalQueryPlan queryPlan,
				ISpoutGenerator spoutGenerator, Map<String, Object> conf, boolean debug, boolean localMode){
			this.topologyName = topologyName;
			this.queryPlan =queryPlan;
			this.spoutGenerator = spoutGenerator;
			this.debug = debug;
			this.localDebug = localMode;
			this.conf = conf;
            this.aggregatorId = 1;
		}

        public int nextAggregatorId() {
            this.aggregatorId += 1;
            return this.aggregatorId;
        }
	}
	
	public TridentTopologyGenerator() {
		
	}
	
	private static int getParallelismHint(Map<String, Object> conf) {
		return Configuration.getInt(conf, Configuration.TridentParallelismHint, 1);
	}
	
	@Override
	public StormTopology generate(String topologyName, LogicalQueryPlan queryPlan,
									ISpoutGenerator spoutGenerator, Map<String, Object> conf,
									boolean debug, boolean localMode) {
		
		final GenerateContext context = new GenerateContext(topologyName, queryPlan, spoutGenerator, conf, debug, localMode);
		context.topology = new TridentTopologyEx();
		Object ctx = context;
		RelationalExprOperator op = getLeafOp(queryPlan);
		while(op != null){
			ctx = op.accept(this, ctx);
			op = op.getParent();
		}

		final ArrayList<OutputField> outputFields = queryPlan.getOutputFields();
		final OutputSchema outputSchema = new OutputSchema(topologyName, outputFields);
		final Iterable<OutputTarget> outputTargets = queryPlan.getOutputTargets();
		final PikeTridentStateStateFactory stateFactory = new PikeTridentStateStateFactory(queryPlan.getBaseProcessPeriod(), outputSchema, outputTargets);
		final PikeTridentStateUpdater stateUpdater = new PikeTridentStateUpdater();
		
		context.stream = context.stream.global();
		Stream stream = context.stream;
		TridentState state = stream.partitionPersist(stateFactory, stream.getOutputFields(), stateUpdater);
		state = state.parallelismHint(1);
		if (conf.get("$$tridentgraph") == Boolean.TRUE) {
			return context.topology.build(true);
		}
		else {
			return context.topology.build();
		}
	}
	
	private static RelationalExprOperator getLeafOp(LogicalQueryPlan queryPlan) {
		return queryPlan.getLeafOp();
	}
	
	// methods of IRelationalOpVisitor
	public Object visit(Object ctx, DoNothingOp op) {
		return ctx;
	}
	
	public Object visit(Object ctx, LeafTableOp op) {
		GenerateContext context = (GenerateContext)ctx;
		if (op.getParent() != null && op.getParent().getClass() == ProjectOp.class){
			return context;
		}
		else{
			return visit(context, op, null);
		}
	}

	private GenerateContext visit(GenerateContext context, LeafTableOp tableOp, ProjectOp projectOp) {
		if (projectOp != null) {
			assert tableOp.getParent() == projectOp && projectOp.getChild() == tableOp;
		}
		
		Table table = tableOp.getOutputSchema();

        LogicalQueryPlan.StreamingColumns cols = context.queryPlan.getStreamingTableRequiredColumns();

		final String[] columnNames = cols.requiredColumns().toArray(new String[0]);
		final Period period = context.queryPlan.getBaseProcessPeriod();
		final String topologyName = context.topologyName;
		PikeBatchSpout spout = context.spoutGenerator.create(topologyName, table.getName(), columnNames, period, context.conf);
		
		context.stream = context.topology.newStream(spout.getSpoutName(), spout);
		context.stream.parallelismHint(getParallelismHint(context.conf)); // TODO, allow set from extended SQL syntax ?
		if (context.debug) {
			context.stream = context.stream.each(context.stream.getOutputFields(), new Debug());
		}
		context.stream = context.stream.localOrShuffle();
		
		return context;
	}
	
	public Object visit(Object ctx, RenameOp op) {
		return ctx;
	}
	
	public Object visit(Object ctx, SelectOp op) {
		GenerateContext context = (GenerateContext)ctx;

		final Map<CaseIgnoredString, Column> refColumns = ExpressionRefColumnsGetter.getAllReferencedColumns(op.conditionExpr());
		Fields inputFields = new Fields( CaseIgnoredString.toStringArray(refColumns.keySet()) );
		WhereConditionFilter filter = new WhereConditionFilter(op.conditionExpr());
		context.stream = context.stream.each(inputFields, filter);
		context.stream.parallelismHint(getParallelismHint(context.conf)); // TODO, allow set from extended SQL syntax ?
		return context;
	}
	
	public Object visit(Object ctx, ProjectOp op) {
		GenerateContext context = (GenerateContext)ctx;
		assert op.getChildren() != null && op.getChildren().length == 1;
		if (op.getChildren()[0].getClass() != LeafTableOp.class){
			context.stream = doProjectIfPossible(context.stream, op.getOutputSchema());
			context.stream.parallelismHint(getParallelismHint(context.conf)); // TODO, allow set from extended SQL syntax ?
			return context;
		}

		context = visit(context, (LeafTableOp)op.getChildren()[0], op);
		return context;
	}
	
	private static Stream doProjectIfPossible(Stream stream, RelationalExprSchema outputSchema){
		if (outputSchema.getColumns().length >= stream.getOutputFields().size()){
			assert outputSchema.getColumns().length == stream.getOutputFields().size();
			return stream;
		}
		ArrayList<String> columnNames = new ArrayList<String>(outputSchema.getColumns().length);
		for(Column c : outputSchema.getColumns()){
			assert stream.getOutputFields().contains(c.getName());
			columnNames.add(c.getName());
		}
		return stream.project(new Fields(columnNames));
	}
	
	public Object visit(Object ctx, TransformOp op) {
		GenerateContext context = (GenerateContext)ctx;
		
		ArrayList<AbstractExpression> exprs = new ArrayList<AbstractExpression>();
		ArrayList<String> outputColNames = new ArrayList<String>();
		for (TransformField f : op.getNonAggregateFields()) {
			if (f.getExpression().getClass() == ColumnExpression.class){
				ColumnExpression expr = (ColumnExpression)f.getExpression();
				if (f.getAlias().value().equals(expr.getColumnName())){
					continue;
				}
			}
			exprs.add(f.getExpression());
			outputColNames.add(f.getAlias().value());
		}

		Iterable<Column> columns = op.getProjectColumns();
		ArrayList<String> columnNames = new ArrayList<String>(op.getProjectColumnCount());
		for(Column col : columns){
			columnNames.add(col.getName());
		}
		context.stream = context.stream.each(new Fields(columnNames), new TransformFunction(exprs), new Fields(outputColNames));
		context.stream.parallelismHint(getParallelismHint(context.conf)); // TODO, allow set from extended SQL syntax ?
		context.stream = doProjectIfPossible(context.stream, op.getOutputSchema());
		context.stream.parallelismHint(getParallelismHint(context.conf)); // TODO, allow set from extended SQL syntax ?
		return context;
	}
	
	public Object visit(Object ctx, AggregateOp op) {
		GenerateContext context = (GenerateContext)ctx;
		checkSupportedAggregation(op);

        ArrayList<String> groupColumns = new ArrayList<String>();
		GroupedStream groupedStream = null;
		if (op.hasNonAggregateFields()) {
			for(TransformField f : op.getNonAggregateFields()){
				AbstractExpression expr = f.getExpression();
				if (expr.getClass() != ColumnExpression.class) {
					assert false;
					throw new IllegalStateException("bug: program run incorrectly, should never happen");
				}
				groupColumns.add(((ColumnExpression)expr).getColumnName());
			}
			for(Column col : op.getColumnsInGroupBy()){
				groupColumns.add(col.getName());
			}
			groupedStream = context.stream.groupBy(new Fields(groupColumns));
		}

		PikeChainedAggregatorDeclarer chainedAggregatorDeclarer;
		if (groupedStream != null) {
			chainedAggregatorDeclarer = PikeChainedAggregatorDeclarer.chainedAgg(groupedStream);
		} else {
			chainedAggregatorDeclarer = PikeChainedAggregatorDeclarer.chainedAggGlobal(context.stream);
		}
		
		int n = 0;
		for(TransformField f : op.getAggregateFields()){
			n += 1;
			AbstractExpression expression = f.getExpression();
			if ((expression instanceof AggregateExpression) == false){
				assert false;
				throw new IllegalStateException("bug: program run incorrectly, should never happen");
			}
			AggregateExpression expr = (AggregateExpression)expression;
			
			final Map<CaseIgnoredString, Column> refColumns = ExpressionRefColumnsGetter.getAllReferencedColumns(expr);
            final Set<CaseIgnoredString> inputColumns = new HashSet<CaseIgnoredString>(refColumns.keySet());
            for(String s : groupColumns) {
                inputColumns.add(new CaseIgnoredString(s));
            }
			final Fields inputFields = new Fields( CaseIgnoredString.toStringArray(inputColumns) );
			final Fields outputFields = new Fields(f.getAlias().value());
			
			if (expr.isCombinable()) {
				ICombinable<?,?> combiner = expr.createCombinable();
				Period basePeriod = context.queryPlan.getBaseProcessPeriod();
				Period aggregatePeriod = expr.getAggregatePeriod();
				int aggregatePeriodCount = aggregatePeriod == null ? 1 : aggregatePeriod.periodSeconds() / basePeriod.periodSeconds();

				switch(expr.getAggregateMode()){
				case Regular:
					assert aggregatePeriod == null || aggregatePeriod.equals(basePeriod);
					chainedAggregatorDeclarer = chainedAggregatorDeclarer.aggregate(inputFields, basePeriod, combiner, outputFields, groupColumns);
					break;
				case Moving:
					assert aggregatePeriodCount >= 1 && aggregatePeriod.periodSeconds() % basePeriod.periodSeconds() == 0;
					chainedAggregatorDeclarer = chainedAggregatorDeclarer.aggregateMovingStat(context.nextAggregatorId(),
                                                    inputFields, combiner, basePeriod, aggregatePeriod, outputFields, groupColumns);
					break;
				case Accumulating:
					assert aggregatePeriodCount >= 1 && aggregatePeriod.periodSeconds() % basePeriod.periodSeconds() == 0;
					chainedAggregatorDeclarer = chainedAggregatorDeclarer.aggregateAccumulatedStat(context.nextAggregatorId(),
                                                    inputFields, combiner, basePeriod, aggregatePeriod, outputFields, groupColumns);
					break;
				default:
					throw new RuntimeException("should be impossible here");
				}
			}
			else {
				IReducible<?,?> reducer = expr.createDistinctReducible();
				ICombineReducible<?, ?> combineReducer = expr.createCombineReducible();
				
				Period updatePeriod = context.queryPlan.getBaseProcessPeriod();
				Period aggregatePeriod = expr.getAggregatePeriod();
				int aggregatePeriodCount = aggregatePeriod == null ? 1 : aggregatePeriod.periodSeconds() / updatePeriod.periodSeconds();

				switch(expr.getAggregateMode()){
				case Regular:
					assert aggregatePeriod == null || aggregatePeriod.equals(updatePeriod);
					chainedAggregatorDeclarer = chainedAggregatorDeclarer.aggregate(inputFields, reducer, outputFields, groupColumns);
					break;
				case Moving:
					assert combineReducer != null;
					assert aggregatePeriodCount >= 1 && aggregatePeriod.periodSeconds() % updatePeriod.periodSeconds() == 0;
					chainedAggregatorDeclarer = chainedAggregatorDeclarer.aggregateMovingStat(context.nextAggregatorId(),
                                                    inputFields, combineReducer, updatePeriod, aggregatePeriod, outputFields, groupColumns);
					break;
				case Accumulating:
					assert combineReducer != null;
					assert aggregatePeriodCount >= 1 && aggregatePeriod.periodSeconds() % updatePeriod.periodSeconds() == 0;
					chainedAggregatorDeclarer = chainedAggregatorDeclarer.aggregateAccumulatedStat(context.nextAggregatorId(),
                                                    inputFields, combineReducer, updatePeriod, aggregatePeriod, outputFields, groupColumns);
					break;
				default:
					throw new RuntimeException("should be impossible here");
				}
			}

		}
		
		context.stream = chainedAggregatorDeclarer.chainEnd();
		context.stream = doProjectIfPossible(context.stream, op.getOutputSchema());
		context.stream.parallelismHint(getParallelismHint(context.conf)); // TODO, allow set from extended SQL syntax ?
		return context;
	}

	private static void checkSupportedAggregation(AggregateOp op) {
		// TODO: implement in future, need generate a TransformOp on AggregaetOp
		for(TransformField f : op.getAggregateFields()){
			if ( (f.getExpression() instanceof AggregateExpression) == false ){
				String msg = String.format("aggregate item (%s) in SELECT is not simple direct aggregate function call, not implemented yet.", f.getExpression().toString());
				throw new TopologyNotSupportedException(msg);
			}
		}
		
	}

    public Object visit(Object ctx, TopOp op) {
        GenerateContext context = (GenerateContext)ctx;

        if (op.getTopGroupByColumnCount() > 0) {
            ArrayList<String> groupColumns = new ArrayList<String>();
            for(ColumnExpression expr : op.getTopGroupByColumns()){
                groupColumns.add(expr.getColumnName());
            }
            GroupedStream groupedStream = context.stream.groupBy(new Fields(groupColumns));

            ArrayList<String> colNames = op.getOutputColumnsInGroup();
            Fields fields = new Fields(colNames);

            PikeChainedAggregatorDeclarer chainedAggregatorDeclarer;
            chainedAggregatorDeclarer = PikeChainedAggregatorDeclarer.chainedAgg(groupedStream);
            TopNAggregator topNAgg = new TopNAggregator(op.getTopNumber(), op.getOrderByColumnsInGroup());
            chainedAggregatorDeclarer = (PikeChainedAggregatorDeclarer)chainedAggregatorDeclarer.aggregate(fields, topNAgg, fields);
            context.stream = chainedAggregatorDeclarer.chainEnd();

            context.stream.parallelismHint(getParallelismHint(context.conf)); // TODO, allow set from extended SQL syntax ?
        }

        Fields fields = context.stream.getOutputFields();
        PikeChainedAggregatorDeclarer chainedAggregatorDeclarer;
        chainedAggregatorDeclarer = PikeChainedAggregatorDeclarer.chainedAggGlobal(context.stream);

        long topNumber = (op.getTopGroupByColumnCount() > 0 ? 0 : op.getTopNumber());
        TopNAggregator topNAgg = new TopNAggregator(topNumber, op.getOrderByColumns());
        chainedAggregatorDeclarer = (PikeChainedAggregatorDeclarer)chainedAggregatorDeclarer.aggregate(fields, topNAgg, fields);
        context.stream = chainedAggregatorDeclarer.chainEnd();
        context.stream.parallelismHint(getParallelismHint(context.conf)); // TODO, allow set from extended SQL syntax ?
        return context;
    }

    public Object visit(Object ctx, LateralOp op) {
        GenerateContext context = (GenerateContext)ctx;

        final TableGeneratingFunction func = new TableGeneratingFunction(op.functionExpr(), op.getColumnAliasCount());

        Map<CaseIgnoredString, Column> refCols = ExpressionRefColumnsGetter.getAllReferencedColumns(op.functionExpr());
        ArrayList<String> columnNames = new ArrayList<String>(refCols.size());
        for(CaseIgnoredString col : refCols.keySet()) {
            columnNames.add(col.value());
        }

        List<String> udftColumns = op.getColumnAliases();
        context.stream = context.stream.each(new Fields(columnNames), func, new Fields(udftColumns));
        context.stream.parallelismHint(getParallelismHint(context.conf)); // TODO, allow set from extended SQL syntax ?
        return context;
    }
}
