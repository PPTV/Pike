package com.pplive.pike.generator.trident;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.builtin.Debug;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.pplive.pike.Configuration;
import com.pplive.pike.base.Period;
import com.pplive.pike.exec.output.OutputField;
import com.pplive.pike.exec.output.OutputSchema;
import com.pplive.pike.exec.output.OutputTarget;
import com.pplive.pike.exec.output.OutputType;
import com.pplive.pike.exec.spout.PikeBatchSpout;
import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.expression.ColumnExpression;
import com.pplive.pike.expression.ConstantExpression;
import com.pplive.pike.expression.FunctionExpression;
import com.pplive.pike.function.builtin.BuiltinAggBase;
import com.pplive.pike.function.builtin.Count;
import com.pplive.pike.function.builtin.Str;
import com.pplive.pike.function.builtin.SumLong;
import com.pplive.pike.generator.LocalTextFileSpoutGenerator;
import com.pplive.pike.metadata.TextFileTableInfoProvider;

public class TridentTopologyGeneratorTest {
	public static void main(String[] args) {
		
		testPartitionAggregate();
	}
	
	private static void testPartitionAggregate() {
		TextFileTableInfoProvider tableInfoProvider = new TextFileTableInfoProvider("tableInfoXml.txt");
		LocalTextFileSpoutGenerator spoutGenerator = new LocalTextFileSpoutGenerator(tableInfoProvider, "tableData2.txt");
		HashMap<String, Object> conf = new HashMap<String, Object>();
		PikeBatchSpout spout = spoutGenerator.create("testTopology", "tableB", new String[]{"colb1", "colb2", "colb3", "colb4"}, Period.secondsOf(5), conf);

		TridentTopology topology = new TridentTopology();
		Stream stream = topology.newStream(spout.getSpoutName(), spout).parallelismHint(1);

		ColumnExpression expr1 = new ColumnExpression("", ColumnType.String, "colb2");
		ColumnExpression expr2_param1 = new ColumnExpression("", ColumnType.String, "colb4");
		ConstantExpression expr2_param2 = new ConstantExpression(Integer.valueOf(4));
		FunctionExpression expr2 = new FunctionExpression(Arrays.asList(expr2_param1, expr2_param2), Str.StrLeft.class);
		ColumnExpression expr3 = new ColumnExpression("", ColumnType.String, "colb4");
		List<AbstractExpression> exprs = Arrays.asList( (AbstractExpression)expr2 );
		stream = stream.each(new Fields("colb2", "colb4"), new TransformFunction(exprs), new Fields("colb4_begin"));

		//stream = stream.each(stream.getOutputFields(), new Debug());

		GroupedStream groupedStream = stream.groupBy(new Fields("colb2", "colb4_begin"));

		PikeChainedAggregatorDeclarer chainedAggregatorDeclarer = PikeChainedAggregatorDeclarer.chainedAgg(groupedStream);

		ColumnExpression columnExpr = new ColumnExpression("", ColumnType.String, "colb4");
		chainedAggregatorDeclarer = chainedAggregatorDeclarer.aggregate(new Fields("colb4"),
												Count.createDistinctReducible(columnExpr),
												new Fields("partition_count"), null);
		
		stream = chainedAggregatorDeclarer.chainEnd();
		//stream = stream.each(stream.getOutputFields(), new Debug());
		
		groupedStream = stream.groupBy(new Fields("colb2"));
		chainedAggregatorDeclarer = PikeChainedAggregatorDeclarer.chainedAgg(groupedStream);

		columnExpr = new ColumnExpression("", ColumnType.String, "partition_count");	
		chainedAggregatorDeclarer = chainedAggregatorDeclarer.aggregate(new Fields("partition_count"),
												Period.secondsOf(5), BuiltinAggBase.createCombinable(SumLong.class, columnExpr),
												new Fields("final_count"), null);

			
		stream = chainedAggregatorDeclarer.chainEnd();
		stream = stream.each(stream.getOutputFields(), new Debug());


		ArrayList<OutputField> outputFields = new ArrayList<OutputField>();
		outputFields.add(new OutputField("colb2", ColumnType.String));
		outputFields.add(new OutputField("final_count", ColumnType.Long));
		OutputSchema outSchema = new OutputSchema("aggregateTestTopology", outputFields);
		
		OutputTarget[] outTargets = new OutputTarget[]{ new OutputTarget(OutputType.Console, "local", Period.secondsOf(5)) };
		final PikeTridentStateStateFactory stateFactory = new PikeTridentStateStateFactory(Period.secondsOf(5), outSchema, Arrays.asList(outTargets));
		final PikeTridentStateUpdater stateUpdater = new PikeTridentStateUpdater();
		TridentState state = stream.partitionPersist(stateFactory, stream.getOutputFields(), stateUpdater);
		state = state.parallelismHint(1);
		
		StormTopology stormTopology = topology.build();
		runTopology("aggregateTestTopology", stormTopology);
	}
	
	private static void runTopology(String topologyName, StormTopology stormTopology) {

		if (stormTopology == null)
			return;

		Configuration conf = new Configuration();
		conf.setMessageTimeoutSecs(10 * 60);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, conf, stormTopology);
		
		try {
			while(true){
				Thread.sleep(1000);
			}
		}
		catch(InterruptedException e){
			cluster.shutdown();
		}
	}
}
