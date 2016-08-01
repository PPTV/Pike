package com.pplive.pike.generator;

import java.util.Map;

import com.pplive.pike.Configuration;
import com.pplive.pike.parser.LogicalQueryPlan;

import backtype.storm.generated.StormTopology;

public interface ITopologyGenerator {
	public StormTopology generate(String topologyName, LogicalQueryPlan queryPlan,
									ISpoutGenerator spoutGeneragor, Map<String, Object> conf, 
									boolean debug, boolean localMode);
}
