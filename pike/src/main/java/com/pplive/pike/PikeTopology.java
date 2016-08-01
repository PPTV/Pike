package com.pplive.pike;

import java.util.HashMap;
import java.util.Map;

import com.pplive.pike.base.Period;
import com.pplive.pike.parser.LogicalQueryPlan;

import backtype.storm.generated.StormTopology;

public class PikeTopology {

	private final StormTopology _topology;

	public StormTopology topology() {
		return this._topology;
	}

	private final HashMap<String, Object> _pikeOrStormOptions;

	public Map<String, Object> getPikeOrStormOptions() {
		return new HashMap<String, Object>(this._pikeOrStormOptions);
	}

	private final Period _period;

	public Period period() {
		return this._period;
	}

	private final LogicalQueryPlan queryPlan;

	public LogicalQueryPlan getQueryPlan() {
		return queryPlan;
	}
	private final Configuration conf;

	public Configuration getConf() {
		return conf;
	}

	

	public PikeTopology(LogicalQueryPlan queryPlan, StormTopology topology,
			Configuration conf) {

		if (queryPlan == null)
			throw new IllegalArgumentException("queryPlan cannot be null");
		if (topology == null)
			throw new IllegalArgumentException("topology cannot be null");
		this.conf = conf;
		this.queryPlan = queryPlan;
		this._pikeOrStormOptions = new HashMap<String, Object>(
				queryPlan.getParsedOptions());
		this._topology = topology;
		this._period = queryPlan.getBaseProcessPeriod();
	}
}
