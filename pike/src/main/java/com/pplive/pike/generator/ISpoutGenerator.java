package com.pplive.pike.generator;

import java.util.Map;

import com.pplive.pike.Configuration;
import com.pplive.pike.base.Period;
import com.pplive.pike.exec.spout.PikeBatchSpout;
import com.pplive.pike.metadata.MetaDataProvider;

public interface ISpoutGenerator {

	 void init(Configuration conf, MetaDataProvider metaDataProvider);
	PikeBatchSpout create(String topologyName, String tableName, String[] requiredColumns, Period period, Map<String, Object> conf);
}
