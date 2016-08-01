package com.pplive.pike.generator;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import com.pplive.pike.Configuration;
import com.pplive.pike.base.Period;
import com.pplive.pike.exec.spout.PikeBatchSpout;
import com.pplive.pike.exec.spout.rabbitmq.RabbitMQSpout;
import com.pplive.pike.metadata.MetaDataProvider;

public class RabbitMQSpoutGenerator implements ISpoutGenerator {

	@Override
	public void init(Configuration conf, MetaDataProvider metaDataProvider) {

	}

	@Override
	public PikeBatchSpout create(String topologyName, String tableName, String[] requiredColumns, Period period, Map<String, Object> conf){
		if (topologyName == null || topologyName.isEmpty())
			throw new IllegalArgumentException("topologyName cannot be null or empty");
		if (tableName == null || tableName.isEmpty())
			throw new IllegalArgumentException("tableName cannot be null or empty");
		if (requiredColumns == null || requiredColumns.length == 0)
			throw new IllegalArgumentException("requiredColumns cannot be null or empty");
		if (period == null)
			throw new IllegalArgumentException("period cannot be null");

		final String spoutName = generateSpoutName(topologyName, tableName);
		int n = Configuration.getInt(conf, Configuration.SpoutTaskCount, 1);
		RabbitMQSpout spout = new RabbitMQSpout(spoutName, tableName, requiredColumns, period.periodSeconds(), n);
		return spout;
	}
	
	private static String generateSpoutName(String topologyName, String tableName) {
		return String.format("%s_%s_%s" , topologyName, tableName, getNowTimeString());
	}
	
	private static String getNowTimeString() {
		return new SimpleDateFormat("mmssSSS").format(new Date());
	}
}
