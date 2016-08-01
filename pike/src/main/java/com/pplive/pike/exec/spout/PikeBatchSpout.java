package com.pplive.pike.exec.spout;

import java.util.Arrays;
import java.util.Map;

import com.pplive.pike.base.Period;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

public abstract class PikeBatchSpout implements IBatchSpout {

	private static final long serialVersionUID = 1L;
	
	public PikeBatchSpout (String spoutName, String tableName, String[] columnNames, int periodSeconds) {
		this._spoutName = spoutName;
		this.tableName = tableName;
		this.columnNames = Arrays.copyOf(columnNames, columnNames.length);
		this.period = Period.secondsOf(periodSeconds);
	}
	
	protected final String _spoutName;
	protected final String tableName;
	protected final String[] columnNames;
	protected final Period period;
	
	public String getSpoutName() {
		return this._spoutName;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public int getPeriodSeconds(){
		return this.period.periodSeconds();
	}

	public String[] getColumnNames() {
		return columnNames.clone();
	}
	
	public Fields getOutputFields() {
		return new Fields(this.getColumnNames());
	}

	public abstract void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context);

	public abstract void emitBatch(long batchId, TridentCollector collector) ;

	public abstract void ack(long batchId);

	public abstract void close() ;

	@SuppressWarnings("rawtypes")
	public abstract Map getComponentConfiguration();

	
}
