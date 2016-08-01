package com.pplive.pike.exec.spout;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.pplive.pike.AppOptions;
import com.pplive.pike.Configuration;
import com.pplive.pike.base.Period;
import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.metadata.Table;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.metadata.ITableInfoProvider;

import storm.trident.operation.TridentCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

/// used for local debug/test
public class TextFileSpout extends PikeBatchSpout {
	
	private static final long serialVersionUID = 6087746169278938858L;

	private final String _filePath;
	private final ColumnType[] _columnsType;

	private transient TopologyContext _topologyContext;
	private transient String _topologyName;
	private transient boolean _outputFirstPeriod;
	private transient boolean _firstPeriod;
	
	public TextFileSpout(String spoutName, String file, ITableInfoProvider provider, String tableName, int periodSeconds) {
		super(spoutName, tableName, getTableColumns(provider, tableName), periodSeconds);
		this._filePath = file;
		this._columnsType = getTableColumnsType(provider, tableName);
	}
	
	private static String[] getTableColumns(ITableInfoProvider provider, String tableName) {
		Table table = provider.getTable(tableName);
		if (table == null) {
			assert false;
			throw new RuntimeException("cannot find info of table " + tableName);
		}
		Column[] columns = table.getColumns();
		String[] result = new String[columns.length];
		for(int i = 0; i < result.length; i+=1) {
			result[i] = columns[i].getName();
		}
		return result;
	}

	private static ColumnType[] getTableColumnsType(ITableInfoProvider provider, String tableName) {
		Table table = provider.getTable(tableName);
		if (table == null) {
			assert false;
			throw new RuntimeException("cannot find info of table " + tableName);
		}
		Column[] columns = table.getColumns();
		ColumnType[] result = new ColumnType[columns.length];
		for(int i = 0; i < result.length; i+=1) {
			result[i] = columns[i].getColumnType();
		}
		return result;
	}

	private transient BufferedReader _fileReader;
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		this._topologyContext = context;
		this._topologyName = Configuration.getString(conf, Configuration.TOPOLOGY_NAME);
		this._outputFirstPeriod = Configuration.getBoolean(conf, Configuration.OutputFirstPeriodResult, false);
		this._firstPeriod = true;

		try{
			this._fileReader = new BufferedReader(new InputStreamReader(new FileInputStream(this._filePath), "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("should never happen: utf-8 encoding not support", e);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("file not found: " + this._filePath, e);
		}
		
		readFileHeader();
	}
	
	private void readFileHeader() {
		try {
			String line = this._fileReader.readLine();;
			while (line != null && line.trim().equals("data begin:") == false){
				parseSpeedControlInstruction(line);
				line = this._fileReader.readLine();
				
			}
		}
		catch(IOException e){
			closeFile();
		}
	}
	
	// line format: speed: every <NUMBER> line stop <NUMBER> ms
	//          or: speed: every empty line stop <NUMBER> ms
	private static Pattern _pattern1 = Pattern.compile("^\\s*speed:\\s*every\\s+(\\d+)\\s+line\\s+stop\\s+(\\d+)\\s*ms\\s*$", Pattern.CASE_INSENSITIVE);
	private static Pattern _pattern2 = Pattern.compile("^\\s*speed:\\s*every\\s+empty\\s+line\\s+stop\\s+(\\d+)\\s*ms\\s*$", Pattern.CASE_INSENSITIVE);
	private boolean parseSpeedControlInstruction(String line){
		Matcher matcher = _pattern1.matcher(line);
		if (matcher.find()){
			this._speedStopOnEmptyLine = false;
			this._speedReadlines = Integer.parseInt(matcher.group(1));
			this._speedStopMilliseconds = Integer.parseInt(matcher.group(2));
			return true;
		}
		matcher = _pattern2.matcher(line);
		if (matcher.find()){
			this._speedStopOnEmptyLine = true;
			this._speedReadlines = 0;
			this._speedStopMilliseconds = Integer.parseInt(matcher.group(1));
			return true;
		}
		return false;
	}
	
	private int _speedReadlines;
	private int _speedStopMilliseconds;
	private boolean _speedStopOnEmptyLine; 
	
	private static Pattern _patternControl = Pattern.compile("^\\s*control:", Pattern.CASE_INSENSITIVE);
	private static boolean isControlLine(String line){
		return _patternControl.matcher(line).find();
	}
	
	private static Pattern _patternLineStop = Pattern.compile("^\\s*control:\\s+stop\\s+(\\d+)\\s*ms\\s*$", Pattern.CASE_INSENSITIVE);
	private static int parseStopMilliseconds(String line){
		Matcher matcher = _patternLineStop.matcher(line);
		if (matcher.find()){
			return Integer.parseInt(matcher.group(1));
		}
		else{
			return 0;
		}
	}

	private String logId() {
		return String.format("%s:spout:%d", this._topologyName, this._topologyContext.getThisTaskId());
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		final boolean outputFirstPeriod = this._outputFirstPeriod;
		final boolean isFirstPeriod = this._firstPeriod;
		
		if (isFirstPeriod) {
			this._firstPeriod = false;
		}
		
		if (AppOptions.getInstance().isLocalMode()){
			System.out.println("TextFileSpout.emitBatch(): newBatch, batchId=" + batchId);
		}
		if (isFirstPeriod && outputFirstPeriod == false){
			System.out.println(String.format("[%s] emitBatch ignore first period data, batchId %d.", logId(), batchId));
		}
		
		long lineCount = 0;
		int stopMilliseconds = 0;
		Calendar periodEnd = this.period.currentPeriodEnd();
		while(Period.nowStillBeforePeriodEnd(periodEnd)){
			if (isFirstPeriod && outputFirstPeriod == false){
				sleep(1);
				continue;
			}
			if (this._fileReader == null) {
				sleep(1000);
				continue;
			}
			
			if (this._speedReadlines > 0 && (lineCount % this._speedReadlines) == 0){
				sleep(this._speedStopMilliseconds);
			}

			String line;
			try{
				line = this._fileReader.readLine();
				if (line == null){
					closeFile();
					continue;
				}
			}
			catch(IOException e){
				closeFile();
				continue;
			}
			
			lineCount += 1;
			if (isControlLine(line)){
				if ((stopMilliseconds = parseStopMilliseconds(line)) > 0){
					sleep(stopMilliseconds);
				}
				else{
					System.err.println("warning: unkown control line instruction");
				}
				continue;
			}
			else if (this._speedStopOnEmptyLine && line.trim().isEmpty()){
				sleep(this._speedStopMilliseconds);
				continue;
			}

			String[] fields = line.split("\t", -1);
			Object[] tupleFields = new Object[this._columnsType.length];
			if (fields.length < tupleFields.length) {
				//System.err.println("warning: the data in spout file has fields less than table columns");
			}
			int len = tupleFields.length;
			for (int n = 0; n < len; n += 1) {
				ColumnType columnType = this._columnsType[n];
				if (n < fields.length){
					String columnValString = fields[n];
					tupleFields[n] = columnType.tryParse(columnValString);
				}
				else{
					tupleFields[n] = null;
				}
			}
			collector.emit(new Values(tupleFields));
		}
	}
	
	private static void sleep(long milliseconds) {
		try {
			Thread.sleep(milliseconds);
		}
		catch(InterruptedException e){}
	}

	@Override
	public void ack(long batchId) {
		
	}
	
	private void closeFile(){
		if (this._fileReader != null) {
			try{
				this._fileReader.close();
			}
			catch(IOException e){}
			this._fileReader = null;
		}
	}

	@Override
	public void close() {
		closeFile();
	}

	@Override
	public @SuppressWarnings("rawtypes") Map getComponentConfiguration() {
		return null;
	}
}
