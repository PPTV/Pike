package com.pplive.pike.metadata;

import java.util.Set;

import com.pplive.pike.Configuration;

public class TableManager {
	private final ITableInfoProvider provider;
	
	public TableManager(){
		this(new Configuration());
	}
	
	public TableManager(Configuration conf){
		String url = conf.getSchemaThriftURL();
		this.provider = new ThriftTableInfoProvider(url);
	}
	
	public TableManager(ITableInfoProvider provider) {
		this.provider = provider;
	}
	
	public ITableInfoProvider getProvider() {
		return this.provider;
	}

	public Table getTable(String name) {
		return this.getProvider().getTable(name);

	}
	public void registColumns(String id, String tableName, Set<String> columns) {
		this.getProvider().registColumns(id, tableName, columns);
	}

	public String[] getTableNames() {
		return this.getProvider().getTableNames();
	}

	public long getTableBytesByHour(String name) {
		return this.getProvider().getTableBytesByHour(name);
	}

}
