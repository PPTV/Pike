package com.pplive.pike.metadata;

import java.util.Set;

/**
 * 表结构元数据提供
 * @author barryshen
 *
 */
public interface ITableInfoProvider {
	/**
	 * 根据表名获取表结构
	 * @param name 表名
	 * @return 表结构
	 */
	public Table getTable(String name);
	/**
	 * 获取所有表名
	 * @return 表列表
	 */
	public String[] getTableNames();
	/**
	 * 根据表名获取每小时的数据量
	 * @param name
	 * @return
	 */
	public long getTableBytesByHour(String name);
	
	public void registColumns(String id,String tableName,Set<String> columns) ;
	
	
}
