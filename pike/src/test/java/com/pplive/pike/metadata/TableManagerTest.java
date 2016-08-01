package com.pplive.pike.metadata;

import org.apache.commons.lang.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.pplive.pike.BaseUnitTest;
@Ignore
public class TableManagerTest extends BaseUnitTest {
	//@Test
	public void showTables() {
		this.assumeScope(AppServerDependent);
		TableManager manager = new TableManager();
		System.out.println(StringUtils.join(manager.getTableNames(), ","));
	}

	//@Test
	public void descTable() {
		this.assumeScope(AppServerDependent);
		TableManager manager = new TableManager();
		String[] names = manager.getTableNames();
		for (String name : names) {
			Table table = manager.getTable(name);
			System.out.println(table.toString());
		}

	}

}
