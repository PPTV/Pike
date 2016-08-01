package com.pplive.pike.exec;

import org.junit.Test;
import org.junit.Assert;

import com.pplive.pike.BaseUnitTest;
import com.pplive.pike.PikeSqlCompiler;
import com.pplive.pike.metadata.TableManager;
import com.pplive.pike.metadata.TextFileTableInfoProvider;

public class PikeSqlCompilerTest  extends BaseUnitTest {

	@Test
	public void testParseSqlSyntax(){
		String sql = "select * from a";
		Assert.assertTrue(PikeSqlCompiler.parseSQLSyntax(sql));

		sql = "withperiod 5m select * from a";
		Assert.assertTrue(PikeSqlCompiler.parseSQLSyntax(sql));

		sql = "withperiod 5m select * from a group by";
		Assert.assertFalse(PikeSqlCompiler.parseSQLSyntax(sql));
	}
	

	@Test
	public void testParseSql(){
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
"<tables>" +
    "<_tables>" +
        "<name>table1</name>" +
        "<title>table1</title>" +
        "<columns>" +
            "<name>col_1a</name>" +
            "<tilte>col_1a</tilte>" +
            "<columnType>Boolean</columnType>" +
        "</columns>" +
        "<columns>" +
            "<name>col_1b</name>" +
            "<tilte>col_1b</tilte>" +
            "<columnType>Double</columnType>" +
        "</columns>" +
    "</_tables>" +
    "<_tables>" +
        "<name>table2</name>" +
        "<title>table2</title>" +
        "<columns>" +
            "<name>col_2a</name>" +
            "<tilte>col_2a</tilte>" +
            "<columnType>Int</columnType>" +
        "</columns>" +
        "<columns>" +
            "<name>col_2b</name>" +
            "<tilte>col_2b</tilte>" +
            "<columnType>String</columnType>" +
        "</columns>" +
    "</_tables>" +
"</tables>";
		
		TextFileTableInfoProvider provider = TextFileTableInfoProvider.createDirectly(xml);
		TableManager tableManager = new TableManager(provider);
		
		String sql = "select count(col_1a) from table1 frequency 5m";
		//Assert.assertNotNull(PikeSqlCompiler.parseSQL(sql, tableManager));

		sql = "select col_1a, sum(col_1b) from table1 group by col_1a frequency 5m";
		//Assert.assertNotNull(PikeSqlCompiler.parseSQL(sql, tableManager));

		sql = "select col_1A, sum(col_1b) from table1 group by col_1A frequency 5m";
		//Assert.assertNotNull(PikeSqlCompiler.parseSQL(sql, tableManager));


		sql = "select col_1a, sum(col_1c) from table1 group by col_1a frequency 5m";
		//Assert.assertNull(PikeSqlCompiler.parseSQL(sql, tableManager));

		sql = "select sum(col_1b) from table1 group by col_1a frequency 5m";
		//Assert.assertNull(PikeSqlCompiler.parseSQL(sql, tableManager));
	}
	

}
