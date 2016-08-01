package com.pplive.pike.metadata;

import java.io.StringReader;
import java.util.Set;

import javax.xml.bind.annotation.XmlRootElement;

import com.pplive.pike.util.SerializeUtils;

/// used for local debug/test
public class TextFileTableInfoProvider implements ITableInfoProvider {
	
	@XmlRootElement
	private static class Tables {
		public Tables(){}
		
		public Tables(Table[] tables){
			this._tables = tables;
		}
		public Table[]  _tables;
		
		@Override
		public String toString() {
			String s = String.format("%s tables%n", this._tables.length);
			for(Table t : this._tables) {
				s += t.toString() + "\r\n";
			}
			return s;
		}
	}
	
	private Table[]  _tables;
	
	/* table info xml file example:
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<tables>
    <_tables>
        <tableDataSource>Streaming</tableDataSource>
        <name>tableA</name>
        <title>tableA</title>
        <columns>
            <name>colA1</name>
            <title>colA1</title>
				<columnType>Boolean</columnType>
        </columns>
        <columns>
            <name>colA2</name>
            <title>colA2</title>
            <columnType>Double</columnType>
        </columns>
    </_tables>
    <_tables>
        <tableDataSource>Preloadable</tableDataSource>
        <name>tableB</name>
        <title>tableB</title>
        <columns>
            <name>colB1</name>
            <title>colB1</title>
            <columnType>Int</columnType>
        </columns>
        <columns>
            <name>colB2</name>
            <title>colB2</title>
            <columnType>String</columnType>
        </columns>
    </_tables>
</tables>
	 */
	public TextFileTableInfoProvider(String tableInfoFile) {
		try {
			Tables tables = SerializeUtils.xmlDeserialize(Tables.class, tableInfoFile);
			this._tables = tables._tables;
		}
		catch(RuntimeException e){
			this._tables = new Table[0];
			System.err.println(String.format("read table info failed from file %s", tableInfoFile));
			throw(e);
		}
	}
	
	private TextFileTableInfoProvider() {
		
	}
	
	public static TextFileTableInfoProvider createDirectly(String fileContentXml) {
		try {
			TextFileTableInfoProvider result = new TextFileTableInfoProvider();
			Tables tables = SerializeUtils.xmlDeserialize(Tables.class, new StringReader(fileContentXml));
			result._tables = tables._tables;
			for(Table table : result._tables) {
				table.freeze();
			}
			return result;
		}
		catch(RuntimeException e){
			throw(e);
		}
	}
	
	public Table getTable(String name) {
		for(Table t : this._tables) {
			if (t.getName().equals(name))
				return t;
		}
		return null;
	}
	
	public String[] getTableNames() {
		String[] names = new String[this._tables.length];
		for(int n = 0; n < names.length; n +=1){
			names[n] = this._tables[n].getName();
		}
		return names;
	}
	
	public long getTableBytesByHour(String name) {
		return 0;
	}

	@Override
	public void registColumns(String id, String tableName, Set<String> columns) {
		// TODO Auto-generated method stub
		
	}
}
