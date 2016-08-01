package com.pplive.pike.metadata;

import java.util.Arrays;

import com.pplive.pike.base.Freezable;

@Freezable
public class Table {
	private boolean frozen;
	private TableDataSource tableDataSource;
	private String name;
	private String title;
	private Column[] columns;
	
	public Table(TableDataSource tableDataSource, String name, String title, Column[] columns) {
		this.frozen = true;
		this.tableDataSource = tableDataSource;
		this.name = name;
		this.title = title;
		this.columns = (columns != null ? columns.clone() : new Column[0]);
	}
	
	// default constructor is for xml deserialize
	public Table(){
		this.frozen = false;
		this.tableDataSource = TableDataSource.Streaming;
		this.name = "";
		this.title = "";
		this.columns = new Column[0];
	}
	
	public boolean isFrozen() {
		return this.frozen;
	}
	
	public void freeze() {
		this.frozen = true;
		for(int n = 0; n < this.columns.length; n +=1) {
			this.columns[n].freeze();
		}
	}

	public TableDataSource getTableDataSource() {
		return this.tableDataSource;
	}

	public void setTableDataSource(TableDataSource tableDataSource) {
		if (this.frozen)
			throw new RuntimeException("object is frozen for immutability, cannot change back");
		this.tableDataSource = tableDataSource;
	}

	public String getName() {
		return this.name == null ? "__unknown__" : this.name;
	}

	public void setName(String name) {
		if (this.frozen)
			throw new RuntimeException("object is frozen for immutability, cannot change back");
		this.name = name;
	}

	public String getTitle() {
		return this.title == null ? "__UnknownTable__" : this.title;
	}
	
	public void setTitle(String title) {
		if (this.frozen)
			throw new RuntimeException("object is frozen for immutability, cannot change back");
		this.title = title;
	}

	public Column[] getColumns() {
		return Arrays.copyOf(this.columns, this.columns.length);
	}
	
	public void setColumns(Column[] columns) {
		if (this.frozen)
			throw new RuntimeException("object is frozen for immutability, cannot change back");
		this.columns = columns.clone();
	}

	public Column getColumn(String columnName){
		for(Column col:this.columns){
			if(col.getName().equalsIgnoreCase(columnName))
				return col;
		}
		return null;
	}
	
	public int indexOfColumn(String columnName){
		if (this.columns == null)
			return -1;
		
		for(int i = 0;i<this.columns.length;i++){
			if(this.columns[i].getName().equalsIgnoreCase(columnName))
				return i;
		}
		return -1;
	}
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(String.format("%s[%s, %s]%n", getTitle(), getName(), getTableDataSource()));
		int length = this.getColumns().length;
		for (int i = 0; i < length; i++) {
			Column c = this.columns[i];
			builder.append(String.format("\t%d:%s%n", i, c));
		}
		return builder.toString();
	}

}
