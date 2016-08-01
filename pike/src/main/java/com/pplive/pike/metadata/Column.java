package com.pplive.pike.metadata;

import com.pplive.pike.base.Freezable;
import com.pplive.pike.exec.spoutproto.ColumnType;

@Freezable
public class Column {
	private boolean frozen;
	private String name;
	private String title;
	private ColumnType columnType;
	private String columnTypeValue;

	public Column(String name, ColumnType columnType){
		this(name, name, columnType);
	}
	
	public Column(String name, String title, ColumnType columnType) {
		this(name, title, columnType, "");
	}

	public Column(String name, String title, Class<?> columnType) {
		this(name, title, ColumnType.convert(columnType), "");
	}

	public Column(String name, String title, ColumnType columnType, String value) {
		this.frozen = true;
		this.name = name;
		this.title = title;
		this.columnType = columnType;
		this.columnTypeValue = value;
	}
	
	// default constructor is for xml deserialize
	public Column(){
		this.frozen = false;
		this.name = "";
		this.title = "";
		this.columnType = ColumnType.Boolean;
		this.columnTypeValue = "";
	}
	
	public boolean isFrozen() {
		return this.frozen;
	}
	
	public void freeze() {
		this.frozen = true;
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

	public ColumnType getColumnType() {
		return columnType;
	}
	
	public void setColumnType(ColumnType columnType) {
		if (this.frozen)
			throw new RuntimeException("object is frozen for immutability, cannot change back");
		this.columnType = columnType;
	}

	public String getColumnTypeValue() {
		return columnTypeValue;
	}
	
	public void setColumnTypeValue(String columnTypeValue) {
		if (this.frozen)
			throw new RuntimeException("object is frozen for immutability, cannot change back");
		this.columnTypeValue = columnTypeValue;
	}

	@Override
	public String toString() {
		return String.format("%s\t%s\t%s\t%s", this.name, this.title,
				this.columnType, this.columnTypeValue == null ? ""
						: this.columnTypeValue);
	}
}
