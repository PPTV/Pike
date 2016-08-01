package com.pplive.pike.client;

import javax.xml.bind.annotation.XmlElement;

/**
 * Created by jiatingjin on 2016/7/26.
 */
public class Column {
    @XmlElement
    public String name;
    @XmlElement(name="type")
    public ColumnType type;

    public Column() {}

    public Column(String name, ColumnType type) {
        this.name = name;
        this.type = type;
    }
}
