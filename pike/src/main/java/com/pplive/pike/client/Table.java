package com.pplive.pike.client;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;

/**
 * Created by jiatingjin on 2016/7/28.
 */
public class Table {
    @XmlElement
    public String name;

    @XmlElementWrapper(name="columns")
    @XmlElement(name="column")
    public Column[] columns;

    public Table() {
    }

    public Table(String name, Column[] columns) {
        this.name = name;
        this.columns = columns;
    }

    public String getName() {
        return name;
    }
}
