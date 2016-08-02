package com.pplive.examples.meta.provider;

import com.pplive.pike.Configuration;
import com.pplive.pike.client.Column;
import com.pplive.pike.client.ColumnType;
import com.pplive.pike.client.Table;
import com.pplive.pike.metadata.SimpleMetaDataProvider;

/**
 * Created by jiatingjin on 2016/8/3.
 */
public class MyRawMetaDataProvider extends SimpleMetaDataProvider {
    @Override
    public void init(Configuration conf) {
        Column[] columns = new Column[3];
        columns[0] = new Column("channel", ColumnType.Long);
        columns[1] = new Column("vvid", ColumnType.String);
        columns[2] = new Column("user", ColumnType.String);
        Table t = new Table("dol_smart", columns);
        this.addTable(t);
    }
}