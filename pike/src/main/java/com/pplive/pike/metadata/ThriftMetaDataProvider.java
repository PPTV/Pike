package com.pplive.pike.metadata;

import com.pplive.pike.Configuration;
import com.pplive.pike.client.Table;


/**
 * Created by jiatingjin on 2016/7/28.
 */
public class ThriftMetaDataProvider implements MetaDataProvider {

    @Override
    public void init(Configuration conf) {

    }

    @Override
    public Table getTable(String name) {
        return null;
    }

    @Override
    public String[] getTableNames() {
        return new String[0];
    }
}
