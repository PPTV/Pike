package com.pplive.pike.metadata;

import com.pplive.pike.Configuration;
import com.pplive.pike.client.Table;


/**
 * Created by jiatingjin on 2016/8/1.
 */
public interface MetaDataProvider {

    void init(Configuration conf);

    Table getTable(String name);

    String[] getTableNames();
}
