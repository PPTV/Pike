package com.pplive.pike.metadata;

import com.pplive.pike.Configuration;
import com.pplive.pike.client.Table;

import java.util.HashMap;

/**
 * Created by jiatingjin on 2016/7/26.
 */
public class RawMetaDataProvider implements MetaDataProvider {

    private HashMap<String, Table> tables = new HashMap<>();

    public void addTable(com.pplive.pike.client.Table table) {
        tables.put(table.getName(), table);
    }


    @Override
    public void init(Configuration conf) {

    }

    @Override
    public Table getTable(String name) {
        return  tables.get(name);
    }

    @Override
    public String[] getTableNames() {
        return tables.keySet().toArray(new String[]{});
    }

}
