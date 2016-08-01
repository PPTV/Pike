package com.pplive.pike.metadata;

import com.pplive.pike.util.MetaUtil;

import java.util.Set;

/**
 * Created by jiatingjin on 2016/8/1.
 */
public class MetaDataAdapter implements ITableInfoProvider {


    private MetaDataProvider metaDataProvider;

    public MetaDataAdapter(MetaDataProvider metaDataProvider) {
        this.metaDataProvider = metaDataProvider;
    }

    @Override
    public Table getTable(String name) {
        com.pplive.pike.client.Table t = metaDataProvider.getTable(name);
        return  MetaUtil.convertTable(t);
    }

    @Override
    public String[] getTableNames() {
        return metaDataProvider.getTableNames();
    }

    public TableManager getTableManager() {
        return new TableManager(this);
    }

    @Override
    public long getTableBytesByHour(String name) {
        return 0;
    }

    @Override
    public void registColumns(String id, String tableName, Set<String> columns) {

    }
}
