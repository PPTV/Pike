package com.pplive.pike.util;

import com.pplive.pike.client.ColumnType;
import com.pplive.pike.metadata.Table;
import com.pplive.pike.metadata.TableDataSource;

/**
 * Created by jiatingjin on 2016/7/28.
 */
public class MetaUtil {

    public static com.pplive.pike.exec.spoutproto.ColumnType convertColumnType(ColumnType type) {
        switch (type) {
            case Boolean:
                return com.pplive.pike.exec.spoutproto.ColumnType.Boolean;
            case String:
                return com.pplive.pike.exec.spoutproto.ColumnType.String;
            case Int:
                return com.pplive.pike.exec.spoutproto.ColumnType.Int;
            case Double:
                return com.pplive.pike.exec.spoutproto.ColumnType.Double;
            case Long:
                return com.pplive.pike.exec.spoutproto.ColumnType.Long;
            case Float:
                return com.pplive.pike.exec.spoutproto.ColumnType.Float;
            case Byte:
                return com.pplive.pike.exec.spoutproto.ColumnType.Byte;
            case Short:
                return com.pplive.pike.exec.spoutproto.ColumnType.Short;
            case Date:
                return com.pplive.pike.exec.spoutproto.ColumnType.Date;
            case Time:
                return com.pplive.pike.exec.spoutproto.ColumnType.Time;
            case Timestamp:
                return com.pplive.pike.exec.spoutproto.ColumnType.Timestamp;
            default:
                throw new RuntimeException("no support " + type);
        }
    }

    public static Table convertTable(com.pplive.pike.client.Table table) {
        com.pplive.pike.metadata.Column[] columns = new com.pplive.pike.metadata.Column[table.columns.length];
        for(int i = 0; i< table.columns.length; ++i) {
            columns[i] = new com.pplive.pike.metadata.Column(table.columns[i].name, convertColumnType(table.columns[i].type));
        }
        return new Table(TableDataSource.Streaming, table.name, table.name, columns);
    }
}
