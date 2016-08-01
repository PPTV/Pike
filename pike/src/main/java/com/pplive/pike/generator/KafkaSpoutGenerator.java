package com.pplive.pike.generator;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import com.pplive.pike.metadata.ITableInfoProvider;
import com.pplive.pike.metadata.MetaDataAdapter;
import com.pplive.pike.metadata.MetaDataProvider;
import org.apache.commons.lang.StringUtils;

import com.pplive.pike.Configuration;
import com.pplive.pike.base.Period;
import com.pplive.pike.exec.spout.PikeBatchSpout;
import com.pplive.pike.exec.spout.kafka.KafkaSpout;

public class KafkaSpoutGenerator implements ISpoutGenerator {

    private ITableInfoProvider tableInfoProvider;

    public KafkaSpoutGenerator(ITableInfoProvider tableInfoProvider) {
        this.tableInfoProvider = tableInfoProvider;
    }

    @Override
    public void init(Configuration conf, MetaDataProvider metaDataProvider) {
        this.tableInfoProvider = new MetaDataAdapter(metaDataProvider);
    }

    @Override
    public PikeBatchSpout create(String topologyName, String tableName, String[] requiredColumns, Period period,
            Map<String, Object> conf) {
        if (StringUtils.isEmpty(topologyName)) {
            throw new IllegalArgumentException("topologyName cannot be null or empty");
        }
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("tableName cannot be null or empty");
        }
        if (requiredColumns == null || requiredColumns.length == 0) {
            throw new IllegalArgumentException("requiredColumns cannot be null or empty");
        }
        if (period == null) {
            throw new IllegalArgumentException("period cannot be null");
        }

        final String spoutName = generateSpoutName(topologyName, tableName);
        int n = Configuration.getInt(conf, Configuration.SpoutTaskCount, 1);
        KafkaSpout spout = new KafkaSpout(spoutName, tableName, requiredColumns,tableInfoProvider, period.periodSeconds(), n);
        return spout;
    }

    private static String generateSpoutName(String topologyName, String tableName) {
        //avoid consumer path in zk
        return String.format("%s_%s" , topologyName, tableName);
//        return String.format("%s_%s_%s" , topologyName, tableName, getNowTimeString());
    }
    
    private static String getNowTimeString() {
        return new SimpleDateFormat("yyMMddHHmmssSSS").format(new Date());
    }
}
