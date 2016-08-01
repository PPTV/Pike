package com.pplive.pike.exec.output;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.lang.StringUtils;
import org.apache.tools.ant.util.DateUtils;

import com.pplive.pike.Configuration;
import com.pplive.pike.base.ISizeAwareIterable;
import com.pplive.pike.base.Period;
import com.pplive.pike.biz.VVToKafkaQueue;
import com.pplive.pike.exec.spoutproto.ColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaOutput implements IPikeOutput {

    private final static Logger logger = LoggerFactory.getLogger(KafkaOutput.class);

    protected String tableName;

    protected OutputSchema outputSchema;

    protected Integer[] keyIndexes;

    protected Integer[] valueIndexes;

    int timestampIndex = -1;

    protected final SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

    private VVToKafkaQueue vvToKafkaQueue;

    private static ExecutorService executor = Executors.newCachedThreadPool();

    @Override
    public void init(@SuppressWarnings("rawtypes") Map conf, OutputSchema outputSchema, String targetName,
            Period outputPeriod) {

        this.outputSchema = outputSchema;
        this.tableName = targetName;

        String[] keys = ((String) conf.get(com.pplive.pike.Configuration.KafkaKeysConfigKey)).toLowerCase().split(",");
        int keyLength = keys.length;
        this.keyIndexes = new Integer[keys.length];

        for (int i = 0; i < keyLength; i++) {
            this.keyIndexes[i] = outputSchema.indexOfColumn(keys[i]);
        }
        int length = outputSchema.getOutputFieldCount();
        this.valueIndexes = new Integer[length - keyLength];
        int index = 0;
        for (int i = 0; i < length; i++) {
            OutputField field = outputSchema.getOutputField(i);
            if (Arrays.binarySearch(keys, field.getName().toLowerCase()) < 0) {
                this.valueIndexes[index] = i;
                index++;
            }
        }
        vvToKafkaQueue = new VVToKafkaQueue();
        vvToKafkaQueue.init(conf);
    }

    @Override
    public void write(Calendar periodEnd, ISizeAwareIterable<List<Object>> tuples) {
        Long dateTime = Long.parseLong(format.format(periodEnd.getTime()));
        logger.info("Tuples size: " + tuples.size());
        for (List<Object> values : tuples) {
            if (timestampIndex >= 0) {
                dateTime = (Long) values.get(timestampIndex);
            }

            String key = getRowKeyAsString(values);
            List<FieldValuePair> pairs = new ArrayList<FieldValuePair>();
            int valueLength = valueIndexes.length;
            for (int i = 0; i < valueLength; i++) {
                Object v = values.get(valueIndexes[i]);
                if (v != null) {
                    OutputField field = outputSchema.getOutputField(valueIndexes[i]);
                    FieldValuePair pair = new FieldValuePair(field, v);
                    pairs.add(pair);
                }
            }
            if (pairs.size() > 0) {
                vvToKafkaQueue.write(key, dateTime, pairs);
            }
        }
        if (vvToKafkaQueue != null) {
            vvToKafkaQueue.close();
        }



/*
        FutureTask<Integer> ft = new FutureTask<Integer>(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {

                return 0;
            }
        });
        executor.submit(ft);
*/
    }

    protected String getRowKeyAsString(List<Object> values) {
        String key = "";
        if (this.keyIndexes.length == 1) {
            Object v = values.get(this.keyIndexes[0]);
            if (v != null) {
                key = this.outputAsString(v, this.outputSchema.getOutputField(this.keyIndexes[0]).getValueType());
            }
        } else {
            int length = this.keyIndexes.length;
            String[] keys = new String[length];
            for (int i = 0; i < length; i++) {
                Object v = values.get(this.keyIndexes[i]);
                if (v != null) {
                    keys[i] = v.toString();
                } else {
                    keys[i] = "";
                }
            }
            key = StringUtils.join(keys, "\t");
        }
        return key;
    }

    protected String outputAsString(Object value, ColumnType columnType) {
        switch (columnType) {
        case Boolean:
            if (value instanceof Boolean && !(Boolean) value)
                return "F";
            else if (value instanceof Boolean && (Boolean) value)
                return "T";
            throw new IllegalArgumentException("not boolean " + value.toString());
        case Byte:
            return ((Byte) value).toString();
        case Short:
            return String.valueOf((Short) value);
        case Int:
            return String.valueOf((Integer) value);
        case Long:
            return String.valueOf((Long) value);
        case Float:
            return String.valueOf((Float) value);
        case Double:
            return String.valueOf((Double) value);
        case Time:
            return DateUtils.format((Date) value, "yyyy-MM-dd HH:mm:ss");
        case Date:
            return DateUtils.format((Date) value, "yyyy-MM-dd");
        case Timestamp:
            return String.valueOf((new Date()).getTime());
        default:
            return value.toString();
        }
    }

    @Override
    public void close() throws IOException {
        logger.info("Close...");
    }
}