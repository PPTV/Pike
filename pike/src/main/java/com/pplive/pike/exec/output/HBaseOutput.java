package com.pplive.pike.exec.output;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tools.ant.util.DateUtils;

import com.pplive.pike.base.ISizeAwareIterable;
import com.pplive.pike.base.Period;
import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.util.Path;


public class HBaseOutput implements IPikeOutput {

    private final static Logger LOG = LoggerFactory.getLogger(HBaseOutput.class);

    @Override
    public void init(@SuppressWarnings("rawtypes") Map conf, OutputSchema outputSchema, String targetName,
            Period outputPeriod) {

        Configuration hbaseConfig = new Configuration(new Configuration());
        String hbaseFile =
            Path.combine((String) System.getenv(com.pplive.pike.Configuration.PIKE_CONF_DIR_KEY), "hbase-site.xml");
        hbaseConfig.addResource(new org.apache.hadoop.fs.Path(hbaseFile));
        this.hbaseConfiguration = hbaseConfig;
        this.outputSchema = outputSchema;
        String[] keys = ((String) conf.get(com.pplive.pike.Configuration.HBaseKeysConfigKey)).toLowerCase().split(",");
        int dotIndex = targetName.indexOf('.');
        if (dotIndex < 0) {
            this.tableName = targetName;
            this.familyName = Bytes.toBytes("default_cf"); // todo, PPTV specific, put in
// configuration
        } else {
            this.tableName = targetName.substring(0, dotIndex);
            this.familyName = Bytes.toBytes(targetName.substring(dotIndex + 1));
        }
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
        Object timestampFieldName = conf.get(com.pplive.pike.Configuration.HBaseTimestampConfigKey);
        if (timestampFieldName != null) {
            this.timestampIndex = outputSchema.indexOfColumn(timestampFieldName.toString());
        }

        try {
            hbaseConnection = HConnectionManager.createConnection(hbaseConfiguration);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected String tableName;

    protected byte[] familyName;

    protected Configuration hbaseConfiguration;

    protected OutputSchema outputSchema;

    protected Integer[] keyIndexes;

    protected Integer[] valueIndexes;

    int timestampIndex = -1;

    protected final SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

    protected HConnection hbaseConnection;

    @Override
    public void write(Calendar periodEnd, ISizeAwareIterable<List<Object>> tuples) {

        HTableInterface table = null;
        try {
            table = hbaseConnection.getTable(tableName.getBytes());

            List<Put> putList = new ArrayList<Put>();
            Long dateTimeValue = Long.parseLong(format.format(periodEnd.getTime()));
            for (List<Object> values : tuples) {
                if (this.timestampIndex >= 0) {
                    dateTimeValue = (Long) values.get(this.timestampIndex);
                }
                byte[] key = this.getRowKey(values);
                if (key == null) {
                    continue;
                }

                int valueLength = this.valueIndexes.length;
                Put put = new Put(key);
                for (int i = 0; i < valueLength; i++) {
                    Object v = values.get(this.valueIndexes[i]);
                    if (v != null) {
                        OutputField field = this.outputSchema.getOutputField(this.valueIndexes[i]);
                        put.add(this.familyName, Bytes.toBytes(field.getName()), dateTimeValue,
                            this.toByte(v, field.getValueType()));

                    }
                }
                putList.add(put);

            }
            if (putList.size() > 0) {
                loadhbase(table, putList, 0);
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("%s.%s import error", this.tableName,
                Bytes.toString(this.familyName)), e);
        } finally {
            try {
                if (table != null) {
                    table.close();
                }
            } catch (IOException e) {
                LOG.error("close hbase table error.", e);
            }
        }

    }

    protected byte[] getRowKey(List<Object> values) {
        byte[] key;
        if (this.keyIndexes.length == 1) {
            Object v = values.get(this.keyIndexes[0]);
            if (v != null) {
                key = this.toByte(v, this.outputSchema.getOutputField(this.keyIndexes[0]).getValueType());
            } else {
                key = null;
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
            key = Bytes.toBytes(StringUtils.join(keys, "\t"));
        }
        return key;
    }

    private final static int RETRY_TIME = 10;

    private void loadhbase(HTableInterface table, List<Put> putList, int retryTime) throws IOException {
        try {
            table.put(putList);
            table.flushCommits();
        } catch (IOException e) {
            retryTime++;
            LoggerFactory.getLogger(this.getClass()).warn("Try " + retryTime + " times, Fail to load Data", e);
            if (retryTime < RETRY_TIME) {
                loadhbase(table, putList, retryTime);
            } else {
                putList.clear();
                throw e;
            }
        }
        putList.clear();
    }

    protected byte[] toByte(Object value, ColumnType columnType) {

        switch (columnType) {
        case Boolean:
            if (value instanceof Boolean && !(Boolean) value)
                return Bytes.toBytes(false);
            else if (value instanceof Boolean && (Boolean) value)
                return Bytes.toBytes(true);
            throw new IllegalArgumentException("not boolean " + value.toString());
        case Byte:
            return new byte[] { ((Byte) value).byteValue() };
        case Short:
            return Bytes.toBytes(((Short) value).shortValue());
        case Int:
            return Bytes.toBytes(((Integer) value).intValue());
        case Long:
            return Bytes.toBytes(((Long) value).longValue());
        case Float:
            return Bytes.toBytes(((Float) value).floatValue());
        case Double:
            return Bytes.toBytes(((Double) value).doubleValue());
        case Time:
            return Bytes.toBytes(DateUtils.format((Date) value, "yyyy-MM-dd HH:mm:ss"));
        case Date:
            return Bytes.toBytes(DateUtils.format((Date) value, "yyyy-MM-dd"));
        case Timestamp:
            return Bytes.toBytes((new Date()).getTime());
        default:
            return Bytes.toBytes(value.toString());
        }
    }

    @Override
    public void close() throws IOException {
        hbaseConnection.close();
    }
}
