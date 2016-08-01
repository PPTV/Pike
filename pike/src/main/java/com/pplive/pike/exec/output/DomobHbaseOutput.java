package com.pplive.pike.exec.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.pplive.pike.base.ISizeAwareIterable;
import com.pplive.pike.base.SizeAwareIterable;

public class DomobHbaseOutput extends HBaseOutput {
    private final static Logger LOG = LoggerFactory.getLogger(DomobHbaseOutput.class);
    
    private final static int BATCH_GET_COUNT = 1000;

    private final static byte[] DOWNLOAD_COLUMN_NAME = Bytes.toBytes("channel");

    @Override
    public void write(Calendar periodEnd, ISizeAwareIterable<List<Object>> tuples) {
        if (this.valueIndexes.length <= 0) {
            throw new RuntimeException(String.format("output column can not zero.hbaseTable=%s,familyName=%s",
                this.tableName, Bytes.toString(this.familyName)));
        }

        ArrayList<List<Object>> data = this.checkEffectiveStartup(periodEnd, tuples);
        super.write(periodEnd, SizeAwareIterable.of(data));
    }

    @SuppressWarnings("static-access")
    private ArrayList<List<Object>> checkEffectiveStartup(Calendar periodEnd, ISizeAwareIterable<List<Object>> tuples) {
        ArrayList<List<Object>> data = new ArrayList<List<Object>>();

        HTable table = null;
        try {
            table = new HTable(this.hbaseConfiguration, this.tableName);

            List<Get> getList = new ArrayList<Get>(BATCH_GET_COUNT);
            Map<String, List<Object>> rowKeyData = new HashMap<String, List<Object>>(BATCH_GET_COUNT);

            Date lastDate = new Date(periodEnd.getTimeInMillis() - DateUtils.MILLIS_PER_DAY);
            Date currentDateAddOne = new Date(periodEnd.getTimeInMillis() + DateUtils.MILLIS_PER_SECOND);

            long minStamp = Long.parseLong(this.format.format(lastDate));
            long maxStamp = Long.parseLong(this.format.format(currentDateAddOne));

            Integer firstHbaseColumn = -1;
            if (this.timestampIndex >= 0 && this.valueIndexes[0] == this.timestampIndex && this.valueIndexes.length > 1) {
                firstHbaseColumn = this.valueIndexes[1];
            } else {
                firstHbaseColumn = this.valueIndexes[0];
            }

            int count = 1;
            for (List<Object> values : tuples) {
                byte[] key = this.getRowKey(values);
                if (key == null) {
                    continue;
                }

                OutputField field = this.outputSchema.getOutputField(firstHbaseColumn);
                Get get = new Get(key);
                get.addColumn(this.familyName, this.DOWNLOAD_COLUMN_NAME); // 添加right
// join表随机列，如多盟下载渠道商
                get.addColumn(this.familyName, Bytes.toBytes(field.getName())); // 添加left
// join表随机列，如启动
                // 返回在过去一天以内的数据
                get.setTimeRange(minStamp, maxStamp);
                get.setMaxVersions(1);

                getList.add(get);
                rowKeyData.put(Bytes.toString(key), values);
                if (count % BATCH_GET_COUNT == 0) {
                    Result[] results = table.get(getList);
                    for (Result result : results) {
                        if (result.isEmpty()) {
                            continue;
                        }

                        if (result.containsColumn(this.familyName, this.DOWNLOAD_COLUMN_NAME)
                            && result.list().size() == 1) {
                            String newKey = Bytes.toString(result.getRow());
                            data.add(rowKeyData.get(newKey));
                        }
                    }
                    getList.clear();
                    rowKeyData.clear();
                }
                count += 1;
            }

            if (getList.size() > 0) {
                Result[] results = table.get(getList);
                for (Result result : results) {
                    if (result.isEmpty()) {
                        continue;
                    }

                    if (result.containsColumn(this.familyName, this.DOWNLOAD_COLUMN_NAME) && result.list().size() == 1) {
                        String newKey = Bytes.toString(result.getRow());
                        data.add(rowKeyData.get(newKey));
                    }
                }
                getList.clear();
                rowKeyData.clear();
            }

            return data;
        } catch (IOException e) {
            throw new RuntimeException(String.format("%s.%s check data error", this.tableName,
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

}
