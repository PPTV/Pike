package com.pplive.pike.biz;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.tools.ant.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pplive.pike.exec.output.FieldValuePair;


public class VVToKafkaQueue {

    private final static Logger logger = LoggerFactory.getLogger(VVToKafkaQueue.class);

    /** 存放频道当前小时的VV总数。 */
    public static final String TOPIC_REAL_TIME_VV = "RealTimeVV";

    private KafkaWriter kafkaWriter;

    public void init(@SuppressWarnings("rawtypes") Map conf) {
        kafkaWriter = new KafkaWriter();
        kafkaWriter.init(conf);
    }

    public void write(String key, Long dateTime, List<FieldValuePair> pairs) {
        long dt = 0;
        String channelID = key;
        String plt = "";
        long vv = 0;
        try {
            for (int i = 0; i < pairs.size(); i++) {
                FieldValuePair pair = pairs.get(i);
                String name = pair.getField().getName();
                Object value = getFiledValue(pair);
                if (value == null) {
                    continue;
                }
                if ("dt".equals(name)) {
                    dt = (Long) value;
                } else if ("vv".equals(name)) {
                    vv = (Long) value;
                } else if ("plt".equals(name)) {
                    plt = (String) value;
                }
            }

            if (!StringUtils.isEmpty(channelID) && !StringUtils.isEmpty(plt) && dt > 0) {
                String realTimeKey = channelID + "_" + plt + "_" + dt;
                kafkaWriter.write(TOPIC_REAL_TIME_VV, realTimeKey, String.valueOf(vv));
                logger.debug(realTimeKey + "\t" + vv);
            }
        } catch (Exception e) {
            logger.error("Error during write value to kafka: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void close() {
        if (kafkaWriter != null) {
            kafkaWriter.close();
            logger.info("close " + "kafkaWriter");
        }
    }

    public static Object getFiledValue(FieldValuePair pair) {
        if (pair == null || pair.getField() == null) {
            return null;
        }
        Object value = pair.getValue();
        switch (pair.getField().getValueType()) {
        case Short:
            Short v = ((Short) value).shortValue();
            return v;
        case Int:
            Integer iv = ((Integer) value).intValue();
            return iv;
        case Long:
            Long lv = ((Long) value).longValue();
            return lv;
        case Float:
            Float fv = ((Float) value).floatValue();
            return fv;
        case Double:
            Double dv = ((Double) value).doubleValue();
            return dv;
        case Time:
            String timeStr = DateUtils.format((Date) value, "yyyy-MM-dd");
            return timeStr;
        case Date:
            String dateStr = DateUtils.format((Date) value, "yyyy-MM-dd");
            return dateStr;
        case Timestamp:
            String timeStmapStr = String.valueOf(new Date().getTime());
            return timeStmapStr;
        case String:
            return  value;
        default:
            return value.toString();
        }
    }
}