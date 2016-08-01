package com.pplive.pike.biz;

import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.pplive.pike.Configuration;


public class KafkaWriter {

    private final static Logger logger = LoggerFactory.getLogger(KafkaWriter.class);

    Producer<String, String> producer;

    public void init(@SuppressWarnings("rawtypes") Map conf) {
        Properties props = new Properties();

        String brokerList = (String) conf.get(Configuration.KafkaBrokerList);
        if (brokerList == null || brokerList.isEmpty()) {
            brokerList = "bk0.kafka.idc.pplive.cn:9092,bk1.kafka.idc.pplive.cn:9092,bk2.kafka.idc.pplive.cn:9092";
        }

        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }

    public void write(String topic, String key, String value) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, value);
        producer.send(data);
    }

    public void close() {
        if (producer != null) {
            producer.close();
            logger.info("KafkaWriter close() ");
        }
    }
}