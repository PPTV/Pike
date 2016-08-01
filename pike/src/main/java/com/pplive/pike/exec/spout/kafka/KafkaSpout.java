package com.pplive.pike.exec.spout.kafka;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import backtype.storm.Config;
import com.pplive.pike.metadata.ITableInfoProvider;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import storm.trident.operation.TridentCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

import com.pplive.pike.Configuration;
import com.pplive.pike.base.Period;
import com.pplive.pike.base.PikeTopologyClient;
import com.pplive.pike.exec.spout.PikeBatchSpout;
import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.exec.spoutproto.SpoutTupleSerializer;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.metadata.Table;
import com.pplive.pike.metadata.TableManager;

public class KafkaSpout extends PikeBatchSpout{

    private static final long serialVersionUID = 644847633930993222L;
    private static final Logger log = LoggerFactory.getLogger(KafkaSpout.class);
    private transient TopologyContext _topologyContext;
    private transient Map _conf;
    private transient String _topologyName;
    private transient String _topologyId;
    private transient PikeTopologyClient _pikeTopologyClient;
    
    private transient String _zkServers;
    private transient String _zkSessionTimeoutMs;
    private transient Charset _dataCharset;
    private transient String _fieldSeparator;
    private transient String _timeoutMilliseconds;
    private transient boolean _printQueueData;
    private transient boolean _printEmitData;
    
    private transient boolean _checkSelfKilled;
    private transient int _checkKilledIntervalSeconds;
    private transient long _lastCheckKilledTime;
    private transient boolean _outputFirstPeriod;
    private transient boolean _firstPeriod;
    
    private transient ConsumerConnector _consumer;
    private transient ConsumerIterator<byte[], byte[]> _iterator;
    
    private int[] columnsIndex;
    private ColumnType[] columnsType;
    private int _taskCount; // configured value at compile-time
    
    public KafkaSpout(String spoutName, String tableName, String[] columnNames, ITableInfoProvider tableInfoProvider, int periodSeconds, int taskCount) {
        super(spoutName, tableName, columnNames, periodSeconds);
        if (taskCount <= 0) {
            throw new IllegalArgumentException("taskCount must be > 0");
        }

        this._taskCount = taskCount;
        this.columnsIndex = new int[columnNames.length];
        this.columnsType = new ColumnType[columnNames.length];
        final TableManager manager = new TableManager(tableInfoProvider);
        Table table = manager.getTable(this.tableName);
        for (int i = 0; i < columnNames.length; i++) {
            Column column = table.getColumn(columnNames[i]);
            if (column != null) {
                this.columnsIndex[i] = table.indexOfColumn(columnNames[i]);
                this.columnsType[i] = column.getColumnType();
            }
            else {
                String msg = String.format("Table '%s' doesn't have column '%s'", tableName, columnNames[i]);
                throw new RuntimeException(msg);
            }
        }
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        // TODO Auto-generated method stub
        this._topologyContext = context;
        this._conf = conf;
        
        initConfigValues(conf);
        this.removeZKSpoutInfo();
        this.setupConnect(conf);
        this._firstPeriod = true;
    }
    
    private void initConfigValues(Map conf){
        this._topologyName = Configuration.getString(conf, Configuration.TOPOLOGY_NAME);
        this._topologyId = Configuration.getString(conf, Configuration.STORM_ID);
        this._pikeTopologyClient = PikeTopologyClient.getConfigured(conf);
        
        final String charset = Configuration.getString(conf, Configuration.SpoutRabbitmqDataCharset, "utf-8");
        this._dataCharset = Charset.forName(charset);
        
        this._zkServers = Configuration.getString(conf, Configuration.SpoutKafkaZKServers);
        this._zkSessionTimeoutMs = Configuration.getString(conf, Configuration.SpoutKafkaZKSessionTimeout, "60000");
        
        this._fieldSeparator = Configuration.getString(conf, Configuration.SpoutRabbitmqDataFieldSeparator, "\t");
        this._timeoutMilliseconds = Configuration.getString(conf, Configuration.SpoutRabbitmqTimeoutMilliseconds, "10000");
        this._printQueueData = Configuration.getBoolean(conf, Configuration.SpoutRabbitmqPrintQueueData);
        this._printEmitData = Configuration.getBoolean(conf, Configuration.SpoutRabbitmqPrintEmitData);
        
        this._checkSelfKilled = Configuration.getBoolean(conf, Configuration.SpoutRabbitmqCheckTopologyselfKilled, true);
        this._checkKilledIntervalSeconds = Configuration.getInt(conf, Configuration.SpoutRabbitmqCheckKilledIntervalSeconds, 15);
        this._outputFirstPeriod = Configuration.getBoolean(conf, Configuration.OutputFirstPeriodResult, false);
        
    }
    
    private boolean isTopologyKilled() {
        boolean res = this._pikeTopologyClient.checkTopologyKilled(this._topologyId, false, false);
        this._lastCheckKilledTime = System.currentTimeMillis();
        return res;
    }
    
    private boolean needCheckSelfKilled() {
        return nowLongerThan(this._lastCheckKilledTime, this._checkKilledIntervalSeconds);
    }

    private static boolean nowLongerThan(long compareTimePoint, int seconds) {
        return System.currentTimeMillis() - compareTimePoint >= (seconds * 1000);
    }
    
    private String logId() {
        return String.format("%s:spout:%d", this._topologyName, this._topologyContext.getThisTaskId());
    }
    
    // task number per spout/bolt can be configured on each topology,
    // worker number can also be configured on each topology,
    // but executor(thread) number is dynamic and cannot be configured/decided,
    // so it's possible a executor(thread) run several tasks, that means,
    // for each batchId, emitBatch() being called more times in same thread, one call per task.
    // for our spout, in each batch (i.e. period), we must make only first spout emit data,
    // ignore remaing ones, because the period ends when first spout task finish emit.
    private static boolean markBatchPerExecutor(long batchId, TopologyContext context) {
        final String key = "Pike_KafkaSpout_LatestBatchId";
        Object latestBatchId = context.getExecutorData(key);
        if (latestBatchId != null){
            assert latestBatchId instanceof Long;
            Long id = (Long)latestBatchId;
            if (id == batchId) {
                return false;
            }
        }
                
        context.setExecutorData(key, batchId);
        return true;
    }
    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        final boolean outputFirstPeriod = this._outputFirstPeriod;
        final boolean checkSelfKilled = this._checkSelfKilled && this._checkKilledIntervalSeconds < super.getPeriodSeconds();
        final boolean isFirstPeriod = this._firstPeriod;

        long emitCount = 0;

        log.info(String.format("[%s] emitBatch Begin, batchId %d ... ...", logId(), batchId));
        if (markBatchPerExecutor(batchId, this._topologyContext) == false){
            log.info(String.format("[%s] emitBatch ignore remaining tasks after first in same executor, batchId %d.", logId(), batchId));
            return;
        }
        if (isFirstPeriod && outputFirstPeriod == false){
            log.info(String.format("[%s] emitBatch ignore first period data, batchId %d.", logId(), batchId));
        }
        
        this._lastCheckKilledTime = System.currentTimeMillis();
        
        Calendar periodEnd = this.period.currentPeriodEnd();
        
        while (Period.nowStillBeforePeriodEnd(periodEnd)) {
            if (checkSelfKilled && needCheckSelfKilled() && isTopologyKilled()) {
                close();
                log.info(String.format("[%s] emitBatch exit, batchId %d, detect topology killed.", logId(), batchId));
                return;
            }
            
            MessageAndMetadata<byte[], byte[]> message = null;
            try {
                 message = _iterator.next();               
            } catch(final ConsumerTimeoutException e) {

            }
            
            if (message == null) {
                if (checkSelfKilled && isTopologyKilled()) {
                    log.info(String.format("[%s] emitBatch exit, batchId %d, detect topology killed after data timeout.", logId(), batchId));
                    return;
                }
                continue;
            }
            if (isFirstPeriod && outputFirstPeriod == false){
                continue;
            }                      
            
            final List<Values> tuples = getTuplesToEmit(message);
            for(Values tuple : tuples) {
                collector.emit(tuple);
                emitCount += 1;
                String time = new String(message.key());
                log.debug(StringUtils.join(tuple, this._fieldSeparator) + " " + time + "  batchId" + batchId);
                if (this._printEmitData){

//                    log.info(String.format("[%s] consume message time: %s, batchId%d", logId(), time, batchId));
//                    System.out.print(String.format("[%s] emit data: ", logId()));                    

                }
            }                       
        }
        
        if (isFirstPeriod) {
            this._firstPeriod = false;
        } 
        log.info(String.format("[%s] emitBatch end, batchId %d, %d data emitted", logId(), batchId, emitCount));
    }

    @Override
    public void ack(long batchId) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        removeZKSpoutInfo();
        if(this._consumer != null) {
            this._consumer.shutdown();
        }
    }

    @Override
    public Map getComponentConfiguration() {
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put(Configuration.TOPOLOGY_TASKS, this._taskCount);
        return map;
    }
    
    private void setupConnect(Map conf) {
        Properties props = new Properties();  
        props.put("zookeeper.connect", this._zkServers);
        props.put("zookeeper.session.timeout.ms", this._zkSessionTimeoutMs);
        props.put("group.id",   this._spoutName); 
        props.put("consumer.timeout.ms", this._timeoutMilliseconds);

        log.info(String.format("topology group id[%s] setup connect to Kafka Zookeeper[%s]", this._spoutName, this._zkServers));
        this._consumer =  Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        // create a stream of messages from _consumer using the streams as defined on construction
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        String topic = this.tableName.toLowerCase();
        topicCountMap.put(topic, new Integer(1));
        final Map<String, List<KafkaStream<byte[], byte[]>>> streams = _consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream =  streams.get(topic).get(0);   //one stream, one queue, one fetch thread
        _iterator = stream.iterator();
    }
    
    private List<Values> getTuplesToEmit(MessageAndMetadata<byte[], byte[]> message) {
        byte[] queueData = message.message();
        if (isSpoutProtoData_Version2(queueData)) {
            return deserializeTuplesToEmit(queueData, message);
        }
        else if (isSpoutProtoData_Version1(queueData)) {
            Values v = deserializeDataToEmit(queueData, message);
            return Arrays.asList(v);
        }
        else {
            Values v = parseTextDataToEmit(queueData, message);
            return Arrays.asList(v);
        }
    }
    
    private Values getDataToEmit(MessageAndMetadata<byte[], byte[]> message) {
        byte[] queueData = message.message();
        if (isSpoutProtoData_Version1(queueData)) {
            return deserializeDataToEmit(queueData, message);
        }
        else {
            return parseTextDataToEmit(queueData, message);
        }
    }
    
    private static boolean isSpoutProtoData_Version1(byte[] data){
        assert data != null;
        return SpoutTupleSerializer.isSpoutProtoData_Version1(data);
    }
    
    private static boolean isSpoutProtoData_Version2(byte[] data){
        assert data != null;
        return SpoutTupleSerializer.isSpoutProtoData_Version2(data);
    }
    
    private List<Values> deserializeTuplesToEmit(byte[] queueData, MessageAndMetadata<byte[], byte[]> message) {
        LinkedList<Values> values = new LinkedList<Values>();

        SpoutTupleSerializer serializer = new SpoutTupleSerializer();
        serializer.beginDeserializeTuples(queueData);
        while(serializer.hasMoreToDeserialize()){
            byte[] oneTupleData = serializer.nextSerializedTuple();
            Values v = deserializeDataToEmit(oneTupleData, message);
            values.add(v);
        }
        return values;
    }
    
    private Values deserializeDataToEmit(byte[] queueData, MessageAndMetadata<byte[], byte[]> message) {
        LinkedList<SpoutTupleSerializer.TupleField> fields = null;
        final boolean printQueueData = this._printQueueData;
        if (printQueueData){
            fields = new LinkedList<SpoutTupleSerializer.TupleField>();
        }

        final Object[] tuple = new Object[this.columnsIndex.length];
        SpoutTupleSerializer serializer = new SpoutTupleSerializer();
        serializer.beginDeserializeTuple(queueData);
        while(serializer.hasMoreToDeserialize()){
            SpoutTupleSerializer.TupleField tupleField = serializer.nextField();
            if (printQueueData){
                fields.add(tupleField);
            }
            int n = findPosition(tupleField);
            if(n >= 0){
                tuple[n] = tupleField.value();
            }
        }
        serializer.endDeserialize();

        if (printQueueData){
            StringBuilder sb = new StringBuilder(1000);
            //delivery.getProperties().appendPropertyDebugStringTo(sb);
            System.out.print(String.format("[%s] queue data properties: %s%n", logId(), sb));
            System.out.print(String.format("[%s] queue data: ", logId()));
            for(SpoutTupleSerializer.TupleField f : fields){
                System.out.print(f);
                System.out.print(", ");
            }
            System.out.println();
        }
        return new Values(tuple);
    }

    private int findPosition(SpoutTupleSerializer.TupleField tupleField){
        // TODO Performance:
        // consider using Map<int, int> to optimize speed when columnsIndex.length is relatively large
        // currently it's not big issue when the length < ~10
        for (int n = 0; n < this.columnsIndex.length; n += 1){
            if (tupleField.columnIndex() == this.columnsIndex[n]){
                assert tupleField.columnType() == this.columnsType[n];
                return n;
            }
        }
        return -1;
    }
    
    
    private Values parseTextDataToEmit(byte[] queueData, MessageAndMetadata<byte[], byte[]> message) {
        final String data = new String(queueData, this._dataCharset);
        if (this._printQueueData) {
            StringBuilder sb = new StringBuilder(1000);
            //delivery.getProperties().appendPropertyDebugStringTo(sb);
            System.out.print(String.format("[%s] queue data properties: %s%n", logId(), sb));
            System.out.print(String.format("[%s] queue data: ", logId()));
            System.out.println(data);
        }

        final String fieldSeparator = this._fieldSeparator;
        final String[] fields = data.split(fieldSeparator, -1);
        final Values tuple = getDataToEmit(fields);
        return tuple;
    }
    
    private Values getDataToEmit(String[] fields) {
        final Object[] tuple = new Object[this.columnsIndex.length];
        int n = 0;
        for (int index : this.columnsIndex) {
            ColumnType columnType = this.columnsType[n];
            Object columnValue;
            if (index < fields.length) {
                columnValue = columnType.tryParse(fields[index]);
            } else {
                columnValue = null;
            }
            tuple[n] = columnValue;
            n += 1;
        }
        return new Values(tuple);
    }

    private void reconnect() {
        log.info(String.format("[%s] Reconnecting to Kafka broker...", logId()));
        try {
            setupConnect(this._conf);
        } catch (Exception e) {
            throw new RuntimeException(String.format("[%s] Fail to Reconnecting to Kafka broker ", logId()), e);
        }
    }

    private void removeZKSpoutInfo() {
        String path = "/consumers/" + _spoutName;
        try {
            ZKState zkState = new ZKState(_conf);
            zkState.deletePath(path);
            zkState.close();
        }catch(Exception e) {
            e.printStackTrace();
            log.warn("delete spout info in zk error:" + path);
        }
    }

    class ZKState {
        CuratorFramework _curator;
        public ZKState(Map conf) {
            try {
                _curator = CuratorFrameworkFactory.newClient(Configuration.getString(conf, Configuration.SpoutKafkaZKServers),
                        new RetryNTimes(Configuration.getInt(conf, Config.STORM_ZOOKEEPER_RETRY_TIMES),
                                Configuration.getInt(conf, Config.STORM_ZOOKEEPER_RETRY_INTERVAL)));
                _curator.start();
            }catch(Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void deletePath(String path) {
            try {
                if(_curator.checkExists().forPath(path) != null) {
                    List<String> children = _curator.getChildren().forPath(path);
                    for(String child : children) {
                        deletePath(path + "/" + child);
                    }
                    _curator.delete().forPath(path);
                }
            }catch(Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void close() {
            _curator.close();
            _curator = null;
        }
    }
}
