package com.pplive.pike.exec.spout.rabbitmq;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import com.pplive.pike.exec.spoutproto.BinaryDeserializeException;
import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.exec.spoutproto.SpoutTupleDeserializeException;
import com.pplive.pike.exec.spoutproto.SpoutTupleSerializer;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import storm.trident.operation.TridentCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.pplive.pike.Configuration;
import com.pplive.pike.base.Period;
import com.pplive.pike.base.PikeTopologyClient;
import com.pplive.pike.exec.spout.PikeBatchSpout;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.metadata.Table;
import com.pplive.pike.metadata.TableManager;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

@SuppressWarnings("rawtypes")
public class RabbitMQSpout extends PikeBatchSpout {

	private static final long serialVersionUID = 6087746169278938857L;
	private static final Logger log = Logger.getLogger(RabbitMQSpout.class);
	
	private transient TopologyContext _topologyContext;
	private transient Map _conf;
	private transient Connection amqpConnection;
	private transient Channel amqpChannel;
	private transient QueueingConsumer amqpConsumer;
	private transient String amqpConsumerTag;

	private transient Charset _dataCharset;
	private transient String _fieldSeparator;
	private transient int _timeoutMilliseconds;
	private transient boolean _printQueueData;
	private transient boolean _printEmitData;

	private transient String _topologyName;
	private transient String _topologyId;
	private transient PikeTopologyClient _pikeTopologyClient;
	private transient boolean _checkSelfKilled;
	private transient int _checkKilledIntervalSeconds;
	private transient long _lastCheckKilledTime;
	private transient boolean _outputFirstPeriod;
	private transient boolean _firstPeriod;
    private transient String _exchangeName;
    private transient String _routingKey;
    private transient long _reconnectWaitMilliseconds;

	private transient boolean _closed;
	
	private int[] columnsIndex;
	private ColumnType[] columnsType;
	private int _taskCount; // configured value at compile-time

	public RabbitMQSpout(String spoutName, String tableName, String[] columnNames, int periodSeconds, int taskCount) {
		super(spoutName, tableName.toLowerCase(), columnNames, periodSeconds);
		if (taskCount <= 0) {
			throw new IllegalArgumentException("taskCount must be > 0");
		}

		this._taskCount = taskCount;
		this.columnsIndex = new int[columnNames.length];
		this.columnsType = new ColumnType[columnNames.length];
		final TableManager manager = new TableManager();
		Table table = manager.getTable(this.tableName);
		for (int i = 0; i < columnNames.length; i++) {
			Column column = table.getColumn(columnNames[i]);
			if (column != null) {
				this.columnsIndex[i] = table.indexOfColumn(columnNames[i]);
				this.columnsType[i] = column.getColumnType();
			}
			else {
				String msg = String.format("Table '%s' doesnt have column '%s'", tableName, columnNames[i]);
				throw new RuntimeException(msg);
			}
		}
	}

	@Override
	public Map getComponentConfiguration() {
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put(Configuration.TOPOLOGY_TASKS, this._taskCount);
		return map;
	}
	
	@Override
	public void open(Map conf, TopologyContext context) {
		this._closed = false;
		this._topologyContext = context;
		this._conf = conf;
		
		Long prefetchCount = Configuration.getLong(conf, Configuration.SpoutRabbitmqPrefechCount);
		if (prefetchCount == null) {
			log.warn("prefetch count config not found, Using default: 100");
			prefetchCount = 100L;
		}

		if (prefetchCount < 1) {
			throw new IllegalArgumentException(Configuration.SpoutRabbitmqPrefechCount + " must be > 0");
		}

		initConfigValues(conf);
		try {
			setupAMQP(conf);
		} catch (IOException e) {
			throw new RuntimeException("Fail to create connection to rabbitmq cluster", e);
		}

		this._firstPeriod = true;
	}
	
	private void initConfigValues(Map conf){
		this._topologyName = Configuration.getString(conf, Configuration.TOPOLOGY_NAME);
		this._topologyId = Configuration.getString(conf, Configuration.STORM_ID);
		this._pikeTopologyClient = PikeTopologyClient.getConfigured(conf);
		
		final String charset = Configuration.getString(conf, Configuration.SpoutRabbitmqDataCharset, "utf-8");
		this._dataCharset = Charset.forName(charset);
		this._fieldSeparator = Configuration.getString(conf, Configuration.SpoutRabbitmqDataFieldSeparator, "\t");
		this._timeoutMilliseconds = Configuration.getInt(conf, Configuration.SpoutRabbitmqTimeoutMilliseconds, 1000);
		this._printQueueData = Configuration.getBoolean(conf, Configuration.SpoutRabbitmqPrintQueueData);
		this._printEmitData = Configuration.getBoolean(conf, Configuration.SpoutRabbitmqPrintEmitData);
		this._checkSelfKilled = Configuration.getBoolean(conf, Configuration.SpoutRabbitmqCheckTopologyselfKilled, true);
		this._checkKilledIntervalSeconds = Configuration.getInt(conf, Configuration.SpoutRabbitmqCheckKilledIntervalSeconds, 15);
		this._outputFirstPeriod = Configuration.getBoolean(conf, Configuration.OutputFirstPeriodResult, false);
        this._exchangeName = Configuration.getString(conf, Configuration.SpoutRabbitmqExchangeName, "");
        this._routingKey = Configuration.getString(conf, Configuration.SpoutRabbitmqRoutingKey, "");
        this._reconnectWaitMilliseconds = 1000 * Configuration.getInt(conf, Configuration.SpoutRabbitmqReconnectWaitSeconds, 10);
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
	
	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		final long timeoutMilliseconds = this._timeoutMilliseconds;
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
			try {
				QueueingConsumer.Delivery delivery = amqpConsumer.nextDelivery(timeoutMilliseconds);
				if (delivery == null) {
					if (checkSelfKilled && isTopologyKilled()) {
						log.info(String.format("[%s] emitBatch exit, batchId %d, detect topology killed after data timeout.", logId(), batchId));
						return;
					}
					continue;
				}
				if (isFirstPeriod && outputFirstPeriod == false){
					continue;
				}

				final List<Values> tuples = getTuplesToEmit(delivery);
                for(Values tuple : tuples) {
                    collector.emit(tuple);
                    emitCount += 1;

                    if (this._printEmitData){
                        System.out.print(String.format("[%s] emit data: ", logId()));
                        System.out.println(StringUtils.join(tuple, this._fieldSeparator));
                    }
                }
			}
            catch(SpoutTupleDeserializeException e) {
                log.error(String.format("[%s] error on deserializing data", logId()), e);
            }
            catch(BinaryDeserializeException e) {
                log.error(String.format("[%s] error on deserializing data", logId()), e);
            }
			catch (ShutdownSignalException e) {
				if (this._closed){
					log.info(String.format("[%s] queue disconnected, spout closed, exit emitBatch loop ...", logId()));
					break;
				}
				log.warn(String.format("[%s] AMQP connection dropped, will attempt to reconnect after %d ms ...", logId(), this._reconnectWaitMilliseconds));
				Utils.sleep(this._reconnectWaitMilliseconds);
				reconnect();
			} 
			catch (InterruptedException e) {
				if (this._closed){
					log.info(String.format("[%s] wake from sleep, spout closed, exit emitBatch loop ...", logId()));
					break;
				}
				log.warn(e.getMessage(), e);
			}
		}

		if (isFirstPeriod) {
			this._firstPeriod = false;
		}
		log.info(String.format("[%s] emitBatch end, batchId %d, %d data emitted", logId(), batchId, emitCount));
	}
	
	// task number per spout/bolt can be configured on each topology,
	// worker number can also be configured on each topology,
	// but executor(thread) number is dynamic and cannot be configured/decided,
	// so it's possible a executor(thread) run several tasks, that means,
	// for each batchId, emitBatch() being called more times in same thread, one call per task.
	// for our spout, in each batch (i.e. period), we must make only first spout emit data,
	// ignore remaing ones, because the period ends when first spout task finish emit. 
	private static boolean markBatchPerExecutor(long batchId, TopologyContext context) {
		final String key = "Pike_RabbitMQSpout_LatestBatchId";
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

    private List<Values> getTuplesToEmit(QueueingConsumer.Delivery delivery) {
        byte[] queueData = delivery.getBody();
        if (isSpoutProtoData_Version2(queueData)) {
            return deserializeTuplesToEmit(queueData, delivery);
        }
        else if (isSpoutProtoData_Version1(queueData)) {
            Values v = deserializeDataToEmit(queueData, delivery);
            return Arrays.asList(v);
        }
        else {
            Values v = parseTextDataToEmit(queueData, delivery);
            return Arrays.asList(v);
        }
    }

    private Values getDataToEmit(QueueingConsumer.Delivery delivery) {
        byte[] queueData = delivery.getBody();
        if (isSpoutProtoData_Version1(queueData)) {
            return deserializeDataToEmit(queueData, delivery);
        }
        else {
            return parseTextDataToEmit(queueData, delivery);
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

    private List<Values> deserializeTuplesToEmit(byte[] queueData, QueueingConsumer.Delivery delivery) {
        LinkedList<Values> values = new LinkedList<Values>();

        SpoutTupleSerializer serializer = new SpoutTupleSerializer();
        serializer.beginDeserializeTuples(queueData);
        while(serializer.hasMoreToDeserialize()){
            byte[] oneTupleData = serializer.nextSerializedTuple();
            Values v = deserializeDataToEmit(oneTupleData, delivery);
            values.add(v);
        }
        return values;
    }

    private Values deserializeDataToEmit(byte[] queueData, QueueingConsumer.Delivery delivery) {
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
            delivery.getProperties().appendPropertyDebugStringTo(sb);
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

    private Values parseTextDataToEmit(byte[] queueData, QueueingConsumer.Delivery delivery) {
        final String data = new String(queueData, this._dataCharset);
        if (this._printQueueData) {
            StringBuilder sb = new StringBuilder(1000);
            delivery.getProperties().appendPropertyDebugStringTo(sb);
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

	@Override
	public void ack(long batchId) {
	}

	@Override
	public void close() {
		log.info(String.format("[%s] spout closing ...", logId()));
		this._closed = true;
		try {
			if (this.amqpChannel != null) {
				if (this.amqpConsumerTag != null) {
					this.amqpChannel.basicCancel(this.amqpConsumerTag);
				}
				this.amqpChannel.close();
			}
		}
		catch (IOException e) {
			log.warn(String.format("[%s] Error closing AMQP channel", logId()), e);
		} 
		finally {
			try {
				if (this.amqpConnection != null) {
					this.amqpConnection.close();
				}
			} catch (IOException e) {
				log.warn(String.format("[%s] Error closing AMQP connection", logId()), e);
			}
		}
	}

	private void setupAMQP(Map conf) throws IOException {

		final ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost(Configuration.getString(conf, Configuration.SpoutRabbitmqHost));
		connectionFactory.setPort(Configuration.getInt(conf, Configuration.SpoutRabbitmqPort));
		connectionFactory.setUsername(Configuration.getString(conf, Configuration.SpoutRabbitmqUsername));
		connectionFactory.setPassword(Configuration.getString(conf, Configuration.SpoutRabbitmqPassword));
		connectionFactory.setVirtualHost(Configuration.getString(conf, Configuration.SpoutRabbitmqVirtualHost));
		connectionFactory.setRequestedHeartbeat(5);

		this.amqpConnection = connectionFactory.newConnection();
		this.amqpChannel = amqpConnection.createChannel();

		this.amqpChannel.basicQos(1);

        String exchange = this._exchangeName;
        if (exchange.isEmpty()) {
		    final int pos = this.tableName.indexOf("_");
		    if (pos <= 0){
			    throw new RuntimeException("option 'pike.spout.rabbitmq.exchange_name' is not set, and get it from table name failed. "
                        + "(try parse table name as form: xx_xxxx, use first part to be queue exchange name)");
		    }
		    exchange = this.tableName.substring(0, pos);
        }
		final String routingKey = (this._routingKey.isEmpty() ? this.tableName : this._routingKey);
		final String queueName = String.format("queue_%s", this._spoutName);
		final QueueDeclaration queueDeclaration = new SharedAutoDeleteQueueWithBinding(queueName, exchange, routingKey);
		queueDeclaration.declare(this.amqpChannel);
		
		log.info(String.format("[%s] Consuming queue %s", logId(), queueName));

		this.amqpConsumer = new QueueingConsumer(amqpChannel);
		this.amqpConsumerTag = amqpChannel.basicConsume(queueName, true /* no auto-ack */, amqpConsumer);
	}
	
	private void reconnect() {
		log.info(String.format("[%s] Reconnecting to AMQP broker...", logId()));
		try {
			setupAMQP(this._conf);
		} catch (IOException e) {
			throw new RuntimeException(String.format("[%s] Fail to Reconnecting to AMQP broker ", logId()), e);
		}

	}

	private String logId() {
		return String.format("%s:spout:%d", this._topologyName, this._topologyContext.getThisTaskId());
	}

}
