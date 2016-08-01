package com.pplive.pike.base;

import java.util.List;
import java.util.Map;

import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;
import org.json.simple.JSONValue;

import com.pplive.pike.Configuration;

import backtype.storm.Config;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;

public class PikeTopologyClient {
	
	public static PikeTopologyClient getConfigured(Map conf) {
        if(conf == null) {
            throw new IllegalArgumentException("conf cannot be null");
        }
        String nimbusHost = Configuration.getString(conf, Config.NIMBUS_HOST);
        int nimbusPort = Configuration.getInt(conf, Config.NIMBUS_THRIFT_PORT, 6627);
        
        if (nimbusHost.isEmpty()) {
        	throw new IllegalArgumentException(String.format("%s in topology config is empty", Config.NIMBUS_HOST));
        }
        if (nimbusPort <= 0) {
        	throw new IllegalArgumentException(String.format("%s in topology config is not positive", Config.NIMBUS_THRIFT_PORT));
        }
        return new PikeTopologyClient(nimbusHost, nimbusPort);
	}
	
	private final String _nimbusHost;
	private final int _nimbusThriftPort;
	
	public PikeTopologyClient(String nimbusHost, int nimbusThriftPort) {
        if (nimbusHost == null || nimbusHost.isEmpty()) {
        	throw new IllegalArgumentException("nimbusHost cannot be null or empty");
        }
        if (nimbusThriftPort <= 0) {
        	throw new IllegalArgumentException("nimbusThriftPort must be positive integer");
        }
		this._nimbusHost = nimbusHost;
		this._nimbusThriftPort = nimbusThriftPort;
	}
	
	private TFramedTransport createTransport() {
		TSocket socket = new TSocket(this._nimbusHost, this._nimbusThriftPort);
		return new TFramedTransport(socket);
	}
	
	public List<TopologySummary> getRunningTopologies() {
       TTransport conn = null;
        try {
            conn = createTransport();
            Nimbus.Client client = new Nimbus.Client(new TBinaryProtocol(conn));
            conn.open();
            ClusterSummary clusterSummary = client.getClusterInfo();
            return clusterSummary.get_topologies();
        }
        catch(TException e) {
    		throw new RuntimeException(e);
        }
        finally {
        	if (conn != null) {
        		conn.close();
        	}
        }
	}
	
	public String getRunningTopologyIdByName(String topologyName) {
		return getRunningTopologyIdByName(topologyName, true, null);
	}
	
	@SuppressWarnings("rawtypes")
	public Map getTopologyConf(String topologyId) {
		return getTopologyConf(topologyId, true, null);
	}
	
	public boolean checkTopologyKilled(String topologyId) {
		return checkTopologyKilled(topologyId, true, false);
	}
	
	public String getRunningTopologyIdByName(String topologyName, boolean throwTException, String resultOnTException) {
        if (topologyName == null || topologyName.isEmpty()) {
        	throw new IllegalArgumentException("topologyName cannot be null or empty");
        }
        TTransport conn = null;
        try {
            conn = createTransport();
            Nimbus.Client client = new Nimbus.Client(new TBinaryProtocol(conn));
            conn.open();
            ClusterSummary clusterSummary = client.getClusterInfo();
            for(TopologySummary topology : clusterSummary.get_topologies()) {
            	if (topology.get_name().equals(topologyName)) {
            		return topology.get_id();
            	}
            }
            return "";
        }
        catch(TException e) {
        	if (throwTException)
        		throw new RuntimeException(e);
        	else
        		return resultOnTException;
        }
        finally {
        	if (conn != null) {
        		conn.close();
        	}
        }
	}
	
	@SuppressWarnings("rawtypes")
	public Map getTopologyConf(String topologyId, boolean throwTException, Map resultOnTException) {
        if (topologyId == null || topologyId.isEmpty()) {
        	throw new IllegalArgumentException("topologyId cannot be null or empty");
        }
        TTransport conn = null;
        try {
            conn = createTransport();
            Nimbus.Client client = new Nimbus.Client(new TBinaryProtocol(conn));
            conn.open();
            String conf = client.getTopologyConf(topologyId);
    		return (Map)JSONValue.parse(conf);
        }
        catch(NotAliveException e) {
            return null;
        }
        catch(TException e) {
        	if (throwTException)
        		throw new RuntimeException(e);
        	else
        		return resultOnTException;
        }
        finally {
        	if (conn != null) {
        		conn.close();
        	}
        }
	}
	
	public boolean checkTopologyKilled(String topologyId, boolean throwTException, boolean resultOnTException) {
        if (topologyId == null || topologyId.isEmpty()) {
        	throw new IllegalArgumentException("topologyId cannot be null or empty");
        }
        TTransport conn = null;
        try {
            conn = createTransport();
            Nimbus.Client client = new Nimbus.Client(new TBinaryProtocol(conn));
            conn.open();
            TopologyInfo topology = client.getTopologyInfo(topologyId);
            String status = topology.get_status();
            return isKilledStatus(status);
        }
        catch(NotAliveException e) {
            return true;
        }
        catch(TException e) {
        	if (throwTException)
        		throw new RuntimeException(e);
        	else
        		return resultOnTException;
        }
        finally {
        	if (conn != null) {
        		conn.close();
        	}
        }
	}
	
	private static boolean isKilledStatus(String topologyStatus) {
		return topologyStatus.equalsIgnoreCase("KILLED");
	}
}
