package com.pplive.pike;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.JavaObject;
import backtype.storm.generated.ShellComponent;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;

public class TopologyConsoleDisplayer {

	public static void display(PikeTopology pikeTopology) {
		StormTopology topo = pikeTopology.topology();
		Map<GlobalStreamId, List<String>> streamTargets = analyzeStreamTargets(topo);
		StringBuilder sb = new StringBuilder(1000 * 100);
		getFormattedString(sb, topo, streamTargets);
		System.out.println(sb);
	}
	
	private static Map<GlobalStreamId, List<String>> analyzeStreamTargets(StormTopology topology){
		HashMap<GlobalStreamId, List<String>> streams = new HashMap<GlobalStreamId, List<String>>();
		
		Map<String,SpoutSpec> spouts = topology.get_spouts();
		if (spouts != null){
			for(Map.Entry<String,SpoutSpec> kv : spouts.entrySet()){
				addStreamTargets(streams, kv.getKey(), kv.getValue().get_common());
			}
		}
		
		Map<String,Bolt> bolts = topology.get_bolts();
		if (bolts != null){
			for(Map.Entry<String,Bolt> kv : bolts.entrySet()){
				addStreamTargets(streams, kv.getKey(), kv.getValue().get_common());
			}
		}
		
		Map<String,StateSpoutSpec> stateSpouts = topology.get_state_spouts();
		if (stateSpouts != null){
			for(Map.Entry<String,StateSpoutSpec> kv : stateSpouts.entrySet()){
				addStreamTargets(streams, kv.getKey(), kv.getValue().get_common());
			}
		}
		return streams;
	}
	
	private static void addStreamTargets(Map<GlobalStreamId, List<String>> streams, String componentName, ComponentCommon componentCommon) {
		for(String s : componentCommon.get_streams().keySet()){
			GlobalStreamId streamId = new GlobalStreamId(componentName, s);
			if (streams.containsKey(streamId) == false){
				streams.put(streamId, new ArrayList<String>());
			}
		}
		for(GlobalStreamId streamId : componentCommon.get_inputs().keySet()){
			if (streams.containsKey(streamId) == false){
				streams.put(streamId, new ArrayList<String>());
			}
			streams.get(streamId).add(componentName);
		}
	}
	
	private static void getFormattedString(StringBuilder sb, StormTopology topology, Map<GlobalStreamId, List<String>> streamTargets) {
		String nextLevelLineBeginBlanks = "      ";
		sb.append("Storm Topology:\n");
		Map<String,SpoutSpec> spouts = topology.get_spouts();
		if (spouts == null) {
			sb.append("spouts: <null>\n");
		}
		else if (spouts.isEmpty()) {
			sb.append("spouts: <empty>\n");
		}
		else {
			sb.append("spouts:\n");
			List<String> spoutNames = new ArrayList<String>(spouts.keySet());
			Collections.sort(spoutNames);
			for(String s : spoutNames) {
				getFormattedString(sb, nextLevelLineBeginBlanks, s, spouts.get(s), streamTargets);
			}
		}
		
		Map<String,Bolt> bolts = topology.get_bolts();
		if (bolts == null) {
			sb.append("bolts: <null>\n");
		}
		else if (bolts.isEmpty()) {
			sb.append("bolts: <empty>\n");
		}
		else {
			sb.append("bolts:\n");
			List<String> boltNames = new ArrayList<String>(bolts.keySet());
			Collections.sort(boltNames);
			for(String s : boltNames) {
				getFormattedString(sb, nextLevelLineBeginBlanks, s, bolts.get(s), streamTargets);
			}
		}
	
		Map<String,StateSpoutSpec> stateSpouts = topology.get_state_spouts();
		if (stateSpouts == null) {
			sb.append("stateSpouts: <null>\n");
		}
		else if (bolts.isEmpty()) {
			sb.append("stateSpouts: <empty>\n");
		}
		else {
			sb.append("stateSpouts:\n");
			List<String> stateSpoutNames = new ArrayList<String>(stateSpouts.keySet());
			Collections.sort(stateSpoutNames);
			for(String s : stateSpoutNames) {
				getFormattedString(sb, nextLevelLineBeginBlanks, s, stateSpouts.get(s), streamTargets);
			}
		}
	
	}
	
	private static void getFormattedString(StringBuilder sb, String lineBeginBlanks, String spoutName, SpoutSpec spout, Map<GlobalStreamId, List<String>> streamTargets) {
		final String nextLevelLineBeginBlanks = lineBeginBlanks + "      ";
		sb.append(lineBeginBlanks).append(spoutName).append('\n');
		lineBeginBlanks += "  ";
		
		ComponentCommon componentCommon = spout.get_common();
		if (componentCommon == null) {
			sb.append(lineBeginBlanks).append("common: <null>\n");
		}
		else {
			sb.append(lineBeginBlanks).append("common:\n");
			getFormattedString(sb, nextLevelLineBeginBlanks, spoutName, componentCommon, streamTargets);
		}
		
		ComponentObject spout_obj = spout.get_spout_object();
		if (spout_obj == null) {
			sb.append(lineBeginBlanks).append("spout_object: <null>\n");
		}
		else {
			sb.append(lineBeginBlanks).append("spout_object:\n");
			getFormattedString(sb, nextLevelLineBeginBlanks, spout_obj);
		}
		
	}
	
	private static void getFormattedString(StringBuilder sb, String lineBeginBlanks, String componentName, ComponentCommon componentCommon, Map<GlobalStreamId, List<String>> streamTargets) {
		final String nextLevelLineBeginBlanks = lineBeginBlanks + "    ";
		if (componentCommon.is_set_parallelism_hint()){
			sb.append(String.format("%sparallelism_hint: %d%n", lineBeginBlanks, componentCommon.get_parallelism_hint()));
		}
		if (componentCommon.is_set_json_conf()){
			sb.append(String.format("%sjson_conf: %s%n", lineBeginBlanks, componentCommon.get_json_conf()));
		}

		Map<GlobalStreamId,Grouping> inputs = componentCommon.get_inputs();
		if (inputs == null) {
			sb.append(lineBeginBlanks).append("inputs: <null>\n");
		}
		else if (inputs.isEmpty()) {
			sb.append(lineBeginBlanks).append("inputs: <empty>\n");
		}
		else {
			sb.append(lineBeginBlanks).append("inputs:\n");
			for(Map.Entry<GlobalStreamId,Grouping> kv : inputs.entrySet()) {
				getFormattedString(sb, nextLevelLineBeginBlanks, kv.getKey(), kv.getValue());
			}
		}
		
		Map<String,StreamInfo> streams = componentCommon.get_streams();
		if (streams == null) {
			sb.append(lineBeginBlanks).append("streams: <null>\n");
		}
		else if (streams.isEmpty()) {
			sb.append(lineBeginBlanks).append("streams: <empty>\n");
		}
		else {
			sb.append(lineBeginBlanks).append("streams:\n");
			List<String> streamNames = new ArrayList<String>(streams.keySet());
			Collections.sort(streamNames);
			for(String s : streamNames) {
				getFormattedString(sb, nextLevelLineBeginBlanks, componentName, s, streams.get(s), streamTargets);
			}
		}
	}

	private static void getFormattedString(StringBuilder sb, String lineBeginBlanks, GlobalStreamId globalStreamId, Grouping grouping) {
		sb.append(String.format("%s%s:%s: grouping is %s ", lineBeginBlanks, globalStreamId.get_componentId(), globalStreamId.get_streamId(), grouping.getSetField()));
		if (grouping.is_set_fields()) {
			sb.append('[');
			for(String s : grouping.get_fields()) {
				sb.append(s).append(", ");
			}
			sb.append("]");
		}
		else if (grouping.is_set_custom_serialized()) {
			byte[] bytes = grouping.get_custom_serialized();
			ObjectInputStream oin;
			try {
				ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
				oin = new ObjectInputStream(byteStream);
				sb.append(String.format("<%s>", oin.readObject().getClass().getSimpleName()));
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else if (grouping.is_set_custom_object()) {
			sb.append("<...>");
		}
		sb.append('\n');
	}
	
	private static void getFormattedString(StringBuilder sb, String lineBeginBlanks, String componentName, String streamName, StreamInfo streamInfo, Map<GlobalStreamId, List<String>> streamTargets) {
		sb.append(String.format("%s%s: %s, ", lineBeginBlanks, streamName, streamInfo.is_direct() ? "direct" : "non-direct"));
		lineBeginBlanks += "  ";
		
		List<String> fields = streamInfo.get_output_fields();
		if (fields == null) {
			sb.append("fields: <null>\n");
		}
		else if (fields.isEmpty()) {
			sb.append("fields: <empty>\n");
		}
		else {
			sb.append("fields: [");
			boolean first = true;
			for(String s : fields) {
				if (first == false)
					sb.append(", ");
				else
					first = false;
				sb.append(s);
			}
			sb.append("]\n");
		}
		sb.append(lineBeginBlanks).append("targets: (");
		GlobalStreamId streamId = new GlobalStreamId(componentName, streamName);
		List<String> targets = streamTargets.get(streamId);
		if (targets != null){
			boolean first = true;
			for(String s : targets){
				if (first == false)
					sb.append(", ");
				else
					first = false;
				sb.append(s);
			}
		}
		sb.append(")\n");
	}

	private static void getFormattedString(StringBuilder sb, String lineBeginBlanks, ComponentObject componentObj) {
		if (componentObj.is_set_java_object()) {
			JavaObject javaObj = componentObj.get_java_object();
			sb.append(String.format("%sjava_object: %s%n", lineBeginBlanks, javaObj == null ? "<null>" : "<...>"));
		}
		
		if (componentObj.is_set_serialized_java()) {
			byte[] bytes = componentObj.get_serialized_java();
			String objType = "<null>";
			ObjectInputStream oin;
			if (bytes != null) {
				try {
					ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
					oin = new ObjectInputStream(byteStream);
					objType = " " + oin.readObject().getClass().getSimpleName();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			sb.append(String.format("%sserialized_java: %s%n", lineBeginBlanks, objType));
		}
		
		if (componentObj.is_set_shell()) {
			ShellComponent shell = componentObj.get_shell();
			sb.append(String.format("%sshell: %s%n", lineBeginBlanks, shell == null ? "<null>" : "<...>"));
		}
	}
	
	private static void getFormattedString(StringBuilder sb, String lineBeginBlanks, String boltName, Bolt bolt, Map<GlobalStreamId, List<String>> streamTargets) {
		final String nextLevelLineBeginBlanks = lineBeginBlanks + "      ";
		sb.append(lineBeginBlanks).append(boltName).append('\n');
		lineBeginBlanks += "  ";
		
		ComponentCommon componentCommon = bolt.get_common();
		if (componentCommon == null) {
			sb.append(lineBeginBlanks).append("common: <null>\n");
		}
		else {
			sb.append(lineBeginBlanks).append("common:\n");
			getFormattedString(sb, nextLevelLineBeginBlanks, boltName, componentCommon, streamTargets);
		}
		
		ComponentObject bolt_obj = bolt.get_bolt_object();
		if (bolt_obj == null) {
			sb.append(lineBeginBlanks).append("bolt_obj: <null>\n");
		}
		else {
			sb.append(lineBeginBlanks).append("bolt_obj:\n");
			getFormattedString(sb, nextLevelLineBeginBlanks, bolt_obj);
		}
	}
	
	private static void getFormattedString(StringBuilder sb, String lineBeginBlanks, String stateSpoutName, StateSpoutSpec stateSpout, Map<GlobalStreamId, List<String>> streamTargets) {
		final String nextLevelLineBeginBlanks = lineBeginBlanks + "      ";
		sb.append(lineBeginBlanks).append(stateSpoutName).append('\n');
		lineBeginBlanks += "  ";
		
		ComponentCommon componentCommon = stateSpout.get_common();
		if (componentCommon == null) {
			sb.append(lineBeginBlanks).append("common: <null>\n");
		}
		else {
			sb.append(lineBeginBlanks).append("common:\n");
			getFormattedString(sb, nextLevelLineBeginBlanks, stateSpoutName, componentCommon, streamTargets);
		}
		
		ComponentObject state_spout_obj = stateSpout.get_state_spout_object();
		if (state_spout_obj == null) {
			sb.append(lineBeginBlanks).append("state_spout_object: <null>\n");
		}
		else {
			sb.append(lineBeginBlanks).append("state_spout_object:\n");
			getFormattedString(sb, nextLevelLineBeginBlanks, state_spout_obj);
		}
	}
}
