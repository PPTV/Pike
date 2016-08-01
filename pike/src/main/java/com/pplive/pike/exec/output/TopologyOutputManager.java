package com.pplive.pike.exec.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import com.pplive.pike.function.builtin.DateTime;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.tuple.TridentTuple;

import com.pplive.pike.Configuration;
import com.pplive.pike.base.ISizeAwareIterable;
import com.pplive.pike.base.Period;
import com.pplive.pike.base.PikeTopologyClient;
import com.pplive.pike.base.SizeAwareIterable;
import com.pplive.pike.util.CollectionUtil;

public class TopologyOutputManager {

	public static final Logger log = LoggerFactory
			.getLogger(TopologyOutputManager.class);

    private final Period _baseProcessPeriod;
    private final OutputSchema _outputSchema;
	private final List<OutputTarget> _outputTargets;
    private final boolean _hasOutputContext;

	private Calendar _lastWriteTime;

	@SuppressWarnings("rawtypes")
	private final Map _conf;
    private final boolean _checkSelfKilled;
    private final int _checkMinPeriodSeconds;
	private final boolean _outputLastPeriod;

	private final String _topologyName;
	private final String _topologyId;
	private final PikeTopologyClient _pikeTopologyClient;

	public TopologyOutputManager(@SuppressWarnings("rawtypes") Map conf,
			Period baseProcessPeriod, OutputSchema outputSchema, Iterable<OutputTarget> outputTargets) {
		this._conf = conf;
		this._topologyName = Configuration.getString(conf,
				Configuration.TOPOLOGY_NAME);
		this._topologyId = Configuration
				.getString(conf, Configuration.STORM_ID);
		this._pikeTopologyClient = PikeTopologyClient.getConfigured(conf);
        this._checkMinPeriodSeconds = Configuration.getInt(conf,
                Configuration.OutputCheckMinPeriodSeconds, 15);
        if (baseProcessPeriod.periodSeconds() <= this._checkMinPeriodSeconds) {
            this._checkSelfKilled = false;
        }
        else {
            this._checkSelfKilled = Configuration.getBoolean(conf,
                    Configuration.OutputCheckTopologyselfKilled, false);
        }
		this._outputLastPeriod = Configuration.getBoolean(conf,
				Configuration.OutputLastPeriodResult, false);

        this._baseProcessPeriod = baseProcessPeriod;
        this._outputSchema = outputSchema;
		this._outputTargets = getOutputTargets(conf, outputTargets, this._baseProcessPeriod);
		this._lastWriteTime = this._baseProcessPeriod.currentPeriodBegin();

        boolean hasOutputContext = false;
		for (OutputField f : this._outputSchema.getOutputFields()) {
			f.init();
            if (not(hasOutputContext) && f.hasOutputContext()) {
                hasOutputContext = true;
            }
		}
        this._hasOutputContext = hasOutputContext;
	}

	public void init() {
	}

	public void close() {
	}

    private static int decideTolerantDelayMilliseconds(Period period) {
        return DateTime.decideTolerantDelayMilliseconds(period);
    }

	public void write(ISizeAwareIterable<TridentTuple> tuples) {
		if (this._checkSelfKilled && not(this._outputLastPeriod)) {
			if (isTopologyKilled()) {
				log.info(String
						.format("[%s] output ignore last period data, detect topology killed.",
								this._topologyName));
				return;
			}
		}

		if (tuples == null || tuples.size() == 0) {
			return;
		}

        Period period = this._baseProcessPeriod;
        int tolerantMilliseconds = decideTolerantDelayMilliseconds(period);
        Calendar basePeriodEnd = period.currentPeriodEnd(tolerantMilliseconds);

        List<OutputTarget> outputTargets = new ArrayList<OutputTarget>(this._outputTargets.size());
        ArrayList<OutputContext> outputContexts = new ArrayList<OutputContext>(this._outputTargets.size());
        for (OutputTarget t : this._outputTargets) {
            Calendar periodEnd = t.getOutputPeriod().currentPeriodEnd(tolerantMilliseconds);
            if (basePeriodEnd.equals(periodEnd)){
                outputTargets.add(t);
                outputContexts.add(new OutputContext(t.getOutputPeriod()));
            }
        }

        ArrayList<List<Object>> outputTuples = new ArrayList<List<Object>>(tuples.size());
        for (TridentTuple tuple : tuples) {
            ArrayList<Object> tempResult = evalStep1(tuple, this._outputSchema);
            outputTuples.add(tempResult);
        }

        int nOutput = 0;
        for (OutputTarget t : outputTargets) {
            OutputContext outputCtx = outputContexts.get(nOutput);
            nOutput += 1;

            if (this._hasOutputContext) {
                int n = 0;
                for (TridentTuple tuple : tuples) {
                    List<Object> tempResult = outputTuples.get(n);
                    evalStep2((ArrayList<Object>)tempResult, tuple, this._outputSchema, outputCtx);
                    n += 1;
                }
            }

            IPikeOutput output = this.createOutput(t);
			if (output == null) {
				continue;
			}
			output.init(this._conf, this._outputSchema, t.getTargetName(), t.getOutputPeriod());
			try {
				output.write(basePeriodEnd, SizeAwareIterable.of(outputTuples));
			} finally {
                try
                {
				    output.close();
                } catch(IOException e) {
                    log.error("close output error: ", e);
                }
			}
		}
	}

	private boolean isTopologyKilled() {
		return this._pikeTopologyClient.checkTopologyKilled(this._topologyId,
				false, false);
	}

	private static List<OutputTarget> getOutputTargets(
			@SuppressWarnings("rawtypes") Map conf,
			Iterable<OutputTarget> outputTargets, Period defaultOutputPeriod) {
		List<OutputTarget> targets = CollectionUtil
				.copyArrayList(outputTargets);
		if (targets.size() == 0) {
			targets = getDefaultOutputTargets(conf, defaultOutputPeriod);
		}
		return targets;
	}

	private static List<OutputTarget> getDefaultOutputTargets(
			@SuppressWarnings("rawtypes") Map conf, Period defaultOutputPeriod) {
		Object o = conf.get(Configuration.OutputDefaultTargets);
		if (o == null) {
			return Arrays.asList(new OutputTarget(OutputType.Console, "local", defaultOutputPeriod));
		}

		if (o instanceof String) {
			o = Arrays.asList(o.toString().split(","));
		}

		if (o instanceof List) {
			@SuppressWarnings("unchecked")
			List<Object> list = (List<Object>) o;
			ArrayList<OutputTarget> targets = new ArrayList<OutputTarget>(
					list.size());
			for (Object item : list) {
				if (item == null)
					continue;
				OutputTarget t = tryParseOutputTarget(item.toString(), defaultOutputPeriod);
				if (t != null)
					targets.add(t);
			}
			if (targets.size() == 0)
				targets.add(new OutputTarget(OutputType.Console, "local", defaultOutputPeriod));
			return targets;
		} else {
			return Arrays.asList(new OutputTarget(OutputType.Console, "local", defaultOutputPeriod));
		}
	}

	private static OutputTarget tryParseOutputTarget(String s, Period defaultOutputPeriod) {
		assert s != null;
		s = s.trim();
		int pos = s.indexOf('.');
		if (pos <= 0)
			return null;
		OutputType outputType = OutputType.parse(s.substring(0, pos));
		if (outputType == OutputType.Unknown)
			return null;
		return new OutputTarget(outputType, s.substring(pos + 1), defaultOutputPeriod);
	}

	private IPikeOutput createOutput(OutputTarget outputTarget) {
		String key = String.format("pike.output.%s.className", outputTarget
				.getType().toString().toLowerCase());

		String outputClassName = (String) this._conf.get(key);
		if (StringUtils.isEmpty(outputClassName)) {
			throw new RuntimeException(String.format(
					"set key %s value in pike.yaml ", key));
		}
		Class<?> outputClass;
		try {
			outputClass = Class.forName(outputClassName);
			log.info("create output class name: " + outputClassName);
			return (IPikeOutput) outputClass.newInstance();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Class NotFound", e);
		} catch (InstantiationException e) {
			throw new RuntimeException("Instantiation Exception", e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Illegal Access Exception", e);
		}

	}

	private static ArrayList<Object> evalStep1(TridentTuple tuple, OutputSchema outputSchema) {
		ArrayList<Object> result = new ArrayList<Object>(
				outputSchema.getOutputFieldCount());
		for (OutputField outputField : outputSchema.getOutputFields()) {
            if (outputField.hasOutputContext()) {
                result.add(null);
            }
            else {
                Object obj = outputField.eval(tuple);
			    result.add(obj);
            }
		}
		return result;
	}

    private static ArrayList<Object> evalStep2(ArrayList<Object> tempResult, TridentTuple tuple,
                                               OutputSchema outputSchema, OutputContext outputCtx) {

        assert tempResult != null;
        assert tempResult.size() == outputSchema.getOutputFieldCount();

        int n = 0;
        for (OutputField outputField : outputSchema.getOutputFields()) {
            if (outputField.hasOutputContext()) {
                Object obj = outputField.eval(tuple, outputCtx);
                tempResult.set(n, obj);
            }
            n += 1;
        }
        return tempResult;
    }

    private static boolean not(boolean expr){
        return !expr;
    }

}
