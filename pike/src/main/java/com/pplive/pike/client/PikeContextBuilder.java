package com.pplive.pike.client;

import com.pplive.pike.Configuration;
import com.pplive.pike.exec.output.IPikeOutput;
import com.pplive.pike.generator.ISpoutGenerator;
import com.pplive.pike.generator.KafkaSpoutGenerator;
import com.pplive.pike.generator.LocalTextFileSpoutGenerator;
import com.pplive.pike.metadata.MetaDataProvider;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by jiatingjin on 2016/7/28.
 */
public class PikeContextBuilder {

    private Configuration conf;


    private MetaDataProvider metaDataProvider;

    private ISpoutGenerator spoutGenerator = null;


    private String sql;

    private String topologyName;


    public PikeContextBuilder(Configuration conf) {
        this.conf = conf;
    }

    public PikeContextBuilder withMetaDataProvider(Class<? extends MetaDataProvider> metaDataProviderClass) {
        this.conf.put(Configuration.MetaDataProviderClass, metaDataProviderClass.getCanonicalName());
        return  this;
    }

    public PikeContextBuilder withSpoutGenerator(Class<? extends ISpoutGenerator> spoutGeneratorClass) {
        this.conf.put(Configuration.SpoutGeneratorClass, spoutGeneratorClass.getCanonicalName());
        return  this;
    }


    public PikeContextBuilder withSql(String sql) {
        this.sql = sql;
        return this;
    }

    public PikeContextBuilder withTopologyName(String topologyName) {
        this.topologyName = topologyName;
        return  this;
    }

    /**
     *
     * @param outputClazz implements IPikeOutput
     * @param outputTarget is related to output type, table name for db, dir for hdfs
     * @return
     */
    public PikeContextBuilder withOutput(Class<? extends IPikeOutput> outputClazz, String outputTarget) {
        this.conf.put(Configuration.OutputClassName, outputClazz.getCanonicalName());
        this.conf.put(Configuration.OutputTargetName, outputTarget);
        return  this;
    }

    public PikeContext build() {

        this.metaDataProvider = (MetaDataProvider)createInstance(Configuration.MetaDataProviderClass);
        this.metaDataProvider.init(this.conf);
        this.spoutGenerator = (ISpoutGenerator)createInstance(Configuration.SpoutGeneratorClass);
        this.spoutGenerator.init(this.conf, this.metaDataProvider);

        return new PikeContext(conf, metaDataProvider, sql, topologyName, spoutGenerator);
    }


    private Object createInstance(String fqcn) {
        String clazz = (String)conf.get(fqcn);
        if (StringUtils.isEmpty(clazz)) {
            throw new RuntimeException(String.format("there is no metadata provider, set key %s in pike.yaml", fqcn));
        }

        try {
            Class<?> metaClass=  Class.forName(clazz);
            return metaClass.newInstance();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class NotFound", e);
        } catch (InstantiationException e) {
            throw new RuntimeException("Instantiation Exception", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Illegal Access Exception", e);
        }
    }
}
