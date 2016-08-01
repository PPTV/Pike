package com.pplive.pike.client;

import com.pplive.pike.Configuration;
import com.pplive.pike.generator.ISpoutGenerator;
import com.pplive.pike.generator.KafkaSpoutGenerator;
import com.pplive.pike.generator.LocalTextFileSpoutGenerator;
import com.pplive.pike.metadata.MetaDataProvider;
import org.apache.commons.lang.StringUtils;

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

    public PikeContextBuilder withMetaDataProvider(MetaDataProvider metaDataProvider) {
        this.metaDataProvider = metaDataProvider;
        return  this;
    }

    public PikeContextBuilder withSpoutGenerator(ISpoutGenerator spoutGenerator) {
        this.spoutGenerator = spoutGenerator;
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

    public PikeContextBuilder saveAs() {
        return  this;
    }

    public PikeContext build() {
        if (this.metaDataProvider == null) {
            this.metaDataProvider = (MetaDataProvider)createInstance(Configuration.MetaDataProviderClass);
        }

        metaDataProvider.init(conf);

        if (this.spoutGenerator == null) {
            this.spoutGenerator = (ISpoutGenerator)createInstance(Configuration.SpoutGeneratorClass);

        }

        spoutGenerator.init(conf, metaDataProvider);

        return new PikeContext(conf, metaDataProvider, sql, topologyName, spoutGenerator);
    }


    private Object createInstance(String fqcn) {
        String clazz = (String)conf.get(fqcn);
        if (StringUtils.isEmpty(clazz)) {
            throw new RuntimeException(String.format("there is no metadata provider, set key %s in pike.yaml", fqcn));
        }

        try {
            Class<?> metaClass=  Class.forName(clazz);
            return  metaClass.newInstance();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class NotFound", e);
        } catch (InstantiationException e) {
            throw new RuntimeException("Instantiation Exception", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Illegal Access Exception", e);
        }
    }
}
